//! Count human prompts in persisted RunStarted inputs without decoding model or tool records.

use super::{
    fold_lineage_id, metadata_file_id, validate_contiguous_sequence, WalByteAnchor, WalError,
};
use crate::framing::{self, DecodeStep, FrameError};
use crate::fs_ops::File;
use serde::{de::Error as _, Deserialize};
use serde_json::value::RawValue;
use std::{
    io::{Read, Seek, SeekFrom},
    path::Path,
};

/// The human-prompt count through an exact durable head. A byte anchor allows a later
/// migration read to inspect only new frames when another provider admitted intervening turns.
#[derive(Debug, Clone, Copy)]
pub struct HumanPromptCount {
    pub count: u64,
    pub through_seq: u64,
    pub bytes_read: u64,
    pub anchor: Option<WalByteAnchor>,
    pub lineage_id: Option<framing::WalLineageId>,
}

/// Read only through `through_seq`, bounded by `max_bytes`. Checks every frame's digest,
/// sequence and required features, but decodes only the type and human inputs of RunStarted.
/// An absent, truncated or newer-writer history fails rather than undercounting prompts.
pub fn read_human_prompt_count(
    path: impl AsRef<Path>,
    max_bytes: u64,
    through_seq: u64,
    base: Option<&HumanPromptCount>,
) -> Result<HumanPromptCount, WalError> {
    let file = File::open(path).map_err(WalError::Io)?;
    let metadata = file.metadata().map_err(WalError::Io)?;
    let file_id = metadata_file_id(&metadata);
    let base = base.filter(|base| {
        base.through_seq <= through_seq
            && base.anchor.is_some_and(|anchor| {
                Some(anchor.file_id) == file_id
                    && anchor.seq == base.through_seq
                    && anchor.byte_end <= metadata.len()
            })
    });
    count_from(file, max_bytes, through_seq, base, file_id)
}

/// Count human prompts in an immutable archive member, checking frames through the requested
/// head just as the file reader does. The archive reader bounds the member size; archive bytes
/// have no live-file anchor, and a missing head is never mistaken for an empty history.
pub fn count_human_prompts_in_bytes(
    bytes: &[u8],
    max_bytes: u64,
    through_seq: u64,
) -> Result<HumanPromptCount, WalError> {
    count_from(
        std::io::Cursor::new(bytes),
        max_bytes,
        through_seq,
        None,
        None,
    )
}

fn count_from(
    mut file: impl Read + Seek,
    max_bytes: u64,
    through_seq: u64,
    base: Option<&HumanPromptCount>,
    file_id: Option<u64>,
) -> Result<HumanPromptCount, WalError> {
    let mut count = base.map_or(0, |base| base.count);
    let mut previous = base.map(|base| base.through_seq);
    let start = base
        .and_then(|base| base.anchor)
        .map_or(0, |anchor| anchor.byte_end);
    file.seek(SeekFrom::Start(start)).map_err(WalError::Io)?;
    let mut offset = start;
    let mut lineage = base.and_then(|base| base.lineage_id);
    while previous.unwrap_or(0) < through_seq {
        let mut header = [0; framing::HEADER_LEN];
        file.read_exact(&mut header).map_err(WalError::Io)?;
        let len = u32::from_le_bytes(header[..4].try_into().unwrap());
        if len > framing::MAX_PAYLOAD_LEN {
            return Err(WalError::Corrupt {
                offset: offset as usize,
                source: FrameError::PayloadTooLarge { len },
            });
        }
        let size = header.len() + len as usize;
        let read = offset - start + size as u64;
        if read > max_bytes {
            return Err(WalError::TooLarge {
                max_bytes,
                file_size: read,
            });
        }
        let mut frame = Vec::with_capacity(size);
        frame.extend_from_slice(&header);
        frame.resize(size, 0);
        file.read_exact(&mut frame[header.len()..])
            .map_err(WalError::Io)?;
        match framing::decode_projected(&frame, |seq, _, payload| {
            Ok((
                seq,
                human_prompts_in_payload(framing::FRAMING_VERSION, payload)?,
            ))
        }) {
            DecodeStep::Record { framed, .. } => {
                let (seq, prompts) = *framed;
                validate_contiguous_sequence(previous, 1, seq)?;
                previous = Some(seq);
                count = count.checked_add(prompts).ok_or_else(|| {
                    WalError::Io(std::io::Error::other("human prompt count overflow"))
                })?;
            }
            DecodeStep::Lineage { lineage_id, .. } => {
                fold_lineage_id(&mut lineage, lineage_id)?;
            }
            DecodeStep::Corrupt(source) => {
                return Err(WalError::Corrupt {
                    offset: offset as usize,
                    source,
                })
            }
            DecodeStep::NewerWriter { ver, missing } => {
                return Err(WalError::NewerWriter {
                    offset: offset as usize,
                    ver,
                    missing,
                })
            }
            DecodeStep::TornTail => {
                return Err(WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "human prompt history ends before its durable head",
                )))
            }
        }
        offset += size as u64;
    }
    Ok(HumanPromptCount {
        count,
        through_seq,
        bytes_read: offset - start,
        anchor: file_id.map(|file_id| WalByteAnchor {
            file_id,
            seq: through_seq,
            byte_end: offset,
        }),
        lineage_id: lineage,
    })
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    record: &'a RawValue,
}
#[derive(Deserialize)]
struct Input<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(borrow)]
    input: Option<&'a RawValue>,
}
#[derive(Deserialize)]
struct Message<'a> {
    role: broca_protocol::Role,
    #[serde(borrow)]
    content: Vec<Block<'a>>,
}
#[derive(Deserialize)]
struct Block<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
}

fn human_prompts_in_payload(ver: u8, payload: &[u8]) -> Result<u64, serde_json::Error> {
    let record = if ver == framing::GATED_FRAMING_VERSION {
        serde_json::from_slice::<Envelope<'_>>(payload)?
            .record
            .get()
            .as_bytes()
    } else {
        payload
    };
    let input: Input<'_> = serde_json::from_slice(record)?;
    if input.kind != "run_started" {
        return Ok(0);
    }
    let messages: Vec<Message<'_>> = serde_json::from_str(
        input
            .input
            .ok_or_else(|| serde_json::Error::custom("RunStarted is missing input"))?
            .get(),
    )?;
    Ok(messages
        .iter()
        .filter(|message| {
            message.role == broca_protocol::Role::User
                && message
                    .content
                    .iter()
                    .any(|block| block.kind != "tool_result")
        })
        .count() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(seq: u64, json: &str) -> Vec<u8> {
        framing::encode_payload(seq, 1, json.as_bytes()).unwrap()
    }

    #[test]
    fn selective_count_decodes_only_episode_inputs_and_refuses_incomplete_history() {
        // Deliberately omit irrelevant record fields: a full WalRecord decoder would reject
        // these records. The projection needs only episode inputs, not tool/model schemas.
        let first = record(
            1,
            r#"{"type":"run_started","input":[{"role":"user","content":[{"type":"text","text":"imported one"}]},{"role":"user","content":[{"type":"image","source":{}}]}]}"#,
        );
        let other = record(
            2,
            r#"{"type":"tool_result","result":"opaque, not decoded","input":{"not":"an episode input"}}"#,
        );
        let last = record(
            3,
            r#"{"type":"run_started","input":[{"role":"user","content":[{"type":"tool_result"}]},{"role":"assistant","content":[{"type":"text"}]},{"role":"user","content":[{"type":"text"}]}]}"#,
        );
        let mut bytes = [first.clone(), other, last].concat();
        let count = count_human_prompts_in_bytes(&bytes, bytes.len() as u64, 3).unwrap();
        assert_eq!(count.count, 3);
        assert_eq!(count.bytes_read, bytes.len() as u64);
        assert_eq!(
            count_human_prompts_in_bytes(&bytes, first.len() as u64, 1)
                .unwrap()
                .count,
            2
        );
        assert!(matches!(
            count_human_prompts_in_bytes(&bytes, first.len() as u64 - 1, 1),
            Err(WalError::TooLarge { .. })
        ));
        assert!(
            count_human_prompts_in_bytes(&bytes[..bytes.len() - 1], bytes.len() as u64, 3).is_err()
        );
        assert!(count_human_prompts_in_bytes(&bytes, bytes.len() as u64, 4).is_err());
        bytes[framing::HEADER_LEN + 4] ^= 1;
        assert!(matches!(
            count_human_prompts_in_bytes(&bytes, bytes.len() as u64, 3),
            Err(WalError::Corrupt {
                source: FrameError::DigestMismatch { .. },
                ..
            })
        ));
        let gap = record(2, r#"{"type":"turn_finished"}"#);
        assert!(count_human_prompts_in_bytes(&gap, gap.len() as u64, 2).is_err());
        let newer = framing::encode_with_requires(
            1,
            1,
            &["future-prompt/v99"],
            &crate::WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1),
            },
        )
        .unwrap();
        assert!(matches!(
            count_human_prompts_in_bytes(&newer, newer.len() as u64, 1),
            Err(WalError::NewerWriter { .. })
        ));
    }

    #[test]
    fn a_live_count_reuses_only_anchors_from_the_same_file() {
        let path = std::env::temp_dir().join(format!("broca-prompt-anchor-{}", std::process::id()));
        let first = record(
            1,
            r#"{"type":"run_started","input":[{"role":"user","content":[{"type":"text"}]}]}"#,
        );
        let second = record(
            2,
            r#"{"type":"run_started","input":[{"role":"user","content":[{"type":"text"}]}]}"#,
        );
        std::fs::write(&path, &first).unwrap();
        let base = read_human_prompt_count(&path, 4096, 1, None).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut file, &second).unwrap();
        let extended = read_human_prompt_count(&path, 4096, 2, Some(&base)).unwrap();
        assert_eq!(extended.count, 2);
        if let Some(mut anchor) = base.anchor {
            assert_eq!(extended.bytes_read, second.len() as u64);
            anchor.file_id ^= 1;
            let stale = HumanPromptCount {
                count: 99,
                anchor: Some(anchor),
                ..base
            };
            let rescanned = read_human_prompt_count(&path, 4096, 2, Some(&stale)).unwrap();
            assert_eq!(rescanned.count, 2);
            assert_eq!(rescanned.bytes_read, (first.len() + second.len()) as u64);
        }
        drop(file);
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod lineage_tests {
    use super::*;

    #[test]
    fn incremental_counts_validate_repeated_lineage_metadata() {
        let path =
            std::env::temp_dir().join(format!("broca-prompt-lineage-{}", std::process::id()));
        let id = framing::WalLineageId::from_bytes([1; 16]);
        let stamp = framing::encode_lineage(id);
        let first = framing::encode_payload(
            1,
            1,
            br#"{"type":"run_started","input":[{"role":"user","content":[{"type":"text"}]}]}"#,
        )
        .unwrap();
        std::fs::write(&path, [&stamp[..], &first].concat()).unwrap();
        let base = read_human_prompt_count(&path, 4096, 1, None).unwrap();
        let second = framing::encode_payload(
            2,
            1,
            br#"{"type":"run_started","input":[{"role":"user","content":[{"type":"text"}]}]}"#,
        )
        .unwrap();
        let good = [&stamp[..], &first, &stamp[..], &second, &stamp[..]].concat();
        std::fs::write(&path, &good).unwrap();
        let extended = read_human_prompt_count(&path, 4096, 2, Some(&base)).unwrap();
        assert_eq!(extended.count, 2);
        assert_eq!(extended.lineage_id, Some(id));
        let foreign = framing::encode_lineage(framing::WalLineageId::from_bytes([2; 16]));
        std::fs::write(&path, [&stamp[..], &first, &foreign[..], &second].concat()).unwrap();
        assert!(read_human_prompt_count(&path, 4096, 2, Some(&base)).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
