//! Measures a converted log in the new format: its exact framed size, how long loading it takes
//! (read, decrypt, decode), what its history would cost sealed (zstd in 16 KiB blocks, with and
//! without opaque reasoning pruned), and what appending it costs with real barriers.
//!
//! Frames here are real encrypted frames. The size and load measurements frame each record as its
//! own one-frame batch, which slightly overstates frame count but not bytes. The append
//! measurement writes through the open log's writer, in real batches with real barriers, and
//! loads the file back. Checkpoints aren't built yet: a written log's checkpoint is empty.

use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use broca_protocol::content::ContentBlock;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::frame::{open, seal, FrameHeader, FrameKey, FrameRead, FrameStream};
use broca_session_store::log::{ChainBase, LogEnd, LogWriter};
use broca_session_store::record::{Record, Stream};

use crate::Converted;

/// Sealed blocks are cut at record boundaries once they reach this size (design 7.3).
pub const BLOCK_TARGET: usize = 16 * 1024;
/// What a sealed block adds around its compressed bytes: nonce, tag and a small header.
const BLOCK_OVERHEAD: usize = 24 + 16 + 16;
/// What a pruned opaque item leaves behind: provider format, model, size and hash.
const PRUNED_STUB: usize = 48;

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct LogMeasure {
    pub records: u64,
    pub frame_bytes: u64,
    /// Read the framed log from memory, decrypt and decode every record.
    pub load_us: u64,
    pub sealed_bytes: u64,
    pub sealed_pruned_bytes: u64,
    /// Opaque reasoning, signatures and provider text state that pruning removes.
    pub pruned_opaque_bytes: u64,
}

fn frame_stream(stream: Stream) -> FrameStream {
    match stream {
        Stream::Transcript => FrameStream::Transcript,
        Stream::Ledger => FrameStream::Ledger,
    }
}

/// Frames every record of a log into one buffer, as an open log would hold it.
pub fn frame_log(records: &[Record], key: &FrameKey, bind: &[u8]) -> Result<Vec<u8>, String> {
    let mut log = Vec::new();
    let mut last_transcript = None;
    let mut last_ledger = None;
    for (index, record) in records.iter().enumerate() {
        let seq = index as u64 + 1;
        let stream = record.stream();
        let previous = match stream {
            Stream::Transcript => &mut last_transcript,
            Stream::Ledger => &mut last_ledger,
        };
        let header = FrameHeader {
            flags: 0,
            stream: frame_stream(stream),
            kind: u8::try_from(record.kind()).map_err(|_| "kind above 255")?,
            kind_version: 1,
            key_id: key.id(),
            seq,
            batch_first: seq,
            batch_count: 1,
            frame_index: index as u32,
            synced_through: 0,
            prev_in_stream: previous.replace(seq),
        };
        log.extend(seal(&header, &record.encode_body(), key, bind).map_err(|e| format!("{e:?}"))?);
    }
    Ok(log)
}

/// Decrypts and decodes a framed log, as loading an open log does.
pub fn load_log(log: &[u8], key: &FrameKey, bind: &[u8]) -> Result<Vec<Record>, String> {
    let mut records = Vec::new();
    let mut rest = log;
    while !rest.is_empty() {
        match open(rest, |id| (id == key.id()).then_some(key), bind) {
            Ok(FrameRead::Frame { header, body, len }) => {
                records.push(
                    Record::decode(u32::from(header.kind), &body).map_err(|e| e.to_string())?,
                );
                rest = &rest[len..];
            }
            Ok(FrameRead::Incomplete) => return Err("incomplete frame".into()),
            Err(e) => return Err(format!("{e:?}")),
        }
    }
    Ok(records)
}

fn prune(record: &Record, ended: &HashSet<String>) -> (Record, u64) {
    let Record::Assistant(assistant) = record else {
        return (record.clone(), 0);
    };
    if !ended.contains(&assistant.run_id) {
        return (record.clone(), 0);
    }
    let mut assistant = assistant.clone();
    let mut removed = 0u64;
    let mut stub = |text: &mut String| {
        if !text.is_empty() {
            removed += text.len() as u64;
            text.clear();
        }
    };
    for block in &mut assistant.message.content {
        match block {
            ContentBlock::RedactedReasoning { data, .. } => stub(data),
            ContentBlock::Reasoning { signature, .. } => {
                if let Some(signature) = signature.as_mut() {
                    stub(signature);
                }
            }
            ContentBlock::TextWithState { provider_state, .. } => stub(&mut provider_state.data),
            ContentBlock::ToolCall(call) => {
                if let Some(signature) = call.thought_signature.as_mut() {
                    stub(signature);
                }
            }
            _ => {}
        }
    }
    (Record::Assistant(assistant), removed)
}

fn sealed_size(bodies: impl Iterator<Item = Vec<u8>>) -> u64 {
    let mut total = 0u64;
    let mut block: Vec<u8> = Vec::with_capacity(BLOCK_TARGET * 2);
    let flush = |block: &mut Vec<u8>, total: &mut u64| {
        if !block.is_empty() {
            let compressed = zstd::bulk::compress(block, 3).expect("zstd compresses");
            *total += (compressed.len() + BLOCK_OVERHEAD) as u64;
            block.clear();
        }
    };
    for body in bodies {
        if !block.is_empty() && block.len() + body.len() > BLOCK_TARGET {
            flush(&mut block, &mut total);
        }
        broca_session_store::codec::put_varint(&mut block, body.len() as u64);
        block.extend_from_slice(&body);
    }
    flush(&mut block, &mut total);
    total
}

pub fn measure(converted: &Converted, key: &FrameKey, bind: &[u8]) -> Result<LogMeasure, String> {
    let log = frame_log(&converted.records, key, bind)?;
    let started = Instant::now();
    let loaded = load_log(&log, key, bind)?;
    let load_us = started.elapsed().as_micros() as u64;
    if loaded != converted.records {
        return Err("a record did not read back as written".into());
    }

    let ended: HashSet<String> = converted
        .records
        .iter()
        .filter_map(|r| match r {
            Record::RunEnd(end) => Some(end.run_id.clone()),
            _ => None,
        })
        .collect();
    let mut sealed = 0;
    let mut sealed_pruned = 0;
    let mut pruned_bytes = 0;
    for stream in [Stream::Transcript, Stream::Ledger] {
        let of_stream = || {
            converted
                .records
                .iter()
                .filter(move |r| r.stream() == stream)
        };
        sealed += sealed_size(of_stream().map(Record::encode_body));
        let mut pruned = Vec::new();
        let mut pruned_records = 0u64;
        for record in of_stream() {
            let (record, removed) = prune(record, &ended);
            if removed > 0 {
                pruned_bytes += removed;
                pruned_records += 1;
            }
            pruned.push(record);
        }
        // Each pruned record keeps a small stub per pruned item; one per record is a floor.
        sealed_pruned += sealed_size(pruned.iter().map(Record::encode_body))
            + pruned_records * PRUNED_STUB as u64;
    }

    Ok(LogMeasure {
        records: converted.records.len() as u64,
        frame_bytes: log.len() as u64,
        load_us,
        sealed_bytes: sealed,
        sealed_pruned_bytes: sealed_pruned,
        pruned_opaque_bytes: pruned_bytes,
    })
}

/// Whether the live writer would sync after `record`, given the record after it: after a run's
/// inputs, after the dispatch intents of a batch (before any call leaves), after a batch's last
/// result, and at a run's end. Everything else rides the next barrier.
pub fn barrier_after(record: &Record, next: Option<&Record>) -> bool {
    match record {
        Record::Input(_) => !matches!(next, Some(Record::Input(_))),
        Record::ToolIntent(_) => !matches!(next, Some(Record::ToolIntent(_))),
        Record::BatchEnd(_) | Record::RunEnd(_) => true,
        _ => false,
    }
}

#[derive(Debug, Default, serde::Serialize)]
pub struct AppendMeasure {
    pub barriers: u64,
    pub bytes: u64,
    pub total_ms: f64,
    /// Microseconds per barrier: write the batch, then sync it to the device.
    pub barrier_us: Vec<u64>,
    /// Microseconds to load the written log: read the file, read and decrypt every frame,
    /// and fold both streams.
    pub load_us: u64,
}

/// Writes a log to `file_path` through the open log's writer, as the live writer would: each
/// group of records up to a barrier is one batch, written in one call and then synced. On
/// macOS, `sync_all` issues `F_FULLFSYNC`, today's barrier. Then it loads the file back, checks
/// it reads clean with every record, and times that load.
pub fn append_log(
    records: &[Record],
    key: &FrameKey,
    bind: &[u8],
    file_path: &Path,
) -> Result<AppendMeasure, String> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(file_path)
        .map_err(|e| e.to_string())?;
    let mut measure = AppendMeasure::default();
    let started = Instant::now();
    let mut writer = LogWriter::create(
        file,
        key.clone(),
        bind.to_vec(),
        &[],
        1,
        ChainBase::default(),
    )
    .map_err(|e| format!("{e:?}"))?;
    let mut batch_start = 0;
    for (index, record) in records.iter().enumerate() {
        if barrier_after(record, records.get(index + 1)) || index + 1 == records.len() {
            let barrier = Instant::now();
            writer
                .append(&records[batch_start..=index])
                .map_err(|e| format!("{e:?}"))?;
            writer.barrier().map_err(|e| format!("{e:?}"))?;
            measure
                .barrier_us
                .push(barrier.elapsed().as_micros() as u64);
            measure.barriers += 1;
            batch_start = index + 1;
        }
    }
    measure.bytes = writer.offset();
    measure.total_ms = started.elapsed().as_secs_f64() * 1000.0;
    drop(writer);

    let load = Instant::now();
    let bytes = std::fs::read(file_path).map_err(|e| e.to_string())?;
    let log = broca_session_store::log::read(&bytes, key, bind, 0, ChainBase::default())
        .map_err(|e| format!("{e:?}"))?;
    let mut conversation = Conversation::new(false);
    let mut operations = Operations::default();
    for (_, record) in &log.records {
        match record.stream() {
            Stream::Transcript => conversation.apply(record).map_err(|e| format!("{e:?}"))?,
            Stream::Ledger => operations.apply(record).map_err(|e| format!("{e:?}"))?,
        }
    }
    measure.load_us = load.elapsed().as_micros() as u64;
    if log.end != LogEnd::Clean || log.records.len() != records.len() {
        return Err(format!(
            "read back {:?} with {} of {} records",
            log.end,
            log.records.len(),
            records.len()
        ));
    }
    Ok(measure)
}
