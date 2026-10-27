// A `src/bin/` target is its OWN crate root and does NOT inherit the sibling lib/main
// attribute, so this must be stated per binary or the guarantee silently does not apply here.
#![forbid(unsafe_code)]
// This operator verifier is outside the WAL write/recovery path and power-loss
// proofs: its own I/O only lists/reads logs and writes its verification report.
// Durability work delegated to the library remains covered by the library's lint.
#![allow(clippy::disallowed_methods)]

//! Replay every WAL file under a directory and compare the result against a baseline.
//!
//! Written for verifying a state-root move, where the failure that matters is a tree that
//! arrived with less in it than it started with. Size and file count are both satisfied by
//! a truncated copy, and the damage stays silent until someone tries to resume the
//! affected session. Decoding is what exercises the property: every frame carries a digest
//! over its own contents, so a partial frame cannot decode clean.
//!
//! DECODING ALONE IS NOT ENOUGH, WHICH IS THE WHOLE REASON THIS TAKES A BASELINE. A
//! truncated file decodes clean and reports success: the reader treats a partial final
//! frame as a torn tail and discards it, which is correct behaviour for reading alongside
//! a live writer, but it means a clean decode says nothing about completeness. Measured on
//! a real WAL: intact 4 frames, the same file with 40 bytes removed 3 frames, both clean.
//! Only the counts reveal the loss, so only a comparison can gate on it.
//!
//! The baseline is written by one run and read by the next, rather than printed for
//! someone to carry across. An expected value that a person transcribes puts that person
//! in the failure path, and a migration window is exactly when attention is thinnest.
//!
//! ALL THREE COUNTS ARE COMPARED, AND THE FILE COUNT IS NOT REDUNDANT: a session whose WAL
//! was created but never appended to is a zero-byte file, which contributes nothing to the
//! frame or byte totals. Losing one is invisible to two of the three fields. They are not
//! rare -- around one in fifty files in a real tree -- so dropping the file count as
//! implied by the others would leave a population of sessions outside the comparison.
//!
//! Take the baseline while nothing is writing. Against a live tree it is a floor that is
//! already stale when written, so a later clean check proves only "no loss since the
//! baseline", and anything recorded between then and the daemon stopping sits outside the
//! comparison entirely.
//!
//! This deliberately calls the same reader the engine uses. A second implementation of the
//! frame layout would be a second thing to get wrong, and when a verifier disagrees with
//! the real decoder, the verifier is the one that gets believed.
//!
//! Usage:
//!   wal-verify baseline <dir> <file>   record the inventory of a known-good tree
//!   wal-verify check    <dir> <file>   replay and require it to match that inventory

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const MAX_WAL_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// Frame digests protect the envelope, but do not establish that the independently stored
/// replacement digest agrees with its inline content. Check both live and archived records.
fn verify_inline_replacements(records: &[broca_wal::FramedRecord]) -> Result<(), String> {
    for framed in records {
        if let broca_wal::WalRecord::CompactionMessage {
            replacement,
            sha256,
            ..
        } = &framed.record
        {
            let actual = broca_wal::compaction_replacement_sha256(replacement)
                .map_err(|error| format!("seq={} replacement encoding: {error}", framed.seq))?;
            if *sha256 != actual {
                return Err(format!(
                    "seq={} inline replacement sha256 mismatch: stored {sha256}, computed {actual}",
                    framed.seq
                ));
            }
        }
    }
    Ok(())
}

struct Inventory {
    files: usize,
    frames: usize,
    bytes: u64,
    /// Per-WAL verified (frames, bytes), keyed by path relative to the WAL directory.
    ///
    /// BYTES are carried beside frames because frames alone cannot separate the two kinds of
    /// zero-frame file, and they have OPPOSITE safety: a zero-BYTE WAL holds nothing and its
    /// absence cannot lose data, while a 69-byte lineage-frame-only WAL holds a minted
    /// lineage id whose loss makes the next open mint a fresh one -- which consumers read as
    /// lineage recreation. Exempting every zero-FRAME entry from the missing check would
    /// blind this tool to losing the second kind while making the first kind reportable.
    ///
    /// Totals alone cannot answer the question this tool exists to answer. Three aggregate
    /// numbers do not preserve SET MEMBERSHIP, so a destination holding none of the baseline's
    /// sessions passes as long as whatever it does hold is bigger -- measured: a baseline over
    /// one real 4,451-byte 4-frame WAL certified a tree containing only an unrelated 560 KB
    /// session, exit 0, "OK, at or above baseline".
    per_wal: BTreeMap<String, (usize, u64)>,
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let (Some(mode), Some(dir), Some(baseline)) = (args.get(1), args.get(2), args.get(3)) else {
        eprintln!("usage: wal-verify baseline|check <dir> <baseline-file>");
        return ExitCode::from(2);
    };
    let dir = PathBuf::from(dir);
    let baseline = PathBuf::from(baseline);

    let taken = match take_inventory(&dir) {
        Ok(inventory) => inventory,
        Err(error) => {
            eprintln!("wal-verify: {error}");
            return ExitCode::from(2);
        }
    };
    println!(
        "files {} frames {} bytes {}",
        taken.files, taken.frames, taken.bytes
    );

    match mode.as_str() {
        "baseline" => match write_baseline(&baseline, &taken) {
            Ok(()) => {
                println!("wal-verify: baseline written to {}", baseline.display());
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("wal-verify: cannot write baseline: {error}");
                ExitCode::from(2)
            }
        },
        "check" => {
            let expected = match read_baseline(&baseline) {
                Ok(expected) => expected,
                Err(error) => {
                    eprintln!("wal-verify: cannot read baseline: {error}");
                    return ExitCode::from(2);
                }
            };
            // Anything short of the baseline is the failure this exists to catch. A tree
            // that grew is not: the daemon may have served traffic between the two runs,
            // so only a shortfall is evidence of loss.
            // Baseline entries that held NOTHING and are now gone. Discounted from the file
            // total below, because the total is a proxy for "did the tree lose history" and a
            // zero-byte WAL is not history: `read_file_capped` maps its presence and its
            // absence to the same empty buffer, so no reader can tell.
            //
            // The file count was added to catch a tree losing files WITHOUT losing frames or
            // bytes -- which is precisely the zero-byte case, so on its own reasoning it was
            // guarding something that needs no guard. What actually covers set membership is
            // the per-file identity check below, and that one still names every real loss.
            // Scoped to zero BYTES: a 69-byte lineage-frame-only WAL also has zero frames,
            // and losing it IS real, because the id it carries is what consumers read to
            // detect lineage recreation.
            let safely_absent = expected
                .per_wal
                .iter()
                .filter(|(name, (frames, bytes))| {
                    *frames == 0 && *bytes == 0 && !taken.per_wal.contains_key(*name)
                })
                .count();
            let expected_files = expected.files.saturating_sub(safely_absent);

            let mut short = Vec::new();
            if taken.files < expected_files {
                short.push(format!(
                    "files {} < {}{}",
                    taken.files,
                    expected_files,
                    if safely_absent > 0 {
                        format!(" (baseline {} less {safely_absent} empty)", expected.files)
                    } else {
                        String::new()
                    }
                ));
            }
            if taken.frames < expected.frames {
                short.push(format!("frames {} < {}", taken.frames, expected.frames));
            }
            if taken.bytes < expected.bytes {
                short.push(format!("bytes {} < {}", taken.bytes, expected.bytes));
            }

            // Identity, which is the check the totals cannot make. Every baseline WAL must
            // still be present and must not have lost frames. Extra WALs and appended
            // frames are fine -- the daemon may have served traffic between the two runs.
            let mut missing = Vec::new();
            let mut shrunk = Vec::new();
            for (name, (expected_frames, expected_bytes)) in &expected.per_wal {
                match taken.per_wal.get(name) {
                    // A file the baseline recorded as ZERO BYTES holds nothing, and its
                    // absence is indistinguishable from its presence to every reader --
                    // `read_file_capped` maps both to an empty buffer. So removing one cannot
                    // lose data, and reporting it as a shortfall would make the deliberate
                    // sweep of 634 such files read as 634 lost sessions on this instrument.
                    //
                    // Scoped to zero BYTES, not zero FRAMES. A 69-byte lineage-frame-only WAL
                    // also has zero frames and its loss IS real: the id it carries is what
                    // consumers use to detect lineage recreation.
                    None if *expected_frames == 0 && *expected_bytes == 0 => {}
                    None => missing.push(name.clone()),
                    // BYTES ARE COMPARED PER FILE, NOT ONLY IN THE AGGREGATE. A WAL
                    // carrying no records still carries its 69-byte lineage identity
                    // frame, so `frames` cannot see that identity disappear: a
                    // lineage-only file truncated to zero reads 0 >= 0 frames and
                    // passes, while ANY unrelated new WAL restores the aggregate byte
                    // total that would otherwise have caught it. Production holds 153
                    // such lineage-only files, and losing one is not cosmetic -- the
                    // next append mints a FRESH identity, which tells every consumer
                    // the session was recreated.
                    //
                    // A shortfall is REPORTED rather than judged. Torn-tail truncation
                    // on open can legitimately shrink a file, so this can fire on a
                    // live tree -- that is the direction to be wrong in, since a false
                    // positive costs one look and a false negative costs a lineage
                    // identity with no signal at all.
                    Some((actual, actual_bytes))
                        if actual < expected_frames || actual_bytes < expected_bytes =>
                    {
                        shrunk.push(format!(
                            "{name} {actual} < {expected_frames} frames, {actual_bytes} < {expected_bytes} bytes"
                        ))
                    }
                    Some(_) => {}
                }
            }
            // Naming a bounded sample rather than every entry: a migration that lost
            // thousands of sessions should not bury the verdict under thousands of lines.
            if !missing.is_empty() {
                let sample: Vec<&str> = missing.iter().take(5).map(String::as_str).collect();
                short.push(format!(
                    "{} baseline WAL(s) absent, e.g. {}",
                    missing.len(),
                    sample.join(", ")
                ));
            }
            if !shrunk.is_empty() {
                let sample: Vec<&str> = shrunk.iter().take(5).map(String::as_str).collect();
                short.push(format!(
                    "{} baseline WAL(s) lost frames, e.g. {}",
                    shrunk.len(),
                    sample.join(", ")
                ));
            }
            if short.is_empty() {
                println!(
                    "wal-verify: OK, at or above baseline (files {} frames {} bytes {}, \
                         all {} baseline WAL identities present)",
                    expected.files,
                    expected.frames,
                    expected.bytes,
                    expected.per_wal.len()
                );
                ExitCode::SUCCESS
            } else {
                eprintln!("wal-verify: SHORT OF BASELINE — {}", short.join(", "));
                eprintln!("wal-verify: the tree is missing durable history. Do not start the daemon against it.");
                ExitCode::from(1)
            }
        }
        other => {
            eprintln!("wal-verify: unknown mode '{other}' (expected baseline or check)");
            ExitCode::from(2)
        }
    }
}

fn take_inventory(dir: &Path) -> Result<Inventory, String> {
    let live_trees = ["wal", "wal-ephemeral", "wal-durable"];
    let state_root = if live_trees
        .iter()
        .chain(["wal-archive", "wal-archive-durable"].iter())
        .any(|name| dir.join(name).is_dir())
    {
        Some(dir)
    } else if dir
        .file_name()
        .is_some_and(|name| live_trees.iter().any(|tree| name == *tree))
    {
        dir.parent()
    } else {
        None
    };
    let wal_dir = state_root
        .map(|root| root.join("wal"))
        .unwrap_or_else(|| dir.to_path_buf());

    let mut files = Vec::new();
    if let Some(root) = state_root {
        for name in live_trees {
            let directory = root.join(name);
            match collect_wals(&directory, &mut files) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("cannot read {}: {error}", directory.display())),
            }
        }
    } else {
        collect_wals(&wal_dir, &mut files)
            .map_err(|error| format!("cannot read {}: {error}", wal_dir.display()))?;
    }
    files.sort();

    let mut frames = 0usize;
    let mut bytes = 0u64;
    let mut damaged = 0usize;
    let mut newer = 0usize;
    let mut per_wal = BTreeMap::new();
    for file in &files {
        // A file that cannot be measured must not be counted as zero bytes. Silently
        // contributing nothing would lower a baseline, and a lowered baseline is one the
        // later comparison passes more easily -- an unreadable file would weaken the very
        // gate meant to catch loss, without appearing anywhere in the output.
        let size = match std::fs::metadata(file) {
            Ok(meta) => meta.len(),
            Err(error) => {
                damaged += 1;
                eprintln!(
                    "unmeasurable {}: {error}",
                    file.strip_prefix(&wal_dir).unwrap_or(file).display()
                );
                continue;
            }
        };
        match broca_wal::replay_file(file, MAX_WAL_BYTES) {
            Ok(records) => {
                if let Err(error) = verify_inline_replacements(&records) {
                    damaged += 1;
                    eprintln!(
                        "damaged {}: {error}",
                        file.strip_prefix(&wal_dir).unwrap_or(file).display()
                    );
                    continue;
                }
                frames += records.len();
                bytes += size;
                per_wal.insert(
                    file.strip_prefix(&wal_dir)
                        .unwrap_or_else(|_| Path::new(file.file_name().unwrap()))
                        .to_string_lossy()
                        .into_owned(),
                    (records.len(), size),
                );
            }
            // Intact bytes this build cannot read. Reported apart from damage so nobody goes
            // looking for a repair: the remedy is to verify with a build that knows the
            // missing features. The counts are still incomplete, so the run still fails.
            Err(error @ broca_wal::WalError::NewerWriter { .. }) => {
                newer += 1;
                eprintln!(
                    "newer-writer {}: {error}",
                    file.strip_prefix(&wal_dir).unwrap_or(file).display()
                );
            }
            Err(error) => {
                damaged += 1;
                eprintln!(
                    "damaged {}: {error}",
                    file.strip_prefix(&wal_dir).unwrap_or(file).display()
                );
            }
        }
    }
    if newer > 0 {
        return Err(format!(
            "{newer} file(s) were written by a newer broca (NewerWriter) and {damaged} could \
             not be read or decoded; verify with a build that knows the missing features; \
             the counts above are incomplete"
        ));
    }
    if damaged > 0 {
        return Err(format!(
            "{damaged} file(s) could not be read or decoded; the counts above are incomplete"
        ));
    }

    let mut archives = Vec::new();
    for name in ["wal-archive", "wal-archive-durable"] {
        let archive_dir = wal_dir.parent().unwrap_or_else(|| Path::new("")).join(name);
        match std::fs::read_dir(&archive_dir) {
            Ok(entries) => {
                for entry in entries {
                    let path = entry.map_err(|e| e.to_string())?.path();
                    if path.extension().is_some_and(|e| e == "ark") {
                        archives.push((
                            broca_wal::archive::fold_timestamp(&path).map_err(|e| e.to_string())?,
                            path,
                        ));
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    archives.sort_by_key(|(stamp, _)| std::cmp::Reverse(*stamp));
    for (_, path) in archives {
        let mut reader = broca_wal::archive::ArchiveReader::open(path, MAX_WAL_BYTES)
            .map_err(|e| e.to_string())?;
        if reader.state().map_err(|e| e.to_string())?
            == broca_wal::archive::ContainerState::Unsealed
        {
            continue;
        }
        for info in reader.rescan().map_err(|e| e.to_string())? {
            let name = format!("{}.wal", info.session_addr);
            // Live wins; among archives only the newest member counts. Inventories
            // count logical WALs, not containers, so a fold preserves the baseline.
            if per_wal.contains_key(&name) {
                continue;
            }
            let member = reader
                .read_member(&info.session_addr)
                .map_err(|e| e.to_string())?
                .ok_or("archive member disappeared")?;
            let range =
                broca_wal::file_wal::read_verified_member_range(&member.payload, 1, 0, None)
                    .map_err(|e| e.to_string())?;
            let count = match range.records() {
                broca_wal::WalRangeRecords::Complete(records) => {
                    verify_inline_replacements(records)
                        .map_err(|error| format!("damaged archive member {name}: {error}"))?;
                    records.len()
                }
                _ => return Err("archive decoder did not return complete records".into()),
            };
            frames += count;
            bytes += info.payload_len;
            per_wal.insert(name, (count, info.payload_len));
        }
    }
    if per_wal.is_empty() {
        return Err(format!(
            "no live or archived WALs under {}",
            wal_dir.display()
        ));
    }
    Ok(Inventory {
        files: per_wal.len(),
        frames,
        bytes,
        per_wal,
    })
}

fn write_baseline(path: &Path, inventory: &Inventory) -> std::io::Result<()> {
    let mut text = format!(
        "files {}\nframes {}\nbytes {}\n",
        inventory.files, inventory.frames, inventory.bytes
    );
    for (name, (frames, size)) in &inventory.per_wal {
        text.push_str(&format!("wal {name} {frames} {size}\n"));
    }
    std::fs::write(path, text)
}

fn read_baseline(path: &Path) -> Result<Inventory, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let mut files = None;
    let mut frames = None;
    let mut bytes = None;
    let mut per_wal = BTreeMap::new();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        match (parts.next(), parts.next()) {
            (Some("files"), Some(v)) => files = v.parse().ok(),
            (Some("frames"), Some(v)) => frames = v.parse().ok(),
            (Some("bytes"), Some(v)) => bytes = v.parse().ok(),
            (Some("wal"), Some(name)) => {
                let Some(count) = parts.next().and_then(|v| v.parse().ok()) else {
                    return Err(format!(
                        "{}: wal line for {name} has no frame count",
                        path.display()
                    ));
                };
                // A pre-bytes baseline is REFUSED rather than defaulted. Defaulting the size
                // to 0 would mark every entry exempt from the missing check below, turning an
                // old baseline into one that certifies a tree it never measured.
                let Some(size) = parts.next().and_then(|v| v.parse().ok()) else {
                    return Err(format!(
                        "{}: wal line for {name} carries no byte count. This baseline predates \
                         per-file sizes; re-take it against a tree you trust rather than \
                         comparing against it.",
                        path.display()
                    ));
                };
                per_wal.insert(name.to_string(), (count, size));
            }
            _ => {}
        }
    }
    // A baseline missing any field would silently skip that comparison, so refuse it
    // rather than check less than the operator believes is being checked.
    let (Some(files), Some(frames), Some(bytes)) = (files, frames, bytes) else {
        return Err(format!(
            "{} is missing one of files/frames/bytes",
            path.display()
        ));
    };
    // A baseline written before per-WAL identities existed carries totals only, and totals
    // alone certify a tree that shares none of its sessions. Refusing is the point: silently
    // falling back to the aggregate comparison would give the weaker answer under the
    // stronger tool's name.
    if per_wal.is_empty() {
        return Err(format!(
            "{} carries no per-WAL identities; it predates identity checking and can only \
             compare totals, which passes for a tree sharing none of its sessions. Re-run \
             `wal-verify baseline` against the source tree.",
            path.display()
        ));
    }
    Ok(Inventory {
        files,
        frames,
        bytes,
        per_wal,
    })
}

fn collect_wals(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if dir
        .components()
        .any(|part| part.as_os_str() == "wal-staging")
    {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_wals(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "wal") {
            match broca_wal::migration::guard(&path, "", false) {
                Ok(()) => {}
                Err(broca_wal::WalError::SessionMoved { .. }) => continue,
                Err(error) => return Err(std::io::Error::other(error.to_string())),
            }
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod move_reader_tests {
    use super::*;
    #[test]
    fn inventory_finds_durable_live_and_archived_wals_without_ordinary_tree() {
        use broca_wal::Wal;
        let root = broca_test_support::scratch_root()
            .join(format!("verify-durable-{}", std::process::id()));
        let path = root.join("wal-durable/1234567890abcdef.wal");
        let mut wal = broca_wal::FileWal::open(&path).unwrap();
        wal.append(
            1,
            &broca_wal::WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .unwrap();
        drop(wal);
        let before = take_inventory(&root).unwrap();
        assert_eq!(before.files, 1);
        assert_eq!(before.frames, 1);
        let archive = root.join("wal-archive-durable/fold-10.ark");
        std::fs::create_dir_all(archive.parent().unwrap()).unwrap();
        let mut writer = broca_wal::archive::ArchiveWriter::create(&archive)
            .unwrap()
            .without_feature_floor();
        writer
            .append("1234567890abcdef", &std::fs::read(&path).unwrap())
            .unwrap();
        writer.seal().unwrap();
        std::fs::remove_file(path).unwrap();
        let after = take_inventory(&root).unwrap();
        assert_eq!(before.per_wal, after.per_wal);
        assert_eq!(before.files, after.files);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn inventory_ignores_staging_and_moved_wals_but_keeps_frozen_reads() {
        use broca_wal::Wal;
        let root =
            broca_test_support::scratch_root().join(format!("verify-moves-{}", std::process::id()));
        let live = root.join("wal/live.wal");
        let moved = root.join("wal/moved.wal");
        let staged = root.join("wal-staging/cut/staged.wal");
        for path in [&live, &moved] {
            let mut writer = broca_wal::FileWal::open(path).unwrap();
            writer
                .append(
                    1,
                    &broca_wal::WalRecord::StepStarted {
                        step_id: broca_protocol::StepId(1),
                    },
                    true,
                )
                .unwrap();
        }
        let bytes = std::fs::read(&live).unwrap();
        broca_wal::migration::stage(
            &root,
            &staged,
            &bytes,
            &broca_wal::migration::digest(&bytes),
        )
        .unwrap();
        broca_wal::migration::freeze(
            &root,
            &live,
            &broca_wal::migration::Freeze {
                cut_id: "cut".into(),
                end_offset: bytes.len() as u64,
                lineage_id: "lineage".into(),
            },
        )
        .unwrap()
        .unwrap();
        broca_wal::migration::atomic_json(
            &root,
            &moved.with_extension("moved"),
            &broca_wal::migration::Moved {
                cut_id: "cut".into(),
                holder: "remote".into(),
                lineage_id: "lineage".into(),
            },
        )
        .unwrap();
        let mut found = Vec::new();
        collect_wals(&root, &mut found).unwrap();
        assert_eq!(found, vec![live]);
    }
}
