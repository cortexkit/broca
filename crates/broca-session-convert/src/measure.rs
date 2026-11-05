//! Measures a converted log in the new format: its exact framed size, how long loading it takes
//! (read, decrypt, decode), what its history would cost sealed (zstd in 16 KiB blocks, with and
//! without opaque reasoning pruned), and what appending it costs with real barriers.
//!
//! Frames here are real encrypted frames. The size and load measurements frame each record as its
//! own one-frame batch, which slightly overstates frame count but not bytes. The append
//! measurement writes a real session on local files, in real batches with real barriers, and
//! loads it back through the real load path.

use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use broca_protocol::content::ContentBlock;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::codec::Digest;
use broca_session_store::codec::Message as _;
use broca_session_store::frame::{open, seal, FrameHeader, FrameKey, FrameRead, FrameStream};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::{PartRef, Placement, Record, Stream};
use broca_session_store::session::Session;
use broca_session_store::setup::SetupStore;
use futures::executor::block_on;

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
    /// Microseconds to load the written session: read and open its head, read the open log,
    /// decrypt and decode every frame, fold both streams from the checkpoint, and open the
    /// writer (one head update and a `WriterOpened` barrier).
    pub load_us: u64,
    /// Microseconds for each seal: a cut at the newest run input that starts an entry (when
    /// there is one), then a ledger seal on top.
    pub seal_us: Vec<u64>,
    /// Whether a cut was sealed.
    pub cut: bool,
    /// Microseconds per read of a sealed message by its ordinal, from a fresh load after the
    /// seals: the directory, the segment's index and one block, through the session's read path.
    pub expand_us: Vec<u64>,
    /// Messages read back by ordinal (window and sealed), each equal to the message the
    /// conversation placed there.
    pub read_back: u64,
    /// Distinct ids the written ledger records name, each found after the seals with exactly
    /// the seqs of the records that name it, and microseconds per lookup (the id index's pages,
    /// then each record's ledger segment, through the session's `find_id`).
    pub ids_checked: u64,
    pub id_lookup_us: Vec<u64>,
    /// Microseconds per setup part stored before the batch that names it. A part the store
    /// already held (from this session or an earlier one) asks the backend nothing and isn't
    /// timed.
    pub setup_put_us: Vec<u64>,
    /// After the seals, the cold step as if the session had been idle for years: whether it
    /// pruned the kept transcript (it doesn't when a run is unfinished), and the open log's size
    /// before and after it.
    pub cold_pruned: bool,
    pub open_before_cold: u64,
    pub open_after_cold: u64,
}

/// The setup parts `record` names.
fn named_parts(record: &Record) -> &[PartRef] {
    match record {
        Record::RunStart(start) => &start.setup,
        Record::SetupStub(stub) => &stub.parts,
        Record::SessionRefresh(refresh) => &refresh.setup,
        _ => &[],
    }
}

/// Writes a converted log as a real session on local files under `root`, as the live writer
/// would: it creates the session, then appends each group of records up to a barrier as one
/// batch, synced. On macOS a sync is `F_FULLFSYNC`, today's barrier. Then a fresh backend (as
/// after a restart) loads the session through the real load path, which must give the state the
/// writer held; that load is timed.
///
/// Every setup part a batch names is stored in `setup` before the batch is appended, as admission
/// must do (a record never names a part that isn't durable). After the reload, every named part
/// must read back from a store with nothing cached, checked against its digest.
///
/// Then it seals twice: a cut at the newest run input where a cut may fall, which must leave the
/// window as exactly the messages from there on, and a ledger seal on top. Then the cold step
/// runs as if the session had been idle for years, so it prunes the kept transcript unless a run
/// is unfinished. After each, the session reloads to the state its writer held. After the
/// reload, every message reads back by its ordinal, and every id the written ledger records name
/// is found, through the id index, with exactly the seqs of the records that name it.
pub fn append_log(
    records: &[Record],
    parts: &std::collections::BTreeMap<Digest, Vec<u8>>,
    setup: &SetupStore<'_, LocalBackend>,
    appends_in_conversation: bool,
    deployment: &DeploymentKey,
    root: &Path,
) -> Result<AppendMeasure, String> {
    let addr = SessionAddr("measure".into());
    let triple = SessionTriple {
        project_root: "/measure".into(),
        harness: "measure".into(),
        session: "measure".into(),
    };
    let backend = LocalBackend::new(root);
    let mut measure = AppendMeasure::default();
    let started = Instant::now();
    let written = block_on(async {
        let mut session = Session::create(
            &backend,
            addr.clone(),
            triple.clone(),
            deployment,
            appends_in_conversation,
        )
        .await
        .map_err(|e| format!("create: {e:?}"))?;
        let mut batch_start = 0;
        let mut run_inputs = Vec::new();
        let mut named: std::collections::BTreeMap<_, Vec<u64>> = std::collections::BTreeMap::new();
        for (index, record) in records.iter().enumerate() {
            if barrier_after(record, records.get(index + 1)) || index + 1 == records.len() {
                for part in records[batch_start..=index].iter().flat_map(named_parts) {
                    let bytes = parts
                        .get(&part.digest)
                        .ok_or("a record names a part the converter didn't keep")?;
                    let put = Instant::now();
                    let (stored, wrote) = setup
                        .put(part.kind, bytes)
                        .await
                        .map_err(|e| format!("setup put: {e:?}"))?;
                    if stored != *part {
                        return Err("a stored part's reference differs from the record's".into());
                    }
                    if wrote {
                        measure.setup_put_us.push(put.elapsed().as_micros() as u64);
                    }
                }
                let barrier = Instant::now();
                let first = session
                    .append(&records[batch_start..=index], true)
                    .await
                    .map_err(|e| format!("append: {e:?}"))?;
                for (offset, record) in records[batch_start..=index].iter().enumerate() {
                    if matches!(record, Record::Input(input) if input.placement == Placement::RunInput)
                    {
                        run_inputs.push(first + offset as u64);
                    }
                    for id in broca_session_store::idindex::record_ids(record) {
                        named.entry(id).or_default().push(first + offset as u64);
                    }
                }
                measure
                    .barrier_us
                    .push(barrier.elapsed().as_micros() as u64);
                measure.barriers += 1;
                batch_start = index + 1;
            }
        }
        measure.bytes = session.open_log().1;
        let state = (session.conversation.encode(), session.operations.encode());
        session.close().await.map_err(|e| format!("close: {e:?}"))?;
        Ok::<_, String>((state, run_inputs, named))
    })?;
    let (written, run_inputs, named) = written;
    measure.total_ms = started.elapsed().as_secs_f64() * 1000.0;

    let reopened = LocalBackend::new(root);
    let load = Instant::now();
    let loaded = block_on(Session::load(&reopened, addr.clone(), &triple, deployment))
        .map_err(|e| format!("load: {e:?}"))?;
    measure.load_us = load.elapsed().as_micros() as u64;
    if (loaded.conversation.encode(), loaded.operations.encode()) != written {
        return Err("the loaded session's state differs from the writer's".into());
    }
    drop(loaded);
    let fresh = SetupStore::new(setup.backend(), setup.key());
    for part in records.iter().flat_map(named_parts) {
        let bytes = block_on(fresh.get(part)).map_err(|e| format!("setup get: {e:?}"))?;
        if parts.get(&part.digest).map(Vec::as_slice) != Some(&*bytes) {
            return Err("a setup part read back differs from the converter's".into());
        }
    }

    let sealed = block_on(async {
        let mut session = Session::load(&reopened, addr.clone(), &triple, deployment)
            .await
            .map_err(|e| format!("load to seal: {e:?}"))?;
        let whole = session.conversation.messages.clone();
        let before: Vec<Vec<u8>> = whole.iter().map(|message| message.encode()).collect();
        for cut in run_inputs.iter().rev() {
            let started = Instant::now();
            match session.seal(Some(*cut), 0).await {
                Ok(_) => {
                    measure.seal_us.push(started.elapsed().as_micros() as u64);
                    measure.cut = true;
                    break;
                }
                Err(broca_session_store::session::SessionError::BadCut(_)) => continue,
                Err(e) => return Err(format!("cut at {cut}: {e:?}")),
            }
        }
        let after: Vec<Vec<u8>> = session
            .conversation
            .messages
            .iter()
            .map(|message| message.encode())
            .collect();
        if !before.ends_with(&after) {
            return Err("the window after the cut isn't the tail of the window before it".into());
        }
        let started = Instant::now();
        session
            .seal(None, 0)
            .await
            .map_err(|e| format!("ledger seal: {e:?}"))?;
        measure.seal_us.push(started.elapsed().as_micros() as u64);
        measure.open_before_cold = session.open_log().1;
        let far_future = u64::MAX / 2;
        match session
            .cold(far_future, 7 * 24 * 60 * 60 * 1000)
            .await
            .map_err(|e| format!("cold: {e:?}"))?
        {
            broca_session_store::session::Cold::Applied { pruned, .. } => {
                measure.cold_pruned = pruned
            }
            // Nothing in the open log carries a time: the session was left alone.
            broca_session_store::session::Cold::Unknown => {}
            other => return Err(format!("cold step: {other:?}")),
        }
        measure.open_after_cold = session.open_log().1;
        let state = (session.conversation.encode(), session.operations.encode());
        session.close().await.map_err(|e| format!("close: {e:?}"))?;
        Ok::<_, String>((state, whole))
    })?;
    let (sealed, whole) = sealed;
    let after_seals = LocalBackend::new(root);
    let loaded = block_on(Session::load(&after_seals, addr, &triple, deployment))
        .map_err(|e| format!("load after seals: {e:?}"))?;
    if (loaded.conversation.encode(), loaded.operations.encode()) != sealed {
        return Err("the session loaded after the seals differs from its writer's state".into());
    }
    // Every message reads back by its ordinal. A sealed message of a finished run lost its
    // opaque reasoning, so both sides are compared with opaque payloads removed.
    let window_start = loaded.conversation.window_start();
    let normalized = |message: &broca_protocol::content::Message| {
        let mut message = message.clone();
        broca_session_store::prune::prune(&mut message, &[0; 32]);
        message
    };
    for (index, expected) in whole.iter().enumerate() {
        let ordinal = index as u64 + 1;
        let started = Instant::now();
        let read = block_on(loaded.message(ordinal))
            .map_err(|e| format!("read message {ordinal}: {e:?}"))?
            .ok_or_else(|| format!("message {ordinal} reads as absent"))?;
        if ordinal < window_start {
            measure.expand_us.push(started.elapsed().as_micros() as u64);
        }
        if normalized(&read) != normalized(expected) {
            return Err(format!("message {ordinal} reads back differently"));
        }
        measure.read_back += 1;
    }
    for ((kind, id), seqs) in &named {
        let started = Instant::now();
        let found = block_on(loaded.find_id(*kind, id))
            .map_err(|e| format!("find {kind:?} {id}: {e:?}"))?;
        measure
            .id_lookup_us
            .push(started.elapsed().as_micros() as u64);
        let found: Vec<u64> = found.iter().map(|(seq, _)| *seq).collect();
        if &found != seqs {
            return Err(format!("{kind:?} {id} finds seqs {found:?}, not {seqs:?}"));
        }
        measure.ids_checked += 1;
    }
    Ok(measure)
}
