use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};

use broca_protocol::BindIdentity;
use broca_subc::connection::{
    RouteHandle, RouteOpenRetry, SubcConnection, ROUTE_OPEN_RETRY_INTERVAL,
};
use ck_session_import::{CoordinateMap, ImportedBlockCoordinate};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// The magic-context module's subc id, the default for `--mc-module-id`.
pub const DEFAULT_MC_MODULE_ID: &str = "magic-context";
const FRAME_CAP_BYTES: usize = 1024 * 1024;
const BUNDLE_CAP_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireCompartment {
    pub seq: i64,
    pub start_message: u64,
    pub end_message: u64,
    pub end_message_id: String,
    pub title: String,
    pub p1: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p2: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p3: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p4: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub importance: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub episode_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StateImportRequest {
    kind: &'static str,
    v: u8,
    session_id: String,
    import_id: String,
    batch_seq: usize,
    batch_count: usize,
    compartments: Vec<WireCompartment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedStateImport {
    pub import_id: String,
    pub compartment_count: usize,
    pub batches: Vec<StateImportRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateImportOutcome {
    pub imported: usize,
    pub duplicate: bool,
}

#[derive(Default)]
struct BundleDigestWriter {
    digest: Sha256,
    bytes_written: usize,
}

impl Write for BundleDigestWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.digest.update(buffer);
        self.bytes_written += buffer.len();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub enum CompartmentLoad {
    None,
    Unavailable(String),
    Found(Vec<WireCompartment>),
}

#[derive(Debug)]
struct DatabaseCompartment {
    seq: i64,
    start_message: i64,
    end_message: i64,
    start_message_id: String,
    end_message_id: String,
    title: String,
    content: String,
    legacy: bool,
    p1: Option<String>,
    p2: Option<String>,
    p3: Option<String>,
    p4: Option<String>,
    importance: Option<i64>,
    episode_type: Option<String>,
}

pub fn default_context_db_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default()
        .join(".local/share/cortexkit/magic-context/context.db")
}

pub fn load_compartments(
    database: &Path,
    source_session_id: &str,
    coordinates: &CoordinateMap,
) -> Result<CompartmentLoad, String> {
    let connection = match Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(connection) => connection,
        Err(error) => {
            return Ok(CompartmentLoad::Unavailable(format!(
                "cannot read {}: {error}",
                database.display()
            )))
        }
    };
    let mut statement = match connection.prepare(
        "SELECT sequence, start_message, end_message, start_message_id, end_message_id, \
         title, content, legacy, p1, p2, p3, p4, importance, episode_type \
         FROM compartments WHERE harness = 'opencode' AND session_id = ?1 ORDER BY sequence",
    ) {
        Ok(statement) => statement,
        Err(error) => {
            return Ok(CompartmentLoad::Unavailable(format!(
                "cannot query {}: {error}",
                database.display()
            )))
        }
    };
    let mut rows = statement
        .query([source_session_id])
        .map_err(|error| format!("query magic-context compartments: {error}"))?;
    let mut database_rows = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("read magic-context compartment row: {error}"))?
    {
        database_rows.push(DatabaseCompartment {
            seq: row.get(0).map_err(row_error)?,
            start_message: row.get(1).map_err(row_error)?,
            end_message: row.get(2).map_err(row_error)?,
            start_message_id: row.get(3).map_err(row_error)?,
            end_message_id: row.get(4).map_err(row_error)?,
            title: row.get(5).map_err(row_error)?,
            content: row.get(6).map_err(row_error)?,
            legacy: row.get::<_, i64>(7).map_err(row_error)? == 1,
            p1: row.get(8).map_err(row_error)?,
            p2: row.get(9).map_err(row_error)?,
            p3: row.get(10).map_err(row_error)?,
            p4: row.get(11).map_err(row_error)?,
            importance: row.get(12).map_err(row_error)?,
            episode_type: row.get(13).map_err(row_error)?,
        });
    }
    if database_rows.is_empty() {
        return Ok(CompartmentLoad::None);
    }

    let mut compartments = Vec::with_capacity(database_rows.len());
    let mut previous_seq = None;
    for row in database_rows {
        if previous_seq.is_some_and(|previous| row.seq <= previous) {
            return Err(format!(
                "magic-context compartment seq {} is not strictly increasing",
                row.seq
            ));
        }
        previous_seq = Some(row.seq);
        let source_start = u64::try_from(row.start_message).map_err(|_| {
            format!(
                "magic-context compartment {} has negative start_message",
                row.seq
            )
        })?;
        let source_end = u64::try_from(row.end_message).map_err(|_| {
            format!(
                "magic-context compartment {} has negative end_message",
                row.seq
            )
        })?;
        let start =
            translate_start_boundary(coordinates, source_start, &row.start_message_id, row.seq)?;
        let end = translate_end_boundary(coordinates, source_end, &row.end_message_id, row.seq)?;
        if start.message_ordinal > end.message_ordinal {
            return Err(format!(
                "magic-context compartment {} translates to an inverted range {}..{}",
                row.seq, start.message_ordinal, end.message_ordinal
            ));
        }
        let end_message_id = end.flat_id();
        if !coordinates.contains_flat_id(&end_message_id) {
            return Err(format!(
                "magic-context compartment {} end_message_id {end_message_id} is absent from the imported message array",
                row.seq
            ));
        }
        let (p1, p2, p3, p4) = tier_texts(&row)?;
        compartments.push(WireCompartment {
            seq: row.seq,
            start_message: start.message_ordinal,
            end_message: end.message_ordinal,
            end_message_id,
            title: row.title,
            p1,
            p2,
            p3,
            p4,
            importance: row.importance,
            episode_type: row.episode_type,
            start_date: boundary_date(coordinates, source_start, &row.start_message_id),
            end_date: boundary_date(coordinates, source_end, &row.end_message_id),
        });
    }

    let last = compartments.last().expect("non-empty compartments");
    if !coordinates.contains_flat_id(&last.end_message_id) {
        return Err(format!(
            "last magic-context compartment end_message_id {} is absent from the imported message array",
            last.end_message_id
        ));
    }
    Ok(CompartmentLoad::Found(compartments))
}

fn row_error(error: rusqlite::Error) -> String {
    format!("decode magic-context compartment row: {error}")
}

type TierTexts = (String, Option<String>, Option<String>, Option<String>);

fn tier_texts(row: &DatabaseCompartment) -> Result<TierTexts, String> {
    let tiers = if row.legacy {
        (row.content.clone(), None, None, None)
    } else {
        (
            row.p1.clone().unwrap_or_default(),
            row.p2.clone(),
            row.p3.clone(),
            row.p4.clone(),
        )
    };
    if tiers.0.trim().is_empty() {
        return Err(format!(
            "magic-context compartment {} has empty p1",
            row.seq
        ));
    }
    Ok(tiers)
}

fn translate_start_boundary<'a>(
    coordinates: &'a CoordinateMap,
    ordinal: u64,
    message_id: &str,
    seq: i64,
) -> Result<&'a ImportedBlockCoordinate, String> {
    let coordinate = if message_id.is_empty() {
        coordinates.start_coordinate_for_ordinal(ordinal)
    } else {
        coordinates.start_coordinate_for_message_id(message_id)
    };
    require_boundary_coordinate(coordinate, ordinal, message_id, seq, "start")
}

fn translate_end_boundary<'a>(
    coordinates: &'a CoordinateMap,
    ordinal: u64,
    message_id: &str,
    seq: i64,
) -> Result<&'a ImportedBlockCoordinate, String> {
    let coordinate = if message_id.is_empty() {
        coordinates.coordinate_for_ordinal(ordinal)
    } else {
        coordinates.coordinate_for_message_id(message_id)
    };
    require_boundary_coordinate(coordinate, ordinal, message_id, seq, "end")
}

fn require_boundary_coordinate<'a>(
    coordinate: Option<&'a ImportedBlockCoordinate>,
    ordinal: u64,
    message_id: &str,
    seq: i64,
    boundary: &str,
) -> Result<&'a ImportedBlockCoordinate, String> {
    coordinate.ok_or_else(|| {
        let target = if message_id.is_empty() {
            format!("origin ordinal {ordinal}")
        } else {
            format!("origin message id {message_id}")
        };
        format!(
            "magic-context compartment {seq} {boundary} boundary {target} has no prior surviving imported block"
        )
    })
}

fn boundary_date(coordinates: &CoordinateMap, ordinal: u64, message_id: &str) -> Option<String> {
    if message_id.is_empty() {
        coordinates.date_for_ordinal(ordinal).map(str::to_owned)
    } else {
        coordinates
            .date_for_message_id(message_id)
            .map(str::to_owned)
    }
}

pub fn prepare_state_import(
    session_id: &str,
    compartments: Vec<WireCompartment>,
) -> Result<PreparedStateImport, String> {
    if compartments.is_empty() {
        return Err("cannot prepare an empty magic-context compartment bundle".into());
    }
    for pair in compartments.windows(2) {
        if pair[0].seq >= pair[1].seq {
            return Err(format!(
                "magic-context compartment seq {} is not strictly before {}",
                pair[0].seq, pair[1].seq
            ));
        }
    }
    let (import_id, encoded_compartment_lengths, bundle_len) = digest_bundle(&compartments)?;
    if bundle_len > BUNDLE_CAP_BYTES {
        return Err(format!(
            "magic-context compartment bundle is {bundle_len} bytes; state.import limit is {BUNDLE_CAP_BYTES} bytes"
        ));
    }

    // The empty request supplies the exact envelope cost. Each candidate then needs only its
    // already-measured JSON length plus a comma, instead of cloning and re-encoding the growing
    // batch for every compartment.
    let empty_probe_len = encoded_len(&request(
        session_id,
        &import_id,
        usize::MAX,
        usize::MAX,
        Vec::new(),
    ))?;
    let mut groups = Vec::<Vec<WireCompartment>>::new();
    let mut current = Vec::new();
    let mut current_encoded_len = empty_probe_len;
    for (compartment, compartment_len) in compartments.into_iter().zip(encoded_compartment_lengths)
    {
        let separator_len = usize::from(!current.is_empty());
        let candidate_len = current_encoded_len + separator_len + compartment_len;
        if candidate_len < FRAME_CAP_BYTES {
            current.push(compartment);
            current_encoded_len = candidate_len;
            continue;
        }
        if current.is_empty() {
            return Err(format!(
                "magic-context compartment {} cannot fit below the {FRAME_CAP_BYTES}-byte frame cap",
                compartment.seq
            ));
        }
        groups.push(std::mem::take(&mut current));
        current_encoded_len = empty_probe_len + compartment_len;
        current.push(compartment);
    }
    if !current.is_empty() {
        groups.push(current);
    }

    let batch_count = groups.len();
    let batches = groups
        .into_iter()
        .enumerate()
        .map(|(batch_seq, compartments)| {
            request(session_id, &import_id, batch_seq, batch_count, compartments)
        })
        .collect::<Vec<_>>();
    for batch in &batches {
        let len = encoded_len(batch)?;
        if len >= FRAME_CAP_BYTES {
            return Err(format!(
                "magic-context state.import batch {} is {len} bytes, not below the {FRAME_CAP_BYTES}-byte frame cap",
                batch.batch_seq
            ));
        }
    }
    Ok(PreparedStateImport {
        import_id,
        compartment_count: batches.iter().map(|batch| batch.compartments.len()).sum(),
        batches,
    })
}

fn digest_bundle(compartments: &[WireCompartment]) -> Result<(String, Vec<usize>, usize), String> {
    let mut writer = BundleDigestWriter::default();
    writer
        .write_all(b"[")
        .map_err(|error| format!("encode magic-context compartment bundle: {error}"))?;
    let mut encoded_lengths = Vec::with_capacity(compartments.len());
    for (index, compartment) in compartments.iter().enumerate() {
        if index != 0 {
            writer
                .write_all(b",")
                .map_err(|error| format!("encode magic-context compartment bundle: {error}"))?;
        }
        let before = writer.bytes_written;
        serde_json::to_writer(&mut writer, compartment)
            .map_err(|error| format!("encode magic-context compartment bundle: {error}"))?;
        encoded_lengths.push(writer.bytes_written - before);
    }
    writer
        .write_all(b"]")
        .map_err(|error| format!("encode magic-context compartment bundle: {error}"))?;
    Ok((
        format!("{:x}", writer.digest.finalize()),
        encoded_lengths,
        writer.bytes_written,
    ))
}

fn request(
    session_id: &str,
    import_id: &str,
    batch_seq: usize,
    batch_count: usize,
    compartments: Vec<WireCompartment>,
) -> StateImportRequest {
    StateImportRequest {
        kind: "state_import",
        v: 1,
        session_id: session_id.to_string(),
        import_id: import_id.to_string(),
        batch_seq,
        batch_count,
        compartments,
    }
}

fn encoded_len(request: &StateImportRequest) -> Result<usize, String> {
    serde_json::to_vec(request)
        .map(|bytes| bytes.len())
        .map_err(|error| format!("encode magic-context state.import batch: {error}"))
}

trait StateImportRoute {
    async fn request(&self, body: Vec<u8>) -> Result<Value, String>;
}

struct SubcStateImportRoute<'a> {
    connection: &'a SubcConnection,
    route: RouteHandle,
}

impl StateImportRoute for SubcStateImportRoute<'_> {
    async fn request(&self, body: Vec<u8>) -> Result<Value, String> {
        self.connection
            .route_request(&self.route, body)
            .await
            .map_err(|error| error.to_string())
    }
}

pub async fn submit_state_import(
    connection: &SubcConnection,
    module_id: &str,
    identity: &BindIdentity,
    bind_timeout: std::time::Duration,
    prepared: &PreparedStateImport,
) -> Result<StateImportOutcome, String> {
    let deadline = std::time::Instant::now() + bind_timeout;
    // The connection layer retries a temporary refusal within the bind timeout; this loop
    // additionally retries every other error until the same deadline.
    let retry = || RouteOpenRetry {
        budget: deadline.saturating_duration_since(std::time::Instant::now()),
        max_interval: ROUTE_OPEN_RETRY_INTERVAL,
    };
    let route = loop {
        match connection
            .route_open_management_within(module_id, identity, retry())
            .await
        {
            Ok(route) => break route,
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(error) => {
                return Err(format!("open {module_id} management route: {error}"));
            }
        }
    };
    submit_batches(
        &SubcStateImportRoute { connection, route },
        &prepared.batches,
    )
    .await
}

async fn submit_batches<R: StateImportRoute>(
    route: &R,
    batches: &[StateImportRequest],
) -> Result<StateImportOutcome, String> {
    for (index, batch) in batches.iter().enumerate() {
        let body = serde_json::to_vec(batch)
            .map_err(|error| format!("encode magic-context state.import batch: {error}"))?;
        let response = route.request(body).await.map_err(|error| {
            format!(
                "state.import batch {}/{} rejected: {error}",
                batch.batch_seq + 1,
                batch.batch_count
            )
        })?;
        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(format!(
                "state.import batch {}/{} returned an unsuccessful response: {response}",
                batch.batch_seq + 1,
                batch.batch_count
            ));
        }
        let final_batch = index + 1 == batches.len();
        if !final_batch {
            if response.get("staged").is_none_or(Value::is_null) {
                return Err(format!(
                    "state.import non-final batch response is missing staged status: {response}"
                ));
            }
            continue;
        }
        let imported = response
            .get("imported")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                format!("state.import final response is missing imported count: {response}")
            })?;
        let duplicate = response
            .get("duplicate")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                format!("state.import final response is missing duplicate status: {response}")
            })?;
        return Ok(StateImportOutcome {
            imported,
            duplicate,
        });
    }
    Err("state.import has no batches to submit".into())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use tokio::sync::{mpsc, oneshot};

    use super::*;

    fn compartment(seq: i64, p1_bytes: usize) -> WireCompartment {
        WireCompartment {
            seq,
            start_message: seq as u64,
            end_message: seq as u64,
            end_message_id: format!("m{seq}#0"),
            title: format!("compartment-{seq}"),
            p1: "x".repeat(p1_bytes),
            p2: None,
            p3: None,
            p4: None,
            importance: Some(50),
            episode_type: Some("work".into()),
            start_date: Some("2026-07-14".into()),
            end_date: Some("2026-07-15".into()),
        }
    }

    #[test]
    fn legacy_rows_use_content_as_p1_and_clear_other_tiers() {
        let row = DatabaseCompartment {
            seq: 7,
            start_message: 0,
            end_message: 1,
            start_message_id: String::new(),
            end_message_id: String::new(),
            title: "legacy".into(),
            content: "legacy content".into(),
            legacy: true,
            p1: None,
            p2: Some("must disappear".into()),
            p3: Some("must disappear".into()),
            p4: Some("must disappear".into()),
            importance: Some(50),
            episode_type: None,
        };
        assert_eq!(
            tier_texts(&row).unwrap(),
            ("legacy content".into(), None, None, None)
        );
    }

    #[test]
    fn legacy_database_row_loads_content_as_p1() {
        use ck_session_import::opencode::OpenCodeReader;
        use ck_session_import::HarnessReader;

        let source_db = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../ck-session-import/tests/fixtures/opencode.db");
        let imported = OpenCodeReader::new(source_db).convert("ses-text").unwrap();
        let path = broca_test_support::scratch_root()
            .join(format!("ck-import-mc-state-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE compartments (\
                   sequence INTEGER, start_message INTEGER, end_message INTEGER, \
                   start_message_id TEXT, end_message_id TEXT, title TEXT, content TEXT, \
                   legacy INTEGER, p1 TEXT, p2 TEXT, p3 TEXT, p4 TEXT, importance INTEGER, \
                   episode_type TEXT, harness TEXT, session_id TEXT\
                 );",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO compartments VALUES \
                 (5, 0, 1, 'm-text-user', 'm-text-assistant', 'legacy', 'carried content', \
                  1, NULL, NULL, NULL, NULL, 50, NULL, 'opencode', 'ses-text')",
                [],
            )
            .unwrap();
        drop(connection);

        let CompartmentLoad::Found(rows) =
            load_compartments(&path, "ses-text", imported.coordinate_map.as_ref().unwrap())
                .unwrap()
        else {
            panic!("legacy row should load");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].p1, "carried content");
        assert_eq!(
            (
                rows[0].p2.as_ref(),
                rows[0].p3.as_ref(),
                rows[0].p4.as_ref()
            ),
            (None, None, None)
        );
        assert_eq!(rows[0].start_message, 0);
        assert_eq!(rows[0].end_message_id, "m1#0");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn split_source_row_spans_every_imported_message_in_compartment_range() {
        use broca_protocol::content::Role;
        use ck_session_import::opencode::OpenCodeReader;
        use ck_session_import::HarnessReader;

        let source_db = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../ck-session-import/tests/fixtures/opencode.db");
        let imported = OpenCodeReader::new(source_db).convert("ses-tools").unwrap();
        assert_eq!(
            imported
                .messages
                .iter()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            vec![Role::User, Role::Assistant, Role::Tool]
        );

        let path = broca_test_support::scratch_root().join(format!(
            "ck-import-mc-state-split-row-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE compartments (\
                   sequence INTEGER, start_message INTEGER, end_message INTEGER, \
                   start_message_id TEXT, end_message_id TEXT, title TEXT, content TEXT, \
                   legacy INTEGER, p1 TEXT, p2 TEXT, p3 TEXT, p4 TEXT, importance INTEGER, \
                   episode_type TEXT, harness TEXT, session_id TEXT\
                 );",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO compartments VALUES \
                 (5, 2, 2, 'm-assistant', 'm-assistant', 'tool turn', '', \
                  0, 'assistant and tool result', NULL, NULL, NULL, 50, NULL, \
                  'opencode', 'ses-tools')",
                [],
            )
            .unwrap();
        drop(connection);

        let CompartmentLoad::Found(rows) = load_compartments(
            &path,
            "ses-tools",
            imported.coordinate_map.as_ref().unwrap(),
        )
        .unwrap() else {
            panic!("compartment should load");
        };
        assert_eq!(
            (
                rows[0].start_message,
                rows[0].end_message,
                rows[0].end_message_id.as_str()
            ),
            (1, 2, "m2#0")
        );
        let _ = std::fs::remove_file(path);
    }

    /// Widening a compartment's start must not make it overlap the one before it.
    ///
    /// A source row can import as several messages, so a compartment's start edge now resolves
    /// to the first of them rather than the last. That moves start edges EARLIER, and the
    /// consumer rejects a compartment set outright when one compartment starts at or before the
    /// previous one's end -- so a widened start that reaches back into its predecessor's range
    /// would fail the whole import at the far side rather than here, where the range was built.
    ///
    /// Adjacent compartments over consecutive source rows are the case that would collide, so
    /// they are what this pins.
    #[test]
    fn widened_starts_do_not_overlap_the_preceding_compartment() {
        use ck_session_import::opencode::OpenCodeReader;
        use ck_session_import::HarnessReader;

        let source_db = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../ck-session-import/tests/fixtures/opencode.db");
        let imported = OpenCodeReader::new(source_db).convert("ses-tools").unwrap();

        let path = broca_test_support::scratch_root().join(format!(
            "ck-import-mc-state-overlap-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE compartments (\
                   sequence INTEGER, start_message INTEGER, end_message INTEGER, \
                   start_message_id TEXT, end_message_id TEXT, title TEXT, content TEXT, \
                   legacy INTEGER, p1 TEXT, p2 TEXT, p3 TEXT, p4 TEXT, importance INTEGER, \
                   episode_type TEXT, harness TEXT, session_id TEXT\
                 );",
            )
            .unwrap();
        // Two compartments over adjacent source rows: the first ends on the user row, the
        // second covers the assistant row that imports as two messages.
        connection
            .execute(
                "INSERT INTO compartments VALUES \
                 (4, 1, 1, 'm-user', 'm-user', 'user turn', '', \
                  0, 'user', NULL, NULL, NULL, 50, NULL, 'opencode', 'ses-tools'), \
                 (5, 2, 2, 'm-assistant', 'm-assistant', 'tool turn', '', \
                  0, 'assistant and tool result', NULL, NULL, NULL, 50, NULL, \
                  'opencode', 'ses-tools')",
                [],
            )
            .unwrap();
        drop(connection);

        let CompartmentLoad::Found(rows) = load_compartments(
            &path,
            "ses-tools",
            imported.coordinate_map.as_ref().unwrap(),
        )
        .unwrap() else {
            panic!("compartments should load");
        };
        assert_eq!(rows.len(), 2, "fixture must have a predecessor to overlap");

        for pair in rows.windows(2) {
            assert!(
                pair[1].start_message > pair[0].end_message,
                "compartment {} starts at {} but compartment {} ends at {}; the consumer \
                 rejects a set where one compartment starts at or before the previous end",
                pair[1].seq,
                pair[1].start_message,
                pair[0].seq,
                pair[0].end_message
            );
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn large_bundle_splits_into_contiguous_sub_mebibyte_batches() {
        let compartments = vec![compartment(3, 600_000), compartment(4, 600_000)];
        let prepared = prepare_state_import("target-session", compartments.clone()).unwrap();
        let rerun = prepare_state_import("another-target", compartments).unwrap();
        assert_eq!(prepared.import_id, rerun.import_id);
        assert_eq!(prepared.batches.len(), 2);
        assert_eq!(prepared.batches[0].batch_seq, 0);
        assert_eq!(prepared.batches[1].batch_seq, 1);
        assert_eq!(prepared.batches[0].batch_count, 2);
        assert!(prepared
            .batches
            .iter()
            .all(|batch| encoded_len(batch).unwrap() < FRAME_CAP_BYTES));
        assert_eq!(prepared.batches[0].compartments[0].seq, 3);
        assert_eq!(prepared.batches[1].compartments[0].seq, 4);
    }

    type FakeRequest = (Vec<u8>, oneshot::Sender<Result<Value, String>>);

    struct FakeRoute {
        requests: mpsc::Sender<FakeRequest>,
    }

    impl StateImportRoute for FakeRoute {
        async fn request(&self, body: Vec<u8>) -> Result<Value, String> {
            let (response_tx, response_rx) = oneshot::channel();
            self.requests
                .send((body, response_tx))
                .await
                .map_err(|_| "fake route server stopped".to_string())?;
            response_rx
                .await
                .map_err(|_| "fake route response dropped".to_string())?
        }
    }

    fn fake_route(
        responses: impl IntoIterator<Item = Result<Value, String>>,
    ) -> (FakeRoute, tokio::task::JoinHandle<Vec<Value>>) {
        let (request_tx, mut request_rx) = mpsc::channel::<FakeRequest>(4);
        let mut responses = responses.into_iter().collect::<VecDeque<_>>();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            while let Some((body, response_tx)) = request_rx.recv().await {
                requests.push(serde_json::from_slice(&body).unwrap());
                let response = responses.pop_front().expect("response for request");
                let _ = response_tx.send(response);
            }
            requests
        });
        (
            FakeRoute {
                requests: request_tx,
            },
            server,
        )
    }

    #[tokio::test]
    async fn wire_batches_are_exact_and_final_response_is_decoded() {
        let prepared = prepare_state_import(
            "target-session",
            vec![compartment(3, 600_000), compartment(4, 600_000)],
        )
        .unwrap();
        let (route, server) = fake_route([
            Ok(serde_json::json!({"ok": true, "staged": true})),
            Ok(serde_json::json!({
                "ok": true,
                "imported": 2,
                "duplicate": false
            })),
        ]);
        let outcome = submit_batches(&route, &prepared.batches).await.unwrap();
        assert_eq!(
            outcome,
            StateImportOutcome {
                imported: 2,
                duplicate: false
            }
        );
        drop(route);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec![
                "batch_count",
                "batch_seq",
                "compartments",
                "import_id",
                "kind",
                "session_id",
                "v",
            ]
        );
        assert_eq!(requests[0]["kind"], "state_import");
        assert_eq!(requests[0]["v"], 1);
        assert_eq!(requests[0]["session_id"], "target-session");
        assert_eq!(requests[0]["import_id"], prepared.import_id);
        assert_eq!(requests[0]["batch_seq"], 0);
        assert_eq!(requests[1]["batch_seq"], 1);
        assert_eq!(requests[1]["batch_count"], 2);
        assert_eq!(requests[0]["compartments"][0]["seq"], 3);
        assert_eq!(requests[1]["compartments"][0]["seq"], 4);
        let compartment = requests[0]["compartments"][0].as_object().unwrap();
        assert!(compartment.get("p2").is_none());
        assert!(compartment.get("start_message_id").is_none());
        assert!(compartment.get("p1_embedding").is_none());
    }

    #[test]
    fn streamed_bundle_measurement_matches_canonical_json() {
        let mut compartments = vec![compartment(3, 10), compartment(4, 20)];
        compartments[0].title = "quotes: \" and slash: \\ and newline: \n".into();
        compartments[1].p2 = Some("nested-looking text: [{\"key\":1}]".into());

        let canonical = serde_json::to_vec(&compartments).unwrap();
        let (import_id, encoded_lengths, bundle_len) = digest_bundle(&compartments).unwrap();

        assert_eq!(bundle_len, canonical.len());
        assert_eq!(import_id, format!("{:x}", Sha256::digest(&canonical)));
        assert_eq!(
            encoded_lengths,
            compartments
                .iter()
                .map(|compartment| serde_json::to_vec(compartment).unwrap().len())
                .collect::<Vec<_>>()
        );
    }

    /// Batch composition must agree with the encoder it replaced, byte for byte.
    ///
    /// Batching is now sized arithmetically rather than by re-encoding each candidate, so the
    /// separator and envelope accounting has to reproduce what serde_json would emit. An error
    /// there packs the wrong number of compartments per frame: too few wastes round trips, and
    /// too many is caught only by the final re-serialization check, which reports a frame-cap
    /// failure on input that is actually within the cap.
    ///
    /// Sizes are chosen so a batch boundary falls mid-run, which is what makes a one-byte
    /// separator drift observable as a different split.
    #[test]
    fn arithmetic_batching_splits_where_reencoding_would() {
        // ~40 KiB each, so roughly 26 fit under the 1 MiB frame cap and the boundary lands
        // partway through the run rather than at either end.
        let compartments: Vec<WireCompartment> =
            (0..60).map(|seq| compartment(seq, 40_000)).collect();

        let prepared = prepare_state_import("target", compartments.clone()).unwrap();
        let arithmetic_split: Vec<usize> = prepared
            .batches
            .iter()
            .map(|batch| batch.compartments.len())
            .collect();

        // Independently recompute the split the way the previous implementation did: grow a
        // batch until the fully re-encoded request would reach the cap.
        let mut reencoded_split = Vec::new();
        let mut current: Vec<WireCompartment> = Vec::new();
        for compartment in compartments {
            current.push(compartment);
            let probe = request(
                "target",
                &prepared.import_id,
                usize::MAX,
                usize::MAX,
                current.clone(),
            );
            if encoded_len(&probe).unwrap() < FRAME_CAP_BYTES {
                continue;
            }
            let last = current.pop().expect("probe contains the new compartment");
            reencoded_split.push(current.len());
            current.clear();
            current.push(last);
        }
        if !current.is_empty() {
            reencoded_split.push(current.len());
        }

        assert!(
            arithmetic_split.len() > 1,
            "fixture must span several frames or the split proves nothing"
        );
        assert_eq!(
            arithmetic_split, reencoded_split,
            "arithmetic sizing must pack each frame exactly as re-encoding would; a drift here \
             silently changes how compartments are distributed across frames"
        );
    }

    /// Every frame but the last must be packed as full as the cap allows.
    ///
    /// Batch sizing is arithmetic now rather than a re-encode per candidate, so the separator
    /// and envelope accounting has to reproduce what serde_json emits. Undercounting packs too
    /// many compartments and the final re-serialization check rejects a bundle that was
    /// actually within the cap; overcounting silently wastes round trips.
    ///
    /// Asserting maximal packing rather than comparing against a re-encoding reference keeps
    /// this linear: the reference would be the quadratic path this change removed, which costs
    /// minutes at the element counts needed to make a per-element drift observable.
    #[test]
    fn each_full_frame_is_packed_to_the_cap() {
        // Small elements, many of them. A per-element accounting error accumulates across a
        // batch, so it only shows up once the total drift exceeds one element; large elements
        // would absorb it and the assertion would hold whatever the arithmetic did.
        let compartments: Vec<WireCompartment> =
            (0..12_000).map(|seq| compartment(seq, 120)).collect();

        let prepared = prepare_state_import("target", compartments).unwrap();
        assert!(
            prepared.batches.len() > 1,
            "fixture must span several frames or packing proves nothing"
        );

        for (index, batch) in prepared.batches.iter().enumerate() {
            let encoded = encoded_len(batch).unwrap();
            assert!(
                encoded < FRAME_CAP_BYTES,
                "batch {index} is {encoded} bytes, at or over the {FRAME_CAP_BYTES} cap"
            );

            // Every frame except the last must be unable to take one more element: that is what
            // distinguishes correct sizing from sizing that merely stays under the cap.
            let Some(next) = prepared.batches.get(index + 1) else {
                break;
            };
            let mut overfull = batch.clone();
            overfull.compartments.push(
                next.compartments
                    .first()
                    .expect("a batch is never empty")
                    .clone(),
            );
            assert!(
                encoded_len(&overfull).unwrap() >= FRAME_CAP_BYTES,
                "batch {index} had room for the next compartment, so sizing is over-conservative"
            );
        }
    }

    #[tokio::test]
    async fn duplicate_and_structural_rejections_surface() {
        let prepared = prepare_state_import("target", vec![compartment(0, 10)]).unwrap();
        let (duplicate, duplicate_server) = fake_route([Ok(serde_json::json!({
            "ok": true,
            "imported": 1,
            "duplicate": true
        }))]);
        assert!(
            submit_batches(&duplicate, &prepared.batches)
                .await
                .unwrap()
                .duplicate
        );
        drop(duplicate);
        duplicate_server.await.unwrap();

        let (rejected, rejected_server) = fake_route([Err("seq_not_increasing".to_string())]);
        let error = submit_batches(&rejected, &prepared.batches)
            .await
            .unwrap_err();
        assert!(error.contains("seq_not_increasing"));
        drop(rejected);
        rejected_server.await.unwrap();
    }
}
