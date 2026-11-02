use super::*;
use broca_store::db::OverhaulDb;
use rusqlite::types::Value;

fn rows(db: &OverhaulDb, table: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    db.read_snapshot(|tx| {
        let filter = if table == "episode_seed" {
            " WHERE seq=(SELECT max(seq) FROM projection_boundary WHERE qualified=1)"
        } else {
            ""
        };
        let mut statement = tx.prepare(&format!("SELECT * FROM {table}{filter} ORDER BY 1,2"))?;
        let names = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect();
        let count = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..count)
                    .map(|column| row.get(column))
                    .collect::<rusqlite::Result<Vec<Value>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok((names, rows))
    })
    .unwrap()
}

fn difference(
    path: &str,
    expected: &serde_json::Value,
    actual: &serde_json::Value,
) -> Option<String> {
    if expected == actual {
        return None;
    }
    match (expected, actual) {
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            for key in left.keys().chain(right.keys()) {
                if let Some(found) =
                    difference(&format!("{path}/{key}"), &expected[key], &actual[key])
                {
                    return Some(found);
                }
            }
        }
        (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
            if left.len() != right.len() {
                return Some(format!(
                    "{path}: array lengths {} != {}",
                    left.len(),
                    right.len()
                ));
            }
            for (index, (left, right)) in left.iter().zip(right).enumerate() {
                if let Some(found) = difference(&format!("{path}/{index}"), left, right) {
                    return Some(found);
                }
            }
        }
        _ => return Some(format!("{path}: {expected:?} != {actual:?}")),
    }
    None
}

fn compare(normal: &OverhaulDb, bulk: &OverhaulDb) {
    for table in OverhaulDb::read_fixture_tables() {
        let (columns, expected) = rows(normal, table);
        let (actual_columns, actual) = rows(bulk, table);
        assert_eq!(columns, actual_columns, "{table}: columns");
        assert_eq!(expected.len(), actual.len(), "{table}: row count");
        for (row, (left, right)) in expected.iter().zip(&actual).enumerate() {
            for (column, (left, right)) in left.iter().zip(right).enumerate() {
                if left != right {
                    let detail = match (left, right) {
                        (Value::Text(left), Value::Text(right)) => {
                            match (
                                serde_json::from_str::<serde_json::Value>(left),
                                serde_json::from_str::<serde_json::Value>(right),
                            ) {
                                (Ok(left), Ok(right)) => difference("", &left, &right)
                                    .unwrap_or_else(|| {
                                        "serialized bytes differ despite equal JSON values".into()
                                    }),
                                _ => format!("{left:?} != {right:?}"),
                            }
                        }
                        _ => format!("{left:?} != {right:?}"),
                    };
                    panic!(
                        "reader-critical {table} row {row} column {} differs: {detail}",
                        columns[column]
                    );
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_fixture_matches_normal_publication_at_200_turns() {
    let template = partial_turn().await;
    for media in [false, true] {
        let normal_root = root(&format!("normal-parity-{media}"));
        let bulk_root = root(&format!("bulk-parity-{media}"));
        let bind = BindIdentity::new("/installed", "runner", format!("parity-{media}"));
        let normal = OverhaulDaemon::boot(&normal_root, WalAccessCounter::default()).await;
        let publication =
            publish_fixture(&normal, &bind, records(&template, 200, media), media, false);
        let bulk = OverhaulDb::open(&bulk_root).unwrap();
        bulk.publish_read_fixture(
            &bind,
            &broca_core::agent_session_ref(&bind),
            broca_store::db::LogLocation::Live,
            &publication,
        )
        .unwrap();
        compare(&normal.stores.overhaul.as_ref().unwrap().db, &bulk);
        println!("TURN_HEAVY_PARITY media={media} steps=200 tables={} final_seed_bytes=identical reader_rows=identical", OverhaulDb::read_fixture_tables().len());
        drop(bulk);
        normal.shutdown().await;
        std::fs::remove_dir_all(normal_root).unwrap();
        std::fs::remove_dir_all(bulk_root).unwrap();
    }
}
