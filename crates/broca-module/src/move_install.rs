//! Filesystem installation primitives used under cut and session locks.
//! Before publishing imported WALs, the billing floor durably names the runs
//! executed elsewhere, so rebuilding a projection cannot bill those runs again.
//! A pending activation intent fences actor writes until the whole move is recorded.
use crate::cut_state::refusal;
use broca_protocol::BindIdentity;
use broca_wire::agent_data::errors::ErrorBody;
use serde_json::{json, Value};
use std::{fs, io, path::Path};

pub struct Installation<'a> {
    pub session: &'a BindIdentity,
    pub staged: &'a Path,
    pub live: &'a Path,
    pub local_archive: Option<&'a [u8]>,
    pub end_offset: u64,
    pub digest: &'a str,
}

fn unavailable(error: impl std::fmt::Display) -> ErrorBody {
    refusal("unavailable", error.to_string(), Value::Null)
}
fn bytes(path: &Path) -> Result<Vec<u8>, ErrorBody> {
    match fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(unavailable(e)),
    }
}

/// Check all sessions before committing billing exclusions, renaming a WAL,
/// removing a moved tombstone or dropping a derived projection. The caller holds
/// all affected actors' dispatch sections and leases protecting their WAL files.
pub fn check_installations(installations: &[Installation<'_>]) -> Result<(), ErrorBody> {
    for item in installations {
        let incoming = if item.staged.try_exists().map_err(unavailable)? {
            bytes(item.staged)?
        } else {
            bytes(item.live)?
        };
        if incoming.len() as u64 != item.end_offset
            || broca_wal::migration::digest(&incoming) != item.digest
        {
            return Err(refusal(
                "digest_mismatch",
                "staged or installed WAL does not match cut",
                json!({"session":item.session.session}),
            ));
        }
        let local = if item.live.try_exists().map_err(unavailable)? {
            bytes(item.live)?
        } else {
            item.local_archive.unwrap_or_default().to_vec()
        };
        if !incoming.starts_with(&local) {
            return Err(refusal(
                "local_advanced",
                "local WAL is not a prefix of the incoming cut",
                json!({"session":item.session.session}),
            ));
        }
        if broca_wal::migration::read_json::<broca_wal::migration::Freeze>(
            &item.live.with_extension("moving"),
        )
        .map_err(unavailable)?
        .is_some()
        {
            return Err(refusal("busy", "local session has a freeze", Value::Null));
        }
    }
    Ok(())
}

/// Commit imported-run billing exclusions before publishing any WAL. A missing
/// staged file counts as a completed rename only under a pending activate intent,
/// which prevents sends from advancing the installed bytes before recovery checks
/// them. Without that intent, do not invoke the missing-staging recovery path.
/// `drop_projection` removes derived session data, never billing facts or floors.
pub fn install_verified(
    cut_id: &str,
    installations: &[Installation<'_>],
    export: &dyn broca_core::ExportIndex,
    drop_projection: impl Fn(&BindIdentity) -> Result<(), String>,
) -> Result<(), ErrorBody> {
    check_installations(installations)?;
    for item in installations {
        let staged = item.staged.try_exists().map_err(unavailable)?;
        if staged {
            let incoming =
                broca_wal::replay_file(item.staged, item.end_offset).map_err(unavailable)?;
            let local = if item.live.try_exists().map_err(unavailable)? {
                broca_wal::replay_file(item.live, item.end_offset).map_err(unavailable)?
            } else {
                match item.local_archive {
                    Some(bytes) => {
                        match broca_wal::file_wal::read_verified_member_range(bytes, 0, 0, None)
                            .map_err(unavailable)?
                            .into_records()
                        {
                            broca_wal::WalRangeRecords::Complete(records) => records,
                            _ => return Err(unavailable("archived local WAL incomplete")),
                        }
                    }
                    None => Vec::new(),
                }
            };
            let imported = broca_core::imported_run_ids(&incoming, &local);
            export
                .record_imported_runs(item.session, cut_id, &imported)
                .map_err(unavailable)?;
            let parent = item
                .live
                .parent()
                .ok_or_else(|| unavailable("live WAL has no directory"))?;
            broca_wal::migration::durable_dir(parent).map_err(unavailable)?;
            fs::rename(item.staged, item.live).map_err(unavailable)?;
            broca_wal::migration::sync_dir(parent).map_err(unavailable)?;
            broca_wal::migration::sync_dir(item.staged.parent().unwrap()).map_err(unavailable)?;
        } else {
            // The imported-run ids were excluded from billing before rename.
            // The installed WAL no longer tells us which ids existed locally before
            // replacement, so preserve those exclusions rather than expanding them.
            export
                .record_imported_runs(item.session, cut_id, &[])
                .map_err(unavailable)?;
        }
        broca_wal::migration::unlink(&item.live.with_extension("moved")).map_err(unavailable)?;
        drop_projection(item.session).map_err(unavailable)?;
    }
    Ok(())
}

/// Call before asking ENGRAM's session-door service for WAL bytes. A receiver
/// must understand every advertised reader feature, even when the first frame
/// does not use that feature.
pub fn check_format(format: &str) -> Result<(), ErrorBody> {
    let (version, features) = format.split_once(";features=").ok_or_else(|| {
        refusal(
            "incompatible_version",
            "unknown WAL cut format",
            json!({"found":format,"required":"broca-wal/v3"}),
        )
    })?;
    let known_version = version == "broca-wal/v1" || version == "broca-wal/v3";
    let known_features = features
        .split(',')
        .filter(|f| !f.is_empty())
        .all(|f| broca_wal::framing::KNOWN_FEATURES.contains(&f));
    if !known_version || !known_features {
        return Err(refusal(
            "incompatible_version",
            "WAL cut requires an unknown reader feature",
            json!({"found":format,"required":broca_wal::framing::KNOWN_FEATURES}),
        ));
    }
    Ok(())
}

/// Verify the SHA-256 that ENGRAM's session-door returns for the immutable cut.
/// Select the RunStarted bind identity matching the shared session ref, rather
/// than accepting an address supplied by a transport or filename.
pub fn verify_received(
    reference: &str,
    incoming: &[u8],
    end_offset: u64,
    digest: &str,
) -> Result<BindIdentity, ErrorBody> {
    if incoming.len() as u64 != end_offset || broca_wal::migration::digest(incoming) != digest {
        return Err(refusal(
            "digest_mismatch",
            "incoming WAL does not match the sealed cut digest",
            Value::Null,
        ));
    }
    let range = broca_wal::file_wal::read_verified_member_range(incoming, 0, 0, None)
        .map_err(unavailable)?;
    let records = match range.records() {
        broca_wal::WalRangeRecords::Complete(records) => records,
        _ => return Err(unavailable("incoming WAL is incomplete")),
    };
    let mut triples = std::collections::BTreeMap::new();
    for frame in records {
        if let broca_wal::WalRecord::RunStarted { session, .. } = &frame.record {
            triples
                .entry((
                    session.project_root.as_os_str().to_owned(),
                    session.harness.clone(),
                    session.session.clone(),
                ))
                .or_insert_with(|| session.clone());
        }
    }
    let session = triples
        .into_values()
        .find(|session| {
            broca_wire::agent_data::session_ref(
                &session.harness,
                &session.project_root.to_string_lossy(),
                &session.session,
            ) == reference
        })
        .ok_or_else(|| {
            refusal(
                "invalid_request",
                format!("no WAL RunStarted triple matches ref {reference}"),
                json!({"ref":reference}),
            )
        })?;
    // Renaming a WAL to another bind identity preserves historical RunStarted
    // records. Until a run starts under the new harness/project/session triple,
    // no record can prove its current shared ref. Refuse that gap instead of
    // choosing a historical address; resolving it needs a registry lookup outside
    // the WAL participant.
    session.validate_keyable().map_err(|error| {
        refusal(
            "invalid_request",
            format!("invalid bind triple for ref {reference}: {error}"),
            json!({"ref":reference}),
        )
    })?;
    Ok(session)
}

#[cfg(test)]
pub(crate) mod tests;
