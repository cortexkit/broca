use super::*;
use broca_module::move_files::{
    store::{failure, store_busy},
    LocalDivergence,
};
use broca_session_store::{
    backend::{LocalBackend, SessionAddr},
    session::{
        moves::{self, Export},
        Session,
    },
};

fn stream_lost(error: impl std::fmt::Debug) -> ErrorBody {
    tracing::warn!(error = ?error, "session move stream copy could not be verified");
    refusal(
        "unavailable",
        "move_stream_lost: cached door prefix or partial frame tail is missing or damaged",
        json!({"reason":"move_stream_lost"}),
    )
}

impl Driver {
    pub(super) async fn cache_stream(
        &self,
        cut: &str,
        plan: &PlannedSession,
        op: Op,
    ) -> Result<(), ErrorBody> {
        use std::io::Write as _;
        let addr = SessionAddr(broca_core::session_addr(&plan.session));
        let path = self.root.join("session-store-moves/streams").join(&addr.0);
        if let Some(prefix) = plan.store_prefix {
            let covered = self
                .door
                .durable_offset(&plan.reference)
                .await
                .map_err(unavailable)?;
            if moves::stream_prefix(&path, covered).map_err(stream_lost)? != prefix {
                return Err(stream_lost("door prefix changed before freeze"));
            }
            if prefix != 0 {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .map_err(unavailable)?;
                file.set_len(prefix)
                    .and_then(|_| file.sync_all())
                    .map_err(unavailable)?;
                if prefix > covered {
                    self.door
                        .export_file(&plan.reference, &path)
                        .await
                        .map_err(unavailable)?;
                }
            }
            let (end, digest) =
                moves::append_stream_file(&path, prefix, &plan.bytes).map_err(stream_lost)?;
            if end != plan.end_offset || digest != plan.digest {
                return Err(stream_lost("source prefix changed"));
            }
            return Ok(());
        }
        let parent = path.parent().unwrap();
        broca_records::move_fence::durable_dir(&self.root, parent).map_err(unavailable)?;
        let temp = path.with_extension(cut);
        let mut file = broca_owner_only::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)
            .map_err(unavailable)?;
        let mut offset = 0;
        let mut hash = sha2::Sha256::new();
        use sha2::Digest as _;
        loop {
            let page = self
                .door
                .read(&plan.reference, offset, cut)
                .await
                .map_err(|error| door_failure(op, error).refusal)?;
            if page.stream_end != plan.end_offset
                || page.digest != plan.digest
                || page.from_offset != offset
                || page.end_offset <= offset
            {
                return Err(unavailable(
                    "sealed prefix changed while caching store move",
                ));
            }
            file.write_all(&page.bytes).map_err(unavailable)?;
            hash.update(&page.bytes);
            offset = page.end_offset;
            if offset == plan.end_offset {
                break;
            }
        }
        if broca_session_store::codec::hex(&hash.finalize()) != plan.digest {
            return Err(refusal(
                "digest_mismatch",
                "cached stream differs from sealed prefix",
                Value::Null,
            ));
        }
        file.sync_all().map_err(unavailable)?;
        drop(file);
        std::fs::rename(&temp, &path).map_err(unavailable)?;
        broca_records::move_fence::sync_dir(parent).map_err(unavailable)?;
        Ok(())
    }
    pub(super) async fn incoming_store_plan(
        &self,
        reference: &str,
        incoming: Incoming,
    ) -> Result<PlannedSession, ErrorBody> {
        let Incoming {
            bytes, end, digest, ..
        } = incoming;
        let frame = moves::last_export(&bytes).map_err(failure)?;
        let header = moves::header(frame).map_err(failure)?;
        let Some(runtime) = self.store() else {
            return Err(header.deployment_key.map_or_else(
                || unavailable("store runtime unavailable"),
                |id| failure(moves::Error::DeploymentKeyMissing(id)),
            ));
        };
        let session = BindIdentity {
            project_root: header.triple.project_root.clone().into(),
            harness: header.triple.harness.clone(),
            session: header.triple.session.clone(),
        };
        if !contract::matches_session_ref(
            reference,
            &session.harness,
            &session.project_root.to_string_lossy(),
            &session.session,
        ) {
            return Err(refusal(
                "invalid_request",
                "store triple does not match ref",
                Value::Null,
            ));
        }
        let addr = SessionAddr(broca_core::session_addr(&session));
        let export = Export::decode(frame, &addr, runtime.deployment()).map_err(failure)?;
        let (head, _) = export.head(&addr, runtime.deployment()).map_err(failure)?;
        if !head.features.is_empty() {
            return Err(refusal(
                "incompatible_version",
                "store cut requires unknown reader features",
                json!({"found":head.features,"required":[]}),
            ));
        }
        export
            .validate(&addr, runtime.deployment())
            .await
            .map_err(failure)?;
        let checks = super::snapshot::store_snapshot(&export, runtime.deployment())
            .await
            .map_err(unavailable)?;
        if checks != export.checks {
            return Err(refusal(
                "digest_mismatch",
                "store verification digests differ or are missing",
                Value::Null,
            ));
        }
        let (head, _) = export.head(&addr, runtime.deployment()).map_err(failure)?;
        let format = format!("{};features={}", moves::FORMAT, head.features.join(","));
        Ok(PlannedSession {
            store_prefix: None,
            reference: reference.into(),
            session,
            end_offset: end,
            digest: digest.clone(),
            sealed_digest: Some(digest),
            bytes,
            format,
            lineage_id: broca_session_store::codec::hex(&head.session_id.0),
            local_archive: None,
            divergence: None,
            divergence_reason: None,
        })
    }

    pub(super) async fn store_plan(
        &self,
        reference: &str,
        session: &BindIdentity,
        cut: &str,
        op: Op,
    ) -> Result<Option<PlannedSession>, ErrorBody> {
        let Some(runtime) = self.store() else {
            return Ok(None);
        };
        let held = runtime.class_of(session).map_err(unavailable)?;
        let addr = broca_core::session_addr(session);
        if op == Op::Abort
            && (held.is_some()
                || self
                    .root
                    .join("session-store-moves/staging")
                    .join(cut)
                    .join(&addr)
                    .exists()
                || self
                    .root
                    .join("session-store-moves/streams")
                    .join(&addr)
                    .exists())
        {
            return Ok(Some(PlannedSession {
                store_prefix: None,
                reference: reference.into(),
                session: session.clone(),
                end_offset: 0,
                digest: String::new(),
                sealed_digest: None,
                bytes: vec![],
                format: moves::FORMAT.into(),
                lineage_id: String::new(),
                local_archive: None,
                divergence: None,
                divergence_reason: None,
            }));
        }
        let Some(class) = held else {
            return Ok(None);
        };
        if matches!(op, Op::Import | Op::Activate) {
            let bytes = self
                .incoming(reference, cut, op)
                .await
                .map_err(|error| error.refusal)?;
            return self.incoming_store_plan(reference, bytes).await.map(Some);
        }
        // A session kept with retention lives in the ephemeral class and is deleted when its
        // deadline passes; a move would keep a copy of it beyond that on another computer.
        if op == Op::Prepare && class == broca_records::session_storage::StorageClass::Ephemeral {
            return Err(refusal(
                "not_exportable",
                "retention session must not leave this computer",
                Value::Null,
            ));
        }
        let backend = LocalBackend::new(class.store_root(&self.root));
        let addr = SessionAddr(broca_core::session_addr(session));
        let (_, current) = backend
            .read_stored_head(&addr)
            .map_err(|e| unavailable(format!("{e:?}")))?
            .ok_or_else(|| unavailable("store head missing"))?;
        let freeze: Option<broca_records::move_fence::Freeze> =
            broca_records::move_fence::read_json(
                &self
                    .root
                    .join("session-store-moves/fences")
                    .join(&addr.0)
                    .with_extension("moving"),
            )
            .map_err(unavailable)?;
        let mut version = freeze
            .as_ref()
            .and_then(|freeze| freeze.store_head_version)
            .unwrap_or(current);
        if freeze.is_none() {
            let stored = backend
                .read_stored_head(&addr)
                .map_err(|e| unavailable(format!("{e:?}")))?
                .unwrap()
                .0;
            let (head, _) = broca_session_store::head::open(&stored, runtime.deployment())
                .map_err(|e| unavailable(format!("{e:?}")))?;
            if matches!(
                head.residence,
                broca_session_store::head::Residence::Moved(_)
            ) {
                for kept in backend
                    .head_versions(&addr)
                    .map_err(|e| unavailable(format!("{e:?}")))?
                    .into_iter()
                    .rev()
                {
                    let bytes = backend
                        .read_head_version(&addr, kept)
                        .map_err(|e| unavailable(format!("{e:?}")))?
                        .unwrap();
                    let (head, _) = broca_session_store::head::open(&bytes, runtime.deployment())
                        .map_err(|e| unavailable(format!("{e:?}")))?;
                    if matches!(
                        head.residence,
                        broca_session_store::head::Residence::Local(())
                    ) {
                        version = kept;
                        break;
                    }
                }
            }
        }
        if op == Op::Busy && freeze.is_none() && version == current {
            return self
                .busy_store_plan(reference, session, &backend, &addr, runtime.deployment())
                .await
                .map(Some);
        }
        let mut export =
            Export::pinned(&backend, &addr, version, class.name(), runtime.deployment())
                .await
                .map_err(failure)?;
        let memory = export
            .memory(&addr, runtime.deployment())
            .await
            .map_err(failure)?;
        let view = Session::view(
            &memory,
            addr.clone(),
            &export.header.triple,
            runtime.deployment(),
        )
        .await
        .map_err(|e| unavailable(format!("{e:?}")))?;
        let busy = store_busy(&view);
        // A prepare of a session with pending work is refused before any door traffic.
        if let (Op::Prepare, Some(reason)) = (op, busy) {
            return Err(broca_module::move_files::busy(
                vec![broca_module::move_files::busy_ref(
                    reference,
                    reason,
                    "source has pending work",
                )],
                "source has pending work",
            ));
        }
        // The move checks travel with an export; `busy` and `diverged` send none.
        if busy.is_none() && !matches!(op, Op::Busy | Op::Diverged) {
            export.checks = super::snapshot::store_snapshot(&export, runtime.deployment())
                .await
                .map_err(unavailable)?;
        }
        let (head, _) = export.head(&addr, runtime.deployment()).map_err(failure)?;
        let cached = self.root.join("session-store-moves/streams").join(&addr.0);
        // A session that never moved has no cached stream; `diverged` compares it by its files.
        let streamless = op == Op::Diverged && !cached.exists();
        let (bytes, end_offset, digest, store_prefix) = if op == Op::Busy || streamless {
            // A busy answer names only the session's state and format, never a stream.
            (Vec::new(), 0, String::new(), None)
        } else if op == Op::Prepare {
            // The door's offsets carry no digest, so prepare checks only the local frame hashes.
            // Import checks the whole-prefix digest returned by session.read {cutId}; admit
            // sends only the suffix and cannot remotely compare already-covered bytes.
            let covered = self
                .door
                .durable_offset(reference)
                .await
                .map_err(unavailable)?;
            let prefix = moves::stream_prefix(&cached, covered).map_err(stream_lost)?;
            let latest = moves::append_stream(&[], &export);
            let (end, digest) =
                moves::planned_stream(&cached, prefix, &latest).map_err(stream_lost)?;
            (latest, end, digest, Some(prefix))
        } else {
            let (bytes, end, digest) =
                moves::stream_file(&cached, super::stream::MOVE_IMPORT_BUDGET)
                    .map_err(stream_lost)?;
            (bytes, end, digest, None)
        };
        let mut divergence_reason = busy.map(str::to_owned);
        let divergence = if op == Op::Diverged {
            match self.incoming(reference, cut, op).await {
                Ok(incoming) if streamless => Some(
                    match same_files(
                        &incoming.bytes,
                        &backend,
                        &addr,
                        (class.name(), version),
                        runtime.deployment(),
                    )
                    .await
                    {
                        Ok(true) => LocalDivergence::AtCut,
                        Ok(false) => LocalDivergence::Advanced,
                        Err(cause) => {
                            tracing::error!(session_ref = %reference, cut, cause, "agent-data cannot compare the cut with a held ref");
                            divergence_reason = Some(cause);
                            LocalDivergence::Unknown
                        }
                    },
                ),
                Ok(incoming) => {
                    // The cached stream is what the last move here brought or sent. Every move
                    // appends one export to the session's stream, so a cut whose stream starts
                    // with this one holds everything it held: the same cut, or one moving the
                    // session back after it went on elsewhere. The session itself can have gone
                    // on here since, which the stream never shows. So the copy is at the cut only
                    // when the cut's stream passes through the cached stream's end with the same
                    // digest, and the session holds no record past the stream's last export.
                    let at_cut =
                        incoming.frame_ends.iter().any(|(end, frame_digest)| {
                            *end == end_offset && *frame_digest == digest
                        }) && view.next_seq()
                            == exported_next_seq(&bytes, &addr, runtime.deployment()).await?;
                    Some(if at_cut {
                        LocalDivergence::AtCut
                    } else {
                        LocalDivergence::Advanced
                    })
                }
                Err(error) => {
                    let cause = error.unknown_cause.ok_or(error.refusal)?;
                    tracing::error!(session_ref = %reference, cut, cause, "agent-data cannot compare the cut with a held ref");
                    divergence_reason = Some(cause.to_owned());
                    Some(LocalDivergence::Unknown)
                }
            }
        } else {
            None
        };
        Ok(Some(PlannedSession {
            store_prefix,
            reference: reference.into(),
            session: session.clone(),
            end_offset,
            digest,
            sealed_digest: None,
            bytes,
            format: format!("{};features={}", moves::FORMAT, head.features.join(",")),
            lineage_id: broca_session_store::codec::hex(&head.session_id.0),
            local_archive: None,
            divergence,
            divergence_reason,
        }))
    }

    /// The plan a `busy` probe needs for a stored session that lives here, unfrozen: its state
    /// read from a view of the session's open window and its format from the head. A probe never
    /// builds an export, so it costs what reading the session's window costs, whatever the
    /// length of the history behind it.
    async fn busy_store_plan(
        &self,
        reference: &str,
        session: &BindIdentity,
        backend: &LocalBackend,
        addr: &SessionAddr,
        deployment: &broca_session_store::keys::DeploymentKey,
    ) -> Result<PlannedSession, ErrorBody> {
        let view = Session::view(
            backend,
            addr.clone(),
            &broca_module::store_open::triple(session),
            deployment,
        )
        .await
        .map_err(|e| unavailable(format!("{e:?}")))?;
        let (stored, _) = backend
            .read_stored_head(addr)
            .map_err(|e| unavailable(format!("{e:?}")))?
            .ok_or_else(|| unavailable("store head missing"))?;
        let (head, _) = broca_session_store::head::open(&stored, deployment)
            .map_err(|e| unavailable(format!("{e:?}")))?;
        Ok(PlannedSession {
            store_prefix: None,
            reference: reference.into(),
            session: session.clone(),
            end_offset: 0,
            digest: String::new(),
            sealed_digest: None,
            bytes: Vec::new(),
            format: format!("{};features={}", moves::FORMAT, head.features.join(",")),
            lineage_id: broca_session_store::codec::hex(&head.session_id.0),
            local_archive: None,
            divergence: None,
            divergence_reason: store_busy(&view).map(str::to_owned),
        })
    }
}

/// The sequence number the next record of the session at `addr` would take, as the last export
/// in the move stream `stream` holds it.
/// The plan `diverged` answers for a held session it can't read: its divergence unknown.
pub(super) fn unreadable_divergence(reference: &str, session: &BindIdentity) -> PlannedSession {
    PlannedSession {
        store_prefix: None,
        reference: reference.into(),
        session: session.clone(),
        end_offset: 0,
        digest: String::new(),
        sealed_digest: None,
        bytes: vec![],
        format: moves::FORMAT.into(),
        lineage_id: String::new(),
        local_archive: None,
        divergence: Some(LocalDivergence::Unknown),
        divergence_reason: None,
    }
}

/// Whether the cut in `stream` holds exactly the files the local version pins, in the same
/// storage class: the comparison import makes before it refuses a local copy as advanced.
async fn same_files(
    stream: &[u8],
    store: &LocalBackend,
    addr: &SessionAddr,
    (class, version): (&str, broca_session_store::backend::Version),
    deployment: &broca_session_store::keys::DeploymentKey,
) -> Result<bool, String> {
    let frame = moves::last_export(stream).map_err(|error| format!("{error:?}"))?;
    let incoming = Export::decode(frame, addr, deployment).map_err(|error| format!("{error:?}"))?;
    if incoming.header.class != class {
        return Ok(false);
    }
    incoming
        .matches_pinned(store, addr, version, deployment)
        .await
        .map_err(|error| format!("{error:?}"))
}

async fn exported_next_seq(
    stream: &[u8],
    addr: &SessionAddr,
    deployment: &broca_session_store::keys::DeploymentKey,
) -> Result<u64, ErrorBody> {
    let export = Export::decode(
        moves::last_export(stream).map_err(failure)?,
        addr,
        deployment,
    )
    .map_err(failure)?;
    let memory = export.memory(addr, deployment).await.map_err(failure)?;
    let view = Session::view(&memory, addr.clone(), &export.header.triple, deployment)
        .await
        .map_err(|e| unavailable(format!("{e:?}")))?;
    Ok(view.next_seq())
}
