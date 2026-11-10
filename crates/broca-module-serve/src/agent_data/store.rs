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
    ) -> Result<(), ErrorBody> {
        use std::io::Write as _;
        let addr = SessionAddr(broca_core::session_addr(&plan.session));
        let path = self.root.join("session-store-moves/streams").join(&addr.0);
        if let Some(prefix) = plan.store_prefix {
            let covered = self
                .door
                .cover(&plan.reference)
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
        broca_wal::migration::durable_dir(&self.root, parent).map_err(unavailable)?;
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
                .map_err(door_refusal)?;
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
        broca_wal::migration::sync_dir(parent).map_err(unavailable)?;
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
        let Some(runtime) = &self.store else {
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
        let checks = broca_session_convert::verify::store_snapshot(&export, runtime.deployment())
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
        let Some(runtime) = &self.store else {
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
                .incoming(reference, cut)
                .await
                .map_err(|error| error.refusal)?;
            return self.incoming_store_plan(reference, bytes).await.map(Some);
        }
        let backend = LocalBackend::new(class.store_root(&self.root));
        let addr = SessionAddr(broca_core::session_addr(session));
        let (_, current) = backend
            .read_stored_head(&addr)
            .map_err(|e| unavailable(format!("{e:?}")))?
            .ok_or_else(|| unavailable("store head missing"))?;
        let freeze: Option<broca_wal::migration::Freeze> = broca_wal::migration::read_json(
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
        if busy.is_none() {
            export.checks =
                broca_session_convert::verify::store_snapshot(&export, runtime.deployment())
                    .await
                    .map_err(unavailable)?;
        }
        let (head, _) = export.head(&addr, runtime.deployment()).map_err(failure)?;
        let cached = self.root.join("session-store-moves/streams").join(&addr.0);
        let (bytes, end_offset, digest, store_prefix) = if op == Op::Busy {
            let bytes = moves::append_stream(&[], &export);
            let digest = broca_wal::migration::digest(&bytes);
            (bytes.clone(), bytes.len() as u64, digest, None)
        } else if op == Op::Prepare {
            // Cover carries no digest, so prepare checks only the local frame hashes.
            // Import checks the whole-prefix digest returned by session.read {cutId}; admit
            // sends only the suffix and cannot remotely compare already-covered bytes.
            let covered = self.door.cover(reference).await.map_err(unavailable)?;
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
        let divergence = if op == Op::Diverged {
            let incoming = self.incoming(reference, cut).await.map_err(|e| e.refusal)?;
            Some(if incoming.end == end_offset && incoming.digest == digest {
                LocalDivergence::AtCut
            } else {
                LocalDivergence::Advanced
            })
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
            divergence_reason: busy.map(str::to_owned),
        }))
    }
}
