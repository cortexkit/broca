use super::*;
use broca_records::session_storage::StorageClass;
use broca_session_store::{
    backend::{LocalBackend, SessionAddr},
    head::Residence,
    session::{
        moves::{self, Export},
        Session,
    },
};

pub fn failure(error: moves::Error) -> ErrorBody {
    match error {
        moves::Error::DeploymentKeyMissing(id) => refusal("unavailable",
            format!("Deployment key {id} is not loaded on this computer. Transfer opaque:broca-deployment:{id} from the source computer if needed, then grant reserved:broca exact read on it on the receiver (grants do not travel)."),
            serde_json::json!({"reason":"deployment_key_missing", "key_id":id})),
        moves::Error::SetupKeyUnavailable => unavailable(crate::store_open::SETUP_KEY_UNAVAILABLE),
        moves::Error::Invalid(reason) if reason.contains("format") => refusal("incompatible_version", "unsupported store export format", Value::Null),
        moves::Error::Invalid(reason) if reason.contains("SHA-256") => refusal("digest_mismatch", "store export file or frame digest differs", Value::Null),
        error => unavailable(format!("{error:?}")),
    }
}

impl FileCutArtifacts {
    pub(super) fn move_sidecar(&self, plan: &PlannedSession) -> PathBuf {
        self.state_root
            .join("session-store-moves/fences")
            .join(broca_core::session_addr(&plan.session))
    }
    pub(super) fn store_runtime(&self) -> Result<Arc<crate::store_open::StoreRuntime>, ErrorBody> {
        self.registry
            .store_runtime()
            .ok_or_else(|| unavailable("session storage is unavailable"))
    }
    pub(super) fn store_staged(&self, cut: &str, plan: &PlannedSession) -> PathBuf {
        self.state_root
            .join("session-store-moves/staging")
            .join(cut)
            .join(broca_core::session_addr(&plan.session))
    }
    pub(super) fn store_export(&self, plan: &PlannedSession) -> Result<Export, ErrorBody> {
        Export::decode(
            broca_session_store::session::moves::last_export(&plan.bytes).map_err(failure)?,
            &SessionAddr(broca_core::session_addr(&plan.session)),
            self.store_runtime()?.deployment(),
        )
        .map_err(failure)
    }
    pub(super) fn store_backend(&self, plan: &PlannedSession) -> Result<LocalBackend, ErrorBody> {
        let class = self.store_export(plan)?.header.class;
        let class = StorageClass::ALL
            .into_iter()
            .find(|item| item.name() == class)
            .ok_or_else(|| unavailable("unknown store class"))?;
        Ok(LocalBackend::new(class.store_root(&self.state_root)))
    }
    pub(super) async fn store_check(
        &self,
        cut: &str,
        plan: &PlannedSession,
        op: Op,
        state: &CutState,
        request: &Request,
    ) -> Result<(), ErrorBody> {
        let runtime = self.store_runtime()?;
        if matches!(op, Op::Abort | Op::Busy | Op::Diverged) {
            return Ok(());
        }
        if let Some(other) =
            move_fence::cut_fence(&self.state_root, &plan.reference).map_err(unavailable)?
        {
            if other != cut {
                return Err(foreign_owner(plan, cut, &other, op, state.step));
            }
        }
        let export = self.store_export(plan)?;
        let addr = SessionAddr(broca_core::session_addr(&plan.session));
        export
            .validate(&addr, runtime.deployment())
            .await
            .map_err(failure)?;
        let backend = self.store_backend(plan)?;
        let local = backend
            .read_stored_head(&addr)
            .map_err(|e| unavailable(format!("{e:?}")))?;
        let live = local
            .as_ref()
            .map(|(bytes, _)| broca_session_store::head::open(bytes, runtime.deployment()))
            .transpose()
            .map_err(|e| unavailable(format!("{e:?}")))?;
        if matches!(op, Op::Prepare | Op::Export | Op::Release) {
            if let Some((head, _)) = &live {
                if let Residence::Moved(moved) = &head.residence {
                    if op == Op::Release
                        && moved.cut_id == cut
                        && request.params()["holder"] == moved.holder
                        && request.params().get("holderBox").and_then(Value::as_str)
                            == moved.holder_box.as_deref()
                    {
                        return Ok(());
                    }
                    return Err(super::moved_source(plan, &moved.holder, &moved.cut_id, op));
                }
            }
            self.owned_freeze(cut, plan, op != Op::Prepare, op, state.step)?;
            let (head, version) = export.head(&addr, runtime.deployment()).map_err(failure)?;
            let Some((live, _)) = live else {
                return Err(unavailable("source store head missing"));
            };
            if live.session_id != head.session_id {
                return Err(unavailable("source store identity changed"));
            }
            if op == Op::Prepare {
                let memory = export
                    .memory(&addr, runtime.deployment())
                    .await
                    .map_err(failure)?;
                let view = Session::view(&memory, addr.clone(), &head.triple, runtime.deployment())
                    .await
                    .map_err(|e| unavailable(format!("{e:?}")))?;
                if let Some(reason) = store_busy(&view) {
                    return Err(busy(
                        vec![busy_ref(&plan.reference, reason, "source has pending work")],
                        "source has pending work",
                    ));
                }
            }
            if !export
                .matches_pinned(&backend, &addr, version, runtime.deployment())
                .await
                .map_err(failure)?
            {
                return Err(unavailable("frozen source store bytes changed"));
            }
        } else if matches!(op, Op::Import | Op::Activate) {
            if runtime
                .class_of(&plan.session)
                .map_err(unavailable)?
                .is_some_and(|class| class.name() != export.header.class)
            {
                return Err(refusal(
                    "local_advanced",
                    "local session is held in a different storage class",
                    Value::Null,
                ));
            }
            if plan.sealed_digest.as_deref() != Some(&plan.digest) {
                return Err(refusal(
                    "digest_mismatch",
                    "sealed store digest differs",
                    Value::Null,
                ));
            }
            if let Some((head, _)) = live {
                match &head.residence {
                    Residence::Moving(_) => {
                        return Err(busy(
                            vec![busy_ref(&plan.reference, "moving", "receiver is frozen")],
                            "receiver is frozen",
                        ))
                    }
                    Residence::Local(())
                        if state
                            .intent
                            .as_ref()
                            .is_none_or(|intent| intent.op != "activate") =>
                    {
                        let version = local.as_ref().unwrap().1;
                        if !export
                            .matches_pinned(&backend, &addr, version, runtime.deployment())
                            .await
                            .map_err(failure)?
                        {
                            return Err(refusal(
                                "local_advanced",
                                "local store differs from incoming cut",
                                Value::Null,
                            ));
                        }
                    }
                    _ => {}
                }
            }
            if op == Op::Activate
                && !self.store_staged(cut, plan).exists()
                && state
                    .intent
                    .as_ref()
                    .is_none_or(|intent| intent.op != "activate")
            {
                return Err(unavailable(
                    "store staging missing without activation intent",
                ));
            }
        }
        let _ = request;
        Ok(())
    }
    pub(super) async fn store_apply(
        &self,
        cut: &str,
        plan: &PlannedSession,
        op: Op,
        request: &Request,
    ) -> Result<(), ErrorBody> {
        // `busy` and `diverged` only read; they change nothing to apply.
        if matches!(op, Op::Busy | Op::Diverged) {
            return Ok(());
        }
        let runtime = self.store_runtime()?;
        let addr = SessionAddr(broca_core::session_addr(&plan.session));
        if op == Op::Abort {
            if let Some(class) = runtime.class_of(&plan.session).map_err(unavailable)? {
                moves::abort(
                    &LocalBackend::new(class.store_root(&self.state_root)),
                    &addr,
                    cut,
                    runtime.deployment(),
                )
                .await
                .map_err(failure)?;
            }
            return Ok(());
        }
        let export = self.store_export(plan)?;
        let backend = self.store_backend(plan)?;
        let sidecar = self.move_sidecar(plan);
        match op {
            Op::Prepare => {
                self.driver.cache_store_stream(cut, plan, op).await?;
                let (_, version) = export.head(&addr, runtime.deployment()).map_err(failure)?;
                move_fence::freeze(
                    &self.state_root,
                    &sidecar,
                    &Freeze {
                        cut_id: cut.into(),
                        end_offset: plan.end_offset,
                        lineage_id: plan.lineage_id.clone(),
                        store_head_version: Some(version),
                    },
                )
                .map_err(unavailable)?
                .map_err(unavailable)?;
                self.registry.end_parked_subscribers(&plan.session);
                moves::freeze(&backend, &addr, version, cut, runtime.deployment())
                    .await
                    .map_err(failure)?;
            }
            Op::Release => {
                let params = request.params();
                let holder = params["holder"]
                    .as_str()
                    .ok_or_else(|| unavailable("release holder missing"))?
                    .to_owned();
                // The receiving computer's box id, which the contract's decoder has checked.
                let holder_box = params.get("holderBox").and_then(Value::as_str);
                moves::release(
                    &backend,
                    &addr,
                    cut,
                    &holder,
                    holder_box,
                    runtime.deployment(),
                )
                .await
                .map_err(failure)?;
                self.registry.end_parked_subscribers(&plan.session);
                move_fence::release(&self.state_root, &sidecar, cut, &holder)
                    .map_err(unavailable)?
                    .map_err(unavailable)?;
            }
            Op::Import => {
                self.driver.cache_store_stream(cut, plan, op).await?;
                let staged = self.store_staged(cut, plan);
                export
                    .stage(&staged, &addr, runtime.deployment())
                    .await
                    .map_err(failure)?;
                self.driver.verify_store(plan, &staged).await?;
            }
            Op::Activate => {
                use broca_session_loop::parts::Parts as _;
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
                let class = StorageClass::ALL
                    .into_iter()
                    .find(|class| class.name() == export.header.class)
                    .unwrap();
                let setup = broca_session_store::setup::SetupReader::new(
                    &memory,
                    runtime.deployment(),
                    export.header.cipher,
                );
                let mut parts = runtime.parts_for(class, export.header.cipher).clone();
                for (_, record) in view
                    .records_from(0)
                    .await
                    .map_err(|e| unavailable(format!("{e:?}")))?
                {
                    let named = match &record {
                        broca_session_store::record::Record::RunStart(start) => &start.setup,
                        broca_session_store::record::Record::SetupStub(stub) => &stub.parts,
                        _ => continue,
                    };
                    for part in named {
                        let bytes = setup
                            .get(part)
                            .await
                            .map_err(|e| unavailable(format!("{e:?}")))?;
                        let value = serde_json::from_slice(&bytes).map_err(unavailable)?;
                        if parts.put(part.kind, &value).map_err(unavailable)? != *part {
                            return Err(unavailable("installed setup digest differs"));
                        }
                    }
                }
                crate::store_open::recovery::register_candidate(
                    &class.store_root(&self.state_root),
                    &addr,
                )
                .map_err(unavailable)?;
                // Its first run here asks routing to keep the account it last ran on.
                crate::store_open::moved_here::mark(&class.store_root(&self.state_root), &addr)
                    .map_err(unavailable)?;
                // A later move finds the session by its ref in the session-ref index alone, so
                // its row is written before the session is published. If the index can't take
                // it, the guard leaves the index marked incomplete until a census repairs it, so
                // a move asking meanwhile is told to retry rather than that the ref is unknown.
                let _admission = broca_core::record_candidate(
                    Some(self.refs.as_ref()),
                    &self.state_root,
                    &addr.0,
                    &plan.session,
                )
                .map_err(unavailable)?;
                // A session that can come due outside the ephemeral root is found by retention's
                // passes only through its mark, which its actor wrote on the computer it came
                // from and the move doesn't carry: marked here before it is published.
                if crate::retention::store::marks::needs_mark(class, &view.operations) {
                    crate::retention::store::marks::mark(
                        &class.store_root(&self.state_root),
                        &addr,
                    )
                    .map_err(unavailable)?;
                }
                export
                    .activate(
                        &self.store_staged(cut, plan),
                        &StorageClass::ALL
                            .into_iter()
                            .find(|class| class.name() == export.header.class)
                            .unwrap()
                            .store_root(&self.state_root),
                        &addr,
                    )
                    .map_err(failure)?;
                move_fence::unlink(&sidecar.with_extension("moved")).map_err(unavailable)?;
            }
            _ => {}
        }
        Ok(())
    }
}

pub fn store_busy<B: broca_session_store::backend::SessionBackend>(
    view: &Session<B>,
) -> Option<&'static str> {
    if !view.operations.queue.is_empty() {
        Some("send_queued")
    } else if view
        .operations
        .run
        .as_ref()
        .and_then(|run| run.open_batch.as_ref())
        .is_some_and(|batch| batch.calls.iter().any(|call| call.held.is_some()))
    {
        Some("approval_open")
    } else if view
        .operations
        .run
        .as_ref()
        .is_some_and(|run| run.finished.is_none())
    {
        Some("run_active")
    } else {
        None
    }
}
