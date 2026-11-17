//! Sends whose `delivery` is an ordered list of modes, on a session kept in the new storage.
//!
//! Admission follows today's actor (`string_sender`, `ordered_sender`,
//! `render_ordered_template` and `queue_ordered_submission` in `crate::actor`), built from the
//! same shared rules: the owner check (`broca_core::scope_admission::decide_owner_access`), the
//! plan's current senders (`broca_session_loop::plan::senders`), llm-runner's `admit_delivery`,
//! `legacy_equivalent` and `render`, and broca-core's `OrderedSend::replaced_by` and
//! `DeliveryOrder::expired_at`.
//!
//! An admitted send is written as `DeliveryOrdered` then `QueuedSend`, durable before the reply,
//! in one batch with the `SendEnded` of the send it supersedes (first) and its own `SendEnded`
//! when it arrives expired (last). It then waits in the session's queue, which the records
//! build (`Operations::queue`): the running run reads it at each boundary and delivers it in the
//! first mode of its list possible there, the same protocol steers use. An order naming
//! `idle_start` starts a run on an idle session, as the queue drains.
//! An order without one never starts a run, and stays queued across runs until a boundary
//! delivers it or a drain finds it expired.

use broca_protocol::{BindPrincipal, DeliveryMode, DeliveryOrder, SubmissionId, SubmissionRequest};
use broca_session_loop::queue;
use broca_session_loop::writer::Writer;
use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{DeliveryOrdered, Record, SendEnded};
use broca_wal::record::SendEndReason;
use cortexkit_role_llm_runner::delivery_order as role;
use cortexkit_role_llm_runner::errors;
use tokio::sync::oneshot;

use super::{retry_of_queued, Start, StoreActor};
use crate::actor::{SendOutcome, StringSender};

/// One ordered send waiting in the queue, read back from its records.
pub(super) struct QueuedOrder {
    pub submission_id: String,
    pub order: DeliveryOrder,
    /// Whose dedupe key it carries: the plan-named sender its order names, else the principal
    /// it was queued under, as today's queue reads it (`OrderedSend::from_pending`).
    pub principal: Option<BindPrincipal>,
}

/// The wall clock an ordered send's expiry is compared against.
fn now_ms() -> u64 {
    broca_core::clock::epoch_millis().unwrap_or(0)
}

/// The `SendEnded` record ending `submission_id` undelivered for `reason`.
fn ended(submission_id: &str, reason: &SendEndReason) -> Result<Record, String> {
    Ok(Record::SendEnded(SendEnded {
        submission_id: submission_id.to_owned(),
        reason: Some(serde_json::to_value(reason).map_err(|error| error.to_string())?),
    }))
}

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// The session's owner summary for the owner check, with the sender policy its latest
    /// accepted refresh set, as today's summary carries it.
    fn sender_ownership(&self) -> Result<broca_core::SessionOwnershipV1, SendOutcome> {
        broca_session_loop::owner::ownership_with_plan(self.session.operations(), &self.parts)
            .map_err(|error| {
                SendOutcome::Transient(format!(
                    "cannot read the session's history for the sender check: {error}"
                ))
            })
    }

    /// The modules other than the owner whose sends the session admits, from its current plan
    /// (the latest accepted refresh's, else the first run's), and whether a refresh set them, as
    /// today's actor reads them (`effective_plan_senders`): every sender read goes through here.
    fn plan_senders(&self) -> Result<PlanSenders, SendOutcome> {
        let operations = self.session.operations();
        Ok(PlanSenders {
            senders: broca_session_loop::plan::senders(operations, &self.parts).map_err(
                |error| {
                    SendOutcome::Transient(format!(
                        "cannot read the session's plan senders: {error}"
                    ))
                },
            )?,
            refreshed: broca_session_loop::plan::refreshed(operations),
        })
    }

    /// Whether a steer from `sender` carrying `marks` is the person's own message (llm-runner
    /// `call_yield`): `sender` is a module the session's plan marks `person`, and the message is
    /// neither a notice nor words spoken for an agent. A plan that can't be read names no one.
    pub(super) fn is_person_steer(
        &self,
        sender: Option<&BindPrincipal>,
        marks: Option<&broca_protocol::MessageMarks>,
    ) -> bool {
        let Some(BindPrincipal::Reserved { module_id }) = sender else {
            return false;
        };
        if marks.is_some_and(|marks| marks.notice || marks.speaks_for.is_some()) {
            return false;
        }
        self.plan_senders().is_ok_and(|plan| {
            plan.senders
                .iter()
                .any(|listed| &listed.module == module_id && listed.person)
        })
    }

    /// Tells the running run that the person's own steer is durable: each of its waiting calls
    /// whose provider can hand calls back is asked to, so the steer lands at the next step
    /// boundary without waiting for that call's work. Without a running run nothing is waiting.
    pub(super) fn person_steered(&self) {
        if let Some(active) = &self.run {
            active.person_steers.send_modify(|count| *count += 1);
        }
    }

    /// How a send with a string `delivery` is admitted, as today's actor admits it
    /// (`string_sender`): from the owner, or a caller the owner check decides on, unchanged; from
    /// a module the plan names, `queue` and `steer` as the lists they stand for (`[idle_start]`
    /// and `[steer, idle_start]`) and `interrupt` on its grant, continuing under the session's
    /// frozen principal, each as its grants allow (`admit_delivery`). Once a refresh set the
    /// session's senders, a module it doesn't name is refused.
    pub(super) fn string_sender(
        &self,
        request: &SubmissionRequest,
        delivery: broca_protocol::Delivery,
    ) -> Result<StringSender, SendOutcome> {
        let Some(BindPrincipal::Reserved { module_id }) = &request.principal else {
            return Ok(StringSender::Owner);
        };
        let ownership = self.sender_ownership()?;
        if broca_core::scope_admission::decide_owner_access(
            &ownership,
            request.principal.as_ref(),
            &request.scope,
        )
        .is_ok()
        {
            return Ok(StringSender::Owner);
        }
        let PlanSenders { senders, refreshed } = self.plan_senders()?;
        // Without a sender policy from a refresh, a module the plan doesn't name is left to the
        // owner check further on, as today's actor leaves it.
        if !senders.iter().any(|sender| &sender.module == module_id) {
            if refreshed {
                return Err(SendOutcome::DeliveryRefused {
                    code: errors::SCOPE_OWNER_MISMATCH,
                    field: "delivery",
                    message: format!(
                        "the session's plan does not name module {module_id} as a sender"
                    ),
                });
            }
            return Ok(StringSender::Owner);
        }
        let frozen = frozen_principal(&ownership)?;
        let role_delivery = match delivery {
            broca_protocol::Delivery::Queue => cortexkit_role_llm_runner::send::Delivery::Queue,
            broca_protocol::Delivery::Steer => cortexkit_role_llm_runner::send::Delivery::Steer,
            broca_protocol::Delivery::Interrupt => {
                cortexkit_role_llm_runner::send::Delivery::Interrupt
            }
        };
        role::admit_delivery(
            module_id,
            Some(&frozen.as_owner_string()),
            &senders,
            role_delivery,
        )
        .map_err(|(code, field)| SendOutcome::DeliveryRefused {
            code,
            field: field.unwrap_or("delivery"),
            message: format!(
                "the session's plan does not let module {module_id} send delivery {}",
                delivery.as_str()
            ),
        })?;
        Ok(match role::legacy_equivalent(role_delivery) {
            Some(modes) => StringSender::AsOrder(DeliveryOrder {
                modes,
                dedupe_key: None,
                expires_at_ms: None,
                template: None,
            }),
            None => StringSender::Interrupt(frozen),
        })
    }

    /// Whether an ordered send comes from the session's owner (`Ok(None)`), from a plan-named
    /// sender (`Ok(Some(the session's frozen principal))`, which the send continues under), or
    /// is refused, as today's actor decides it: the sender is the route's attested principal,
    /// and only a sender the plan grants `idle_start` may send a list naming it.
    pub(super) fn ordered_sender(
        &self,
        request: &SubmissionRequest,
        order: &DeliveryOrder,
        key: &crate::session_cmd::SendKey,
    ) -> Result<Option<BindPrincipal>, SendOutcome> {
        let ownership = self.sender_ownership()?;
        if broca_core::scope_admission::decide_owner_access(
            &ownership,
            request.principal.as_ref(),
            &request.scope,
        )
        .is_ok()
        {
            return Ok(None);
        }
        let refused = |code: &'static str, message: String| SendOutcome::DeliveryRefused {
            code,
            field: "delivery",
            message,
        };
        let Some(BindPrincipal::Reserved { module_id }) = &request.principal else {
            return Err(refused(
                errors::SCOPE_OWNER_MISMATCH,
                "only the session's owner or a module its plan names may send with an ordered \
                 delivery list"
                    .into(),
            ));
        };
        let senders = self.plan_senders()?.senders;
        let Some(sender) = senders.iter().find(|sender| &sender.module == module_id) else {
            // Broca's own delivery of a handed-back call's late result (llm-runner `call_yield`):
            // the contract sends it as the call's provider whether or not the plan names that
            // module. A caller never sends with the internal scope: serve sets every caller's
            // scope from its route, so only broca's own code makes one; this one also carries
            // the notice's send id. It never starts a run for a module the plan doesn't name; it
            // waits for the session's next run.
            let late_notice = matches!(
                key,
                crate::session_cmd::SendKey::SendId(send_id)
                    if crate::late_results_puller::is_notice_send_id(send_id)
            );
            if late_notice && request.scope == broca_protocol::SendScope::Internal {
                if order.starts_runs() {
                    return Err(refused(
                        errors::INVALID_PARAMS,
                        format!("the session's plan does not let module {module_id} start runs"),
                    ));
                }
                return frozen_principal(&ownership).map(Some);
            }
            return Err(refused(
                errors::SCOPE_OWNER_MISMATCH,
                format!("the session's plan does not name module {module_id} as a sender"),
            ));
        };
        if order.starts_runs() && !sender.idle_start {
            return Err(refused(
                errors::INVALID_PARAMS,
                format!("the session's plan does not let module {module_id} start runs"),
            ));
        }
        frozen_principal(&ownership).map(Some)
    }

    /// Renders the send's template once, at admission, from the attested sender's plan entry,
    /// as today's actor renders it: the queued request carries the text, so a later boundary
    /// places those bytes and never renders the slots again. A `part_tag` slot renders the
    /// number the session's next request shows for the part it names ([`Self::shown_numbers`]);
    /// a part that request doesn't show ends the send `template_part_gone`, naming the slot.
    pub(super) async fn render_ordered_template(
        &self,
        request: &mut SubmissionRequest,
        template: &role::TemplateUse,
    ) -> Result<(), SendOutcome> {
        let refused = |problem: String| SendOutcome::DeliveryRefused {
            code: errors::INVALID_PARAMS,
            field: "template",
            message: format!("cannot render the sender's template: {problem}"),
        };
        let Some(BindPrincipal::Reserved { module_id }) = &request.principal else {
            return Err(refused("no template is declared for this caller".into()));
        };
        let senders = self.plan_senders()?.senders;
        let templates = senders
            .iter()
            .find(|sender| &sender.module == module_id)
            .map(|sender| sender.templates.as_slice())
            .unwrap_or_default();
        let numbers = if templates
            .iter()
            .filter(|declared| declared.id == template.id)
            .flat_map(|declared| declared.slots.values())
            .any(|slot| matches!(slot, role::Slot::PartTag))
        {
            self.shown_numbers().await.map_err(SendOutcome::Error)?
        } else {
            Default::default()
        };
        let text = match role::render(templates, template, |part| {
            broca_core::compaction_v2::shown::lookup(&numbers, &part.mid, &part.part)
        }) {
            Ok(text) => text,
            Err(role::TemplateProblem::PartGone(slot)) => {
                return Err(SendOutcome::TemplatePartGone { slot })
            }
            Err(problem) => return Err(refused(format!("{problem:?}"))),
        };
        request.input = vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text { text }],
        )];
        Ok(())
    }

    /// The number the session's next request shows for each part, by its message id and part
    /// id, for a template's `part_tag` slots: that request renders the newest accepted compaction
    /// view when
    /// it still passes the boundary check under the latest run's frozen rules, else the applied
    /// one (`compaction_v2::shown::next_request`). The views come from the compaction state this
    /// actor keeps for the session, read from the open log the first time, as
    /// `session.apply_view` reads it; the entries are the session's own. A session with no
    /// compaction provider has no views, and its next request shows every part's number.
    ///
    /// Only a session on the current compaction contract numbers its parts: off it, its
    /// requests render no part numbers, so none is shown and every `part_tag` slot's part is
    /// gone.
    async fn shown_numbers(
        &self,
    ) -> Result<std::collections::HashMap<(String, String), u64>, String> {
        use broca_core::compaction_v2 as v2;
        if !crate::store_entries::on_contract(&self.session) {
            return Ok(Default::default());
        }
        let Some(start) = self
            .session
            .operations
            .run
            .as_ref()
            .and_then(|run| run.start.as_ref())
        else {
            return Ok(Default::default());
        };
        let config = broca_session_loop::run::run_config_of_start(start, &self.parts)
            .map_err(|error| format!("{error:?}"))?;
        let entries = crate::store_entries::all_entries(&self.session).await?;
        let floor = broca_session_loop::run::signed_floor(
            &self.session,
            &self.parts,
            &mut Default::default(),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
        let floor = broca_session_loop::compaction::floor_through(&self.session, &entries, floor)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let conditions = v2::host::conditions(&config, &entries, false, None, floor.as_deref())?;
        if crate::store_entries::frozen_provider(&self.parts, &self.session)?.is_none() {
            return v2::shown::next_request(&Default::default(), &entries, &conditions);
        }
        let restored = || async {
            let records = self
                .session
                .open_log_records()
                .await
                .map_err(|error| format!("{error:?}"))?;
            broca_session_loop::compaction::restore(&records, &self.key, &self.session.operations)
        };
        let compaction = std::sync::Arc::clone(&self.compaction_session);
        if !compaction.is_initialized() {
            compaction.restore(restored().await?).await;
        }
        let numbers = match compaction.state.try_lock() {
            Ok(state) => v2::shown::next_request(&state, &entries, &conditions),
            // A run holds the state while it writes a view of its own through this actor, and
            // the run waits for this actor to serve that write: until it does, the session's
            // records hold the same views as the state.
            Err(_) => v2::shown::next_request(&restored().await?, &entries, &conditions),
        };
        numbers
    }

    /// The ordered send `submission_id` while it waits in the queue, read from its records.
    pub(super) async fn queued_order(
        &self,
        submission_id: &str,
    ) -> Result<Option<QueuedOrder>, String> {
        let records = self
            .session
            .find_id(IdKind::SubmissionId, submission_id)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let Some(ordered) = records.iter().find_map(|(_, record)| match record {
            Record::DeliveryOrdered(ordered) => Some(ordered),
            _ => None,
        }) else {
            return Ok(None);
        };
        let order: DeliveryOrder = serde_json::from_value(ordered.order.clone())
            .map_err(|error| format!("the queued send {submission_id}'s order: {error}"))?;
        let principal = |value: &serde_json::Value| {
            serde_json::from_value::<BindPrincipal>(value.clone())
                .map_err(|error| format!("the queued send {submission_id}'s sender: {error}"))
        };
        let queued_as = records.iter().find_map(|(_, record)| match record {
            Record::QueuedSend(send) => queue::receipt_field(send, "principal"),
            _ => None,
        });
        let principal = match (&ordered.sent_by, queued_as) {
            (Some(sender), _) => Some(principal(sender)?),
            (None, Some(queued_as)) => Some(principal(&queued_as)?),
            (None, None) => None,
        };
        Ok(Some(QueuedOrder {
            submission_id: submission_id.to_owned(),
            order,
            principal,
        }))
    }

    /// The queue's ordered sends, in queue order.
    async fn queued_orders(&self) -> Result<Vec<QueuedOrder>, String> {
        let ids: Vec<String> = self
            .session
            .operations()
            .queue
            .iter()
            .filter(|pending| pending.ordered)
            .map(|pending| pending.submission_id.clone())
            .collect();
        let mut orders = Vec::with_capacity(ids.len());
        for id in ids {
            orders.push(
                self.queued_order(&id)
                    .await?
                    .ok_or_else(|| format!("the queued send {id} has no order"))?,
            );
        }
        Ok(orders)
    }

    /// Whether the queue holds a send that starts a run once the session is idle, as today's
    /// actor counts it (`has_pending`): every plain send does, and an ordered one only when its
    /// list names `idle_start`.
    pub(super) async fn queue_starts_runs(&self) -> Result<bool, String> {
        for pending in self.session.operations().queue.clone() {
            if !pending.ordered {
                return Ok(true);
            }
            if self
                .queued_order(&pending.submission_id)
                .await?
                .is_some_and(|queued| queued.order.starts_runs())
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Ends every queued ordered send whose expiry has passed, durably, as today's actor does
    /// before its queue drains (`expire_ordered_sends`).
    pub(super) async fn expire_ordered(&mut self) -> Result<(), String> {
        let now = now_ms();
        let mut records = Vec::new();
        for queued in self.queued_orders().await? {
            if queued.order.expired_at(now) {
                records.push(ended(&queued.submission_id, &SendEndReason::Expired)?);
            }
        }
        if records.is_empty() {
            return Ok(());
        }
        let seq = Writer::append(&mut self.durable(), &records, true)
            .await
            .map_err(|error| format!("{error:?}"))?;
        if let Some(active) = self.run.as_mut() {
            active.link.note_own(seq, &records);
        }
        Ok(())
    }

    /// Queues `start`, an ordered send, as today's actor queues one: a retry of a send still
    /// queued is answered from its record; otherwise its input goes through the session's
    /// `pre_user` transforms, and its records are written durably, superseding the undelivered
    /// send from the same sender under the same dedupe key. A send expired on arrival is
    /// answered ended. On an idle session a list naming `idle_start` starts its run now, and the
    /// sender hears `active` once the run's start is durable. `ordered_by` is the plan-named
    /// sender, when it isn't the owner.
    pub(super) async fn queue_ordered(
        &mut self,
        start: Start,
        order: DeliveryOrder,
        ordered_by: Option<BindPrincipal>,
        mut reply: oneshot::Sender<SendOutcome>,
    ) {
        match self
            .admit_ordered(start, order, ordered_by, &mut reply)
            .await
        {
            Ok(Some(submission_id)) => self.drain_for(Some((submission_id, reply))).await,
            Ok(None) => {}
            Err(outcome) => {
                let _ = reply.send(outcome);
            }
        }
    }

    /// [`Self::queue_ordered`] up to the reply: `Ok(Some(id))` when the queued send `id` may
    /// start its run now, `Err` with every other answer.
    async fn admit_ordered(
        &mut self,
        start: Start,
        order: DeliveryOrder,
        ordered_by: Option<BindPrincipal>,
        reply: &mut oneshot::Sender<SendOutcome>,
    ) -> Result<Option<SubmissionId>, SendOutcome> {
        let Start {
            conversation_key,
            run_id,
            one_shot,
            mut request,
            title,
            send_id,
            identity,
            manifest,
            session_class,
            message_marks,
            prepared: run_prepared,
            ..
        } = start;
        let submission_id = SubmissionId(format!("sub-{}", run_id.0));
        // A retry under the same key is the same send: the order is part of its identity, and a
        // send still queued answers `pending`.
        if let Some(existing) = self
            .queued(&submission_id.0)
            .await
            .map_err(SendOutcome::Error)?
        {
            let earlier = self
                .queued_order(&submission_id.0)
                .await
                .map_err(SendOutcome::Error)?;
            if !earlier.is_some_and(|earlier| crate::actor::same_order(&order, &earlier.order)) {
                return Err(SendOutcome::SendIdReuse {
                    differing: "delivery",
                });
            }
            return Err(retry_of_queued(
                &existing,
                request,
                message_marks.as_ref(),
                identity.as_deref(),
                conversation_key.as_deref(),
                broca_protocol::Delivery::Queue,
                &self.parts,
                submission_id,
            ));
        }
        // A list that starts its run now meets that run's plan check here, writing nothing when
        // it fails, as a plain send that starts a run does (`Self::freeze_plan`): a pending
        // refresh that applies at the run must accept the model the send runs on.
        if order.starts_runs()
            && self.run.is_none()
            && !self.session.operations().owner.needs_admission()
        {
            self.run_plan(&request.model, request.variant.as_deref())?;
        }
        let expired = order.expired_at(now_ms());
        let staged = self.stage_ordered_media(&mut request, &order).await?;
        let prepared = self
            .transform_input(
                request.input.clone(),
                broca_protocol::Delivery::Queue,
                manifest.as_deref(),
                None,
                broca_core::clock::epoch_millis(),
                reply,
            )
            .await?;
        if let Some(provider) = prepared.refused.clone() {
            return Err(SendOutcome::PreUserUnavailable { provider });
        }
        request.staged_uploads = None;
        // Whose dedupe key this is: the plan-named sender, or the owner sending as itself.
        let sender = ordered_by.clone().or_else(|| request.principal.clone());
        let superseded = self
            .queued_orders()
            .await
            .map_err(SendOutcome::Error)?
            .into_iter()
            .find(|queued| {
                broca_core::delivery_order::OrderedSend {
                    admitted_at_ms: None,
                    submission_id: SubmissionId(queued.submission_id.clone()),
                    order: queued.order.clone(),
                    principal: queued.principal.clone(),
                    input: Vec::new(),
                }
                .replaced_by(sender.as_ref(), order.dedupe_key.as_deref())
            })
            .map(|queued| queued.submission_id);
        let receipt = queue::Receipt {
            session_class: session_class.filter(|_| self.frozen_session_class().is_none()),
            conversation_key,
            // The input's sender as the session shows it: the plan-named sender, else the
            // principal the send came under.
            sent_by: sender.clone(),
            principal: request.principal.clone(),
            request_identity_digest: identity,
            title,
            hooks: prepared.queued_hooks(),
            retention: run_prepared.retention,
            ..queue::Receipt::default()
        };
        let at = queue::Queued {
            written_at_ms: prepared.at_ms,
            ts_ms: run_prepared.retention.map(|_| (self.now_ms)()),
            ..queue::Queued::default()
        };
        let queued = queue::queued_send(
            &submission_id.0,
            send_id.as_deref(),
            &request,
            &receipt,
            at,
            message_marks.as_ref(),
            &mut self.parts,
        )
        .map_err(SendOutcome::Error)?;
        let json = |error: serde_json::Error| SendOutcome::Error(error.to_string());
        let mut records = Vec::with_capacity(4);
        if let Some(old) = &superseded {
            records.push(
                ended(
                    old,
                    &SendEndReason::Superseded {
                        by: submission_id.clone(),
                    },
                )
                .map_err(SendOutcome::Error)?,
            );
        }
        records.push(Record::DeliveryOrdered(DeliveryOrdered {
            submission_id: submission_id.0.clone(),
            order: serde_json::to_value(&order).map_err(json)?,
            sent_by: ordered_by
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(json)?,
        }));
        records.push(Record::QueuedSend(queued));
        if expired {
            records.push(
                ended(&submission_id.0, &SendEndReason::Expired).map_err(SendOutcome::Error)?,
            );
        }
        // The queued record keeps the send's media, stored as the session's own objects before
        // the batch, as a queued send's are.
        self.append_own_batch_with_media(&records, &staged)
            .await
            .map_err(|error| SendOutcome::Error(format!("{error:?}")))?;
        // The person's steer hands back the running run's waiting calls only once it is durable,
        // and never waits for them.
        if !expired
            && order.modes.first() == Some(&DeliveryMode::Steer)
            && self.is_person_steer(sender.as_ref(), message_marks.as_ref())
        {
            self.person_steered();
        }
        if expired {
            return Err(SendOutcome::SendEnded {
                submission_id,
                reason: SendEndReason::Expired,
            });
        }
        if one_shot {
            self.one_shot_submissions.insert(submission_id.clone());
        }
        // On an idle session `idle_start` is the first possible mode of any list naming it:
        // nothing else is possible without a running run.
        if order.starts_runs() && self.run.is_none() {
            return Ok(Some(submission_id));
        }
        Err(SendOutcome::Pending { submission_id })
    }
}

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Stages the attachments an ordered send's `content` names into its input and checks them,
    /// as today's actor does for an ordered send (`queue_ordered_submission`): against the
    /// running run's media, as its start froze them, when the list may steer into that run, else
    /// against the profile the executor quotes for the send's model. A send holding media whose
    /// list names neither `steer` nor `idle_start` is refused as media its carrier can't take:
    /// a tool result carries text only, and turn-scoped system text is a template's text.
    async fn stage_ordered_media(
        &self,
        request: &mut SubmissionRequest,
        order: &DeliveryOrder,
    ) -> Result<Vec<crate::person_media::StagedMedia>, SendOutcome> {
        let running = match self.run.is_some() && order.modes.contains(&DeliveryMode::Steer) {
            true => self
                .session
                .operations
                .run
                .as_ref()
                .and_then(|run| run.start.as_ref()),
            false => None,
        };
        let frozen = running
            .map(|start| {
                broca_session_loop::run::admission_of_start(start, &self.parts)
                    .map(|admission| super::frozen_media(&admission))
                    .map_err(|error| SendOutcome::Error(format!("{error:?}")))
            })
            .transpose()?;
        let staged = self.stage_media(request, frozen).await?;
        // Only a list naming no steer is refused, and such a list was checked against the send's
        // own model, so the refusal names that model.
        let model = format!(
            "{}/{}",
            request.model.provider_module_id, request.model.model_id
        );
        crate::person_media::ordered_carrier(order, &request.input, &model)
            .map_err(crate::person_media::outcome)?;
        Ok(staged)
    }
}

/// The senders a session's plan names besides its owner, and whether a plan refresh set them.
pub(super) struct PlanSenders {
    pub senders: Vec<role::PlanSender>,
    /// A refresh gave the session a sender policy: a module it doesn't name is refused, even
    /// with a string `delivery`.
    pub refreshed: bool,
}

/// The principal a plan-named sender's send continues under: the session's scope owner when
/// it is scoped, else its first run's principal.
fn frozen_principal(
    ownership: &broca_core::SessionOwnershipV1,
) -> Result<BindPrincipal, SendOutcome> {
    ownership
        .scope
        .as_ref()
        .map(|scope| scope.owner.clone())
        .or_else(|| ownership.first_principal.clone())
        .ok_or_else(|| {
            SendOutcome::Transient("the session has no frozen principal to send under".into())
        })
}

/// Whether a send that leaves its model to the session may take the model of the run it joins,
/// as today's actor lets one whose list names a mode delivered into a running run.
pub(super) fn joins_a_run(order: &DeliveryOrder) -> bool {
    order.modes.iter().any(|mode| {
        matches!(
            mode,
            DeliveryMode::Steer | DeliveryMode::ToolResult | DeliveryMode::TurnSystem
        )
    })
}
