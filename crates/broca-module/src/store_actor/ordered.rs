//! Sends whose `delivery` is an ordered list of modes, on a session kept in the session store.
//!
//! Admission checks owner access with `broca_core::scope_admission::decide_owner_access`, uses
//! the plan's current senders from `broca_session_loop::plan::senders`, and applies
//! llm-runner's `admit_delivery`, `legacy_equivalent` and `render`, plus broca-core's
//! `OrderedSend::replaced_by` and `DeliveryOrder::expired_at`.
//!
//! An admitted send is written as `DeliveryOrdered` then `QueuedSend`, durable before the
//! reply, in one batch with the `SendEnded` of the send it supersedes (first) and its own
//! `SendEnded` when it arrives expired (last). It waits in the session's queue, built by
//! `Operations::queue`: the running run reads it at each boundary and delivers it in the
//! first mode in its list that is possible there, using the same protocol as steers. An order
//! naming `idle_start` starts a run on an idle session as the queue drains. An order without
//! one never starts a run and stays queued across runs until a boundary delivers it or a drain
//! finds it expired.

use broca_core::tool_plane::SteerYield;
use broca_protocol::{BindPrincipal, DeliveryMode, DeliveryOrder, SubmissionId, SubmissionRequest};
use broca_records::record::SendEndReason;
use broca_session_loop::queue;
use broca_session_loop::writer::Writer;
use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{DeliveryOrdered, Record, SendEnded};
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
    /// it was queued under.
    pub principal: Option<BindPrincipal>,
}

/// The wall clock an ordered send's expiry is compared against.
fn now_ms() -> u64 {
    broca_core::clock::epoch_millis().unwrap_or(0)
}

#[cfg(test)]
thread_local! {
    /// How many queued orders this thread read back from their records, for tests pinning how
    /// many a send reads.
    pub(super) static ORDER_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
    /// accepted refresh set.
    fn sender_ownership(&self) -> Result<broca_core::SessionOwnershipV1, SendOutcome> {
        broca_session_loop::owner::ownership_with_plan(self.session.operations(), &self.parts)
            .map_err(|error| {
                SendOutcome::Transient(format!(
                    "cannot read the session's history for the sender check: {error}"
                ))
            })
    }

    /// The modules other than the owner whose sends the session admits, from its current plan
    /// (the latest accepted refresh's, else the first run's), and whether a refresh set them.
    /// Every sender read goes through here.
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

    pub(super) fn check_notice_yield(
        &self,
        request: &SubmissionRequest,
        delivery: broca_protocol::Delivery,
        order: Option<&DeliveryOrder>,
        proposed: Option<&broca_records::record::PlanManifest>,
    ) -> Result<(), SendOutcome> {
        let ownership = self.ownership().map_err(SendOutcome::Transient)?;
        // A repeat can carry a manifest fetched before the first run froze its plan. Once
        // admitted, only the stored policy (including a pending refresh) can grant a yield.
        let proposed = proposed.filter(|_| self.session.operations().owner.needs_admission());
        let senders = if request.urgent_yield && proposed.is_none() {
            self.plan_senders()?.senders
        } else {
            Vec::new()
        };
        let scope_owner =
            crate::notice_yield::scope_owner(ownership.scope.as_ref(), &request.scope);
        crate::notice_yield::check(
            proposed,
            scope_owner.as_deref(),
            request.urgent_yield,
            request.principal.as_ref(),
            delivery,
            order,
            &senders,
        )
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
            plan.senders.iter().any(|listed| {
                broca_protocol::same_module(&listed.module, module_id) && listed.person
            })
        })
    }

    /// Called for steer-first deliveries, after the sender has been checked. Urgent hand-back
    /// does not depend on who spoke the message or whether it was marked as a notice.
    pub(super) fn steer_yield(
        &self,
        urgent: bool,
        sender: Option<&BindPrincipal>,
        marks: Option<&broca_protocol::MessageMarks>,
    ) -> Option<SteerYield> {
        if urgent {
            Some(SteerYield::UrgentNotice)
        } else {
            self.is_person_steer(sender, marks)
                .then_some(SteerYield::PersonMessage)
        }
    }

    /// Tell the active run that a steer is durable without waiting for any tool response.
    pub(super) fn request_call_yield(&self, reason: SteerYield) {
        if let Some(active) = &self.run {
            active
                .steer_yields
                .send_modify(|state| state.record(reason));
        }
    }

    /// How a send with a string `delivery` is admitted: the owner, or a caller allowed by the
    /// owner check, keeps its delivery unchanged. For a plan-named module, `queue` and `steer`
    /// map to the lists they represent (`[idle_start]` and `[steer, idle_start]`);
    /// `interrupt` continues under the session's frozen principal. Each is allowed only by its
    /// grant (`admit_delivery`). Once a refresh has set the session's senders, a module it
    /// doesn't name is refused.
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
        // owner check further on.
        if !senders
            .iter()
            .any(|sender| broca_protocol::same_module(&sender.module, module_id))
        {
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
        // The contract compares ids exactly; a renamed module's two ids compare as one.
        role::admit_delivery(
            broca_protocol::canonical_module(module_id),
            Some(&frozen.canonical_owner_string()),
            &broca_core::delivery_order::canonical_senders(&senders),
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
    /// is refused: the sender is the route's attested principal, and only a sender the plan
    /// grants `idle_start` may send a list naming it.
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
        let Some(sender) = senders
            .iter()
            .find(|sender| broca_protocol::same_module(&sender.module, module_id))
        else {
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

    /// Places the text of the send's template in its input. A keyed send whose key already
    /// names a recorded send takes the text that send rendered at its admission, which its
    /// queued record keeps: the retry is compared with, and answered from, that send. Rendered
    /// again against today's view, a part a compaction has cut since would refuse the retry as
    /// gone, and a part shown under another number would change the text, though the send was
    /// already made. `run_id` is the run id the send's key derives.
    pub(super) async fn ordered_template_input(
        &self,
        request: &mut SubmissionRequest,
        template: &role::TemplateUse,
        run_id: &broca_protocol::RunId,
        key: &crate::actor::SendKey,
    ) -> Result<(), SendOutcome> {
        if key.is_keyed() {
            let run_id = self
                .sender_run_id(run_id, key, request.principal.as_ref())
                .await
                .map_err(SendOutcome::Error)?;
            let records = self
                .session
                .find_id(IdKind::SubmissionId, &format!("sub-{}", run_id.0))
                .await
                .map_err(|error| SendOutcome::Error(format!("{error:?}")))?;
            if let Some(send) = records.iter().find_map(|(_, record)| match record {
                Record::QueuedSend(send) => Some(send),
                _ => None,
            }) {
                request.input = queue::request_of(send, &self.parts)
                    .map_err(SendOutcome::Error)?
                    .input;
                return Ok(());
            }
        }
        self.render_ordered_template(request, template).await
    }

    /// Renders the send's template once, at admission, from the attested sender's plan entry.
    /// The queued request carries the text, so a later boundary places those bytes without
    /// rendering the slots again. A `part_tag` slot renders the number the session's next
    /// request shows for the part it names ([`Self::shown_numbers`]); a part that request
    /// doesn't show ends the send `template_part_gone`, naming the slot.
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
            .find(|sender| broca_protocol::same_module(&sender.module, module_id))
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
            vec![broca_protocol::ContentBlock::Text { text: text.into() }],
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
    /// A session sealed behind a cut shows only its window and the earlier messages its views
    /// name, so only those are read: the sealed history can hold hundreds of thousands of
    /// entries, and this runs on the actor, which serves nothing else meanwhile.
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
        let state = if crate::store_entries::frozen_provider(&self.parts, &self.session)?.is_none()
        {
            v2::state::State::default()
        } else {
            // Restored as a run would, since a state installed here is the one the session's
            // later views are accepted and applied against.
            let restored = || async {
                broca_session_loop::compaction::restore_live(&self.session, &self.key)
                    .await
                    .map_err(|error| format!("{error:?}"))
            };
            let compaction = std::sync::Arc::clone(&self.compaction_session);
            if !compaction.is_initialized() {
                compaction.restore(restored().await?).await;
            }
            let held = compaction
                .state
                .try_lock()
                .ok()
                .map(|state| v2::state::State::clone(&state));
            match held {
                Some(state) => state,
                // A run holds the state while it writes a view of its own through this actor,
                // and the run waits for this actor to serve that write: until it does, the
                // session's records hold the same views as the state.
                None => restored().await?,
            }
        };
        let entries = if self.session.conversation.window_start() > 1 {
            self.window_and_named(super::apply_view::named_mids(
                state.latest().into_iter().chain(state.applied()),
            ))
            .await?
        } else {
            crate::store_entries::all_entries(&self.session).await?
        };
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
        v2::shown::next_request(&state, &entries, &conditions)
    }

    /// The ordered send `submission_id` while it waits in the queue, read from its records.
    pub(super) async fn queued_order(
        &self,
        submission_id: &str,
    ) -> Result<Option<QueuedOrder>, String> {
        #[cfg(test)]
        ORDER_READS.with(|reads| reads.set(reads.get() + 1));
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

    /// Whether the queue holds a send that starts a run once the session is idle: every plain
    /// send does, and an ordered one only when its list names `idle_start`.
    pub(super) async fn queue_starts_runs(&self) -> Result<bool, String> {
        // By position, without copying the queue: nothing here writes, so it doesn't change.
        for index in 0..self.session.operations().queue.len() {
            let pending = &self.session.operations().queue[index];
            if !pending.ordered || self.order_starts_runs(&pending.submission_id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether the queued ordered send `submission_id` names `idle_start`, read from its records
    /// the first time it is asked and kept: an order never changes, and every send on an idle
    /// session asks it of each ordered send queued before it, which would read each of a long
    /// backlog again on every send.
    pub(super) async fn order_starts_runs(&self, submission_id: &str) -> Result<bool, String> {
        let known = self
            .ordered_starts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(submission_id)
            .copied();
        if let Some(starts) = known {
            return Ok(starts);
        }
        let starts = self
            .queued_order(submission_id)
            .await?
            .is_some_and(|queued| queued.order.starts_runs());
        let mut kept = self
            .ordered_starts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Sends that left the queue are dropped once they outnumber those still in it.
        let queue = &self.session.operations().queue;
        if kept.len() >= 2 * queue.len() + 16 {
            let queued: std::collections::HashSet<&str> = queue
                .iter()
                .map(|pending| pending.submission_id.as_str())
                .collect();
            kept.retain(|id, _| queued.contains(id.as_str()));
        }
        kept.insert(submission_id.to_owned(), starts);
        Ok(starts)
    }

    /// Ends every queued ordered send whose expiry has passed, durably, before the queue drains.
    /// A send whose order can't be read back is left for the drain, which ends it naming why.
    pub(super) async fn expire_ordered(&mut self) -> Result<(), String> {
        let now = now_ms();
        let mut records = Vec::new();
        let ids: Vec<String> = self
            .session
            .operations()
            .queue
            .iter()
            .filter(|pending| pending.ordered)
            .map(|pending| pending.submission_id.clone())
            .collect();
        for id in ids {
            let Ok(Some(queued)) = self.queued_order(&id).await else {
                continue;
            };
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

    /// Queues `start`, an ordered send. A retry of a send still queued is answered from its
    /// record; otherwise, its input goes through the session's `pre_user` transforms, and its
    /// records are written durably, superseding the undelivered send from the same sender under
    /// the same dedupe key. A send expired on arrival is answered as ended. On an idle session,
    /// a list naming `idle_start` starts its run now, and the sender hears `active` once the
    /// run's start is durable. `ordered_by` is the plan-named sender, when it isn't the owner.
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
            scope,
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
            self.run_plan(&request.model, request.variant.as_deref(), scope.is_some())
                .await?;
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
        // What the send was admitted with that the session doesn't hold yet: a scope it froze or
        // reopened, and, on a session no run has frozen a plan for, the plan's manifest. Its
        // record keeps them, so the run it starts records them even when the process stops
        // between that record and the run's start.
        let frozen_scope = self.ownership().map_err(SendOutcome::Transient)?.scope;
        let manifest_kept = match manifest
            .as_deref()
            .filter(|_| self.session.operations().owner.needs_admission())
        {
            Some(manifest) => Some(
                queue::receipt_manifest(manifest, &mut self.parts).map_err(SendOutcome::Error)?,
            ),
            None => None,
        };
        // Whose dedupe key this is: the plan-named sender, or the owner sending as itself.
        let sender = ordered_by.clone().or_else(|| request.principal.clone());
        // Only a send with a dedupe key supersedes one, so only its admission reads the queue's
        // orders back.
        let superseded = match order.dedupe_key.as_deref() {
            None => None,
            Some(dedupe_key) => self
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
                    .replaced_by(sender.as_ref(), Some(dedupe_key))
                })
                .map(|queued| queued.submission_id),
        };
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
            scope: scope.filter(|scope| frozen_scope.as_ref() != Some(scope)),
            manifest: manifest_kept,
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
        } else {
            // A send refused as expired freezes nothing; one queued freezes its policy with it.
            records.extend(self.retention_set(run_prepared.retention_policy.clone()));
        }
        // The queued record keeps the send's media, stored as the session's own objects before
        // the batch, as a queued send's are.
        if self.is_accepting_drain() && !expired && order.starts_runs() {
            self.track_recovery().map_err(SendOutcome::Transient)?;
        }
        self.append_own_batch_with_media(&records, &staged)
            .await
            .map_err(|error| SendOutcome::Error(format!("{error:?}")))?;
        if !expired && order.modes.first() == Some(&DeliveryMode::Steer) {
            if let Some(reason) = self.steer_yield(
                request.urgent_yield,
                sender.as_ref(),
                message_marks.as_ref(),
            ) {
                self.request_call_yield(reason);
            }
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
    /// Stages the attachments an ordered send's `content` names into its input and checks them
    /// against the running run's media, as its start froze them, when the list may steer into
    /// that run; otherwise, it checks them against the profile the executor quotes for the
    /// send's model. A send holding media whose list names neither `steer` nor `idle_start` is
    /// refused as media its carrier can't take: a tool result carries text only, and
    /// turn-scoped system text is a template's text.
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

/// Whether a send that leaves its model to the session may take the model of the run it joins:
/// it may do so when its list names a mode delivered into a running run.
pub(super) fn joins_a_run(order: &DeliveryOrder) -> bool {
    order.modes.iter().any(|mode| {
        matches!(
            mode,
            DeliveryMode::Steer | DeliveryMode::ToolResult | DeliveryMode::TurnSystem
        )
    })
}
