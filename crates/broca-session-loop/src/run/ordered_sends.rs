//! Reading the queue's sends that carry a delivery order, which a running run delivers at its
//! step boundaries and batch closes, and ending one whose records can't be found.

use super::*;

/// A queued send with a delivery order, as a boundary delivers it.
pub(super) struct OrderedSend {
    pub(super) order: DeliveryOrder,
    /// The user messages a `steer` or `turn_system` delivery places: the input after the send's
    /// `pre_user` hooks, without system messages.
    pub(super) input: Vec<Message>,
    pub(super) written_at_ms: Option<u64>,
    /// The send's id as a ride names it: the caller's `send_id`, else its submission id.
    pub(super) send_id: String,
    /// Who a ride names as its sender: the module that sent it, else `broca`.
    pub(super) ride_sender: String,
    /// What a ride's text is shown as having come from: its sender and the plan template it was
    /// rendered from.
    pub(super) notice: Notice,
    /// Where the queued record stored the bytes of each medium the send's messages carry, by
    /// digest: a delivered copy of a message points at the same objects.
    pub(super) media: std::collections::HashMap<String, broca_session_store::record::MediaObject>,
}

impl OrderedSend {
    /// Where each medium of `message`, a message this send delivers, is stored. Matched by
    /// digest, since the send's `pre_user` transforms may have rewritten the messages its
    /// queued record kept.
    pub(super) fn media_of(
        &self,
        message: &Message,
    ) -> Vec<broca_session_store::record::InputMedia> {
        broca_session_store::session::message_media(message)
            .into_iter()
            .filter_map(|(block, part, reference)| {
                self.media.get(&reference.sha256).map(|object| {
                    broca_session_store::record::InputMedia {
                        block,
                        part,
                        log: object.log,
                        generation: object.generation,
                        seq: object.seq,
                    }
                })
            })
            .collect()
    }
}

/// The module a principal names, or `broca` if there is none.
fn module_or_broca(principal: Option<&broca_protocol::BindPrincipal>) -> String {
    match principal {
        Some(broca_protocol::BindPrincipal::Reserved { module_id }) => module_id.clone(),
        _ => "broca".into(),
    }
}

/// What a run has read of each ordered send waiting in the queue, by submission id, with the
/// queue entry it was read for. A send's records never change once it is queued, so each is read
/// and decoded once while it waits rather than at every boundary; one whose entry changes is read
/// again, and one that leaves the queue is forgotten.
#[derive(Default)]
pub(super) struct OrderedSendsRead(
    std::collections::HashMap<
        String,
        (
            broca_session_store::fold::operations::Pending,
            Arc<OrderedSend>,
        ),
    >,
);

/// The queue's sends that carry a delivery order, read from their records (or from what `read`
/// kept of them): the order, the input, who sent each send, and when it was accepted. A send
/// whose order or request record can't be found is taken out of the queue (see
/// [`end_unreadable_sends`]) rather than failing every boundary of every run after it.
pub(super) async fn ordered_sends<W: Writer>(
    writer: &mut W,
    read: &mut OrderedSendsRead,
) -> Result<std::collections::BTreeMap<String, Arc<OrderedSend>>, LoopError> {
    // The session's actor queues sends while the run waits on its model or its tools, so the
    // run's copy of the queue may lag. Without catching up, a send queued while a tool ran
    // would miss that tool's results and wait for a later boundary.
    writer.caught_up().await?;
    // In queue order, so the records ending any unreadable ones are written in that order.
    let waiting: Vec<broca_session_store::fold::operations::Pending> = writer
        .operations()
        .queue
        .iter()
        .filter(|pending| pending.ordered)
        .cloned()
        .collect();
    read.0
        .retain(|_, (kept, _)| waiting.iter().any(|pending| pending == kept));
    let mut sends = std::collections::BTreeMap::new();
    let mut unreadable = Vec::new();
    for pending in waiting {
        let id = pending.submission_id.clone();
        if let Some((_, send)) = read.0.get(&id) {
            sends.insert(id, Arc::clone(send));
            continue;
        }
        let records = writer.send_records(&id).await?;
        match ordered_send(&id, &records)? {
            Some(send) => {
                let send = Arc::new(send);
                read.0.insert(id.clone(), (pending, Arc::clone(&send)));
                sends.insert(id, send);
            }
            None => unreadable.push(id),
        }
    }
    if !unreadable.is_empty() {
        end_unreadable_sends(writer, &unreadable).await?;
    }
    Ok(sends)
}

/// Takes `unreadable`, ordered sends whose order or request record can't be found, out of the
/// queue: no boundary can deliver them, and reading them again would fail at every boundary.
/// Each is retracted as a send whose run failed for it, so its sender's status shows it finished
/// with the reason (a retraction without one would say the sender withdrew it), durable before
/// the run goes on.
async fn end_unreadable_sends<W: Writer>(
    writer: &mut W,
    unreadable: &[String],
) -> Result<(), LoopError> {
    let run_id = writer
        .operations()
        .run
        .as_ref()
        .map(|run| run.run_id.clone())
        .unwrap_or_default();
    let mut records = Vec::with_capacity(unreadable.len());
    for id in unreadable {
        let message = format!("queued send {id} is missing its order or its request");
        tracing::warn!(
            target: "session.send",
            submission_id = %id,
            run_id = %run_id,
            "{message}; the send is ended undelivered"
        );
        let failure = broca_records::record::QueuedAdmissionFailure {
            run_id: broca_protocol::RunId(run_id.clone()),
            error: ProviderError {
                class: broca_protocol::ErrorClass::Permanent,
                message,
                provider_code: Some("runner_error".into()),
                provider_details: None,
                cause: None,
                provider_subcode: None,
                refusal: None,
                status: None,
                retry_after_secs: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
                account_continue: None,
            },
        };
        records.push(Record::Retraction(
            broca_session_store::record::Retraction {
                submission_id: id.clone(),
                failure: Some(
                    serde_json::to_value(failure).map_err(|e| LoopError::Parts(e.to_string()))?,
                ),
            },
        ));
    }
    writer.append(&records, true).await?;
    Ok(())
}

/// The ordered send `id` as its records `records` give it, or `None` when they hold no order or
/// no request.
fn ordered_send(id: &str, records: &[Record]) -> Result<Option<OrderedSend>, LoopError> {
    let id = id.to_owned();
    let order = records.iter().find_map(|record| match record {
        Record::DeliveryOrdered(ordered) => Some(ordered.order.clone()),
        _ => None,
    });
    let queued = records.iter().find_map(|record| match record {
        Record::QueuedSend(queued) => Some(queued),
        _ => None,
    });
    let (Some(order), Some(queued)) = (order, queued) else {
        return Ok(None);
    };
    let order: DeliveryOrder =
        serde_json::from_value(order).map_err(|e| LoopError::Parts(e.to_string()))?;
    let principal = |value: Option<&Value>| -> Result<_, LoopError> {
        value
            .map(|value| serde_json::from_value::<broca_protocol::BindPrincipal>(value.clone()))
            .transpose()
            .map_err(|e| LoopError::Parts(format!("queued send {id}'s sender: {e}")))
    };
    let ordered_by = principal(records.iter().find_map(|record| match record {
        Record::DeliveryOrdered(ordered) => ordered.sent_by.as_ref(),
        _ => None,
    }))?;
    let receipt_value = queued
        .receipt
        .as_ref()
        .map(broca_protocol::json::Json::parse);
    let receipt = |name: &str| receipt_value.as_ref().and_then(|receipt| receipt.get(name));
    let queued_by = principal(receipt("sent_by"))?;
    let queued_as = principal(receipt("principal"))?;
    // A ride names the module its queued record says sent it; the notice names the one its
    // order says.
    let ride_sender = module_or_broca(
        queued_by
            .as_ref()
            .or(ordered_by.as_ref())
            .or(queued_as.as_ref()),
    );
    let notice_sender = broca_core::compaction_v2::entries::delivery_sender(
        ordered_by
            .as_ref()
            .or(queued_by.as_ref())
            .or(queued_as.as_ref()),
        DeliveryMode::ToolResult,
    )
    .unwrap_or_else(|| "broca".into());
    let notice = Notice {
        sender: notice_sender,
        template: order.template.as_ref().map(|template| template.id.clone()),
    };
    let hooked = queued
        .receipt
        .as_ref()
        .map(broca_protocol::json::Json::parse)
        .and_then(|receipt| receipt.get("hooks")?.get("final_value").cloned())
        .and_then(|value| serde_json::from_value::<Vec<Message>>(value).ok());
    let input = hooked
        .unwrap_or_else(|| queued.input.clone())
        .into_iter()
        .filter(|message| message.role != Role::System)
        .collect();
    let send_id = queued.send_id.clone().unwrap_or_else(|| id.clone());
    let media = broca_session_store::session::stored_media(&Record::QueuedSend(queued.clone()))
        .into_iter()
        .collect();
    Ok(Some(OrderedSend {
        order,
        input,
        written_at_ms: queued.written_at_ms,
        send_id,
        ride_sender,
        notice,
        media,
    }))
}
