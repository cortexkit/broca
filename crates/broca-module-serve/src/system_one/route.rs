//! Keep System One provider waits and panics off the session request slots and frame loop.

use super::{
    caller_module, log_served_request, request_operation, send_error_bytes, send_json_response,
    Arc, BindObservation, BrocaBindIdentity, EgressSender, Frame, JoinSet, RouteRole, ServeIoError,
};
use crate::system_one::{internal_refusal, refusal_body, SystemOne};

pub(super) fn spawn_request(
    body: Vec<u8>,
    bind: BindObservation,
    session: BrocaBindIdentity,
    service: Option<Arc<SystemOne>>,
    writer: &EgressSender<Frame>,
    request_tasks: &mut JoinSet<Result<(), ServeIoError>>,
    (ver, channel, epoch, corr): (u8, u16, u32, u64),
) {
    let writer = writer.clone();
    request_tasks.spawn(async move {
        let operation = request_operation(&body);
        let caller = caller_module(&bind).map(str::to_owned);
        log_served_request(&operation, &session, caller.as_deref(), "reply", async move {
            let extractor = bind.role == RouteRole::Extractor;
            let answered = match service {
                Some(service) => tokio::spawn(async move {
                    let principal = bind.principal.as_ref();
                    if extractor {
                        service.answer_extractor(&body, principal, &bind.scope).await
                    } else {
                        service.answer(&body, principal, &bind.scope).await
                    }
                }).await,
                None => {
                    let role = if extractor { "extractor" } else { "classifier" };
                    Ok(Err(serde_json::to_vec(&subc_protocol::ErrorBody::new(
                        "unknown_method", format!("this build serves no {role}/v1 role"),
                    )).unwrap_or_default()))
                }
            };
            match answered {
                Ok(Ok(result)) => send_json_response(&writer, ver, channel, epoch, corr, &result).await,
                Ok(Err(body)) => send_error_bytes(&writer, ver, channel, epoch, corr, body).await,
                Err(error) => {
                    tracing::error!(target: "system_one", %error, "answering a System One request failed");
                    let refusal = internal_refusal("serving this request failed inside broca; send it again");
                    send_error_bytes(&writer, ver, channel, epoch, corr, refusal_body(&refusal)).await
                }
            }
        }).await
    });
}
