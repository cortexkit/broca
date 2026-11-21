//! `session.read {tag, call_key}` (llm-runner `tag_read`): a flat selector parsed instead of a
//! page request when `tag` is present. The session's handler admits only the session's owner,
//! from the principal the daemon attests opened the route and its stamp on the route (see
//! `store_tag_read::check_owner`), then resolves the number in the call's request.

use cortexkit_role_llm_runner::tag_read::TagReadRequest;

use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn tag_read<H: ServeHandler + 'static>(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    session: &BrocaBindIdentity,
    bind: &BindObservation,
    params: &serde_json::Value,
    handler: &Arc<H>,
) -> Result<(), ServeIoError> {
    let request = match TagReadRequest::decode(params) {
        Ok(request) => request,
        Err(field) => {
            let message = format!("a tag read takes exactly tag and call_key; {field} is wrong");
            return send_invalid_params(writer, ver, channel, epoch, corr, Some(field), &message)
                .await;
        }
    };
    match handler
        .tag_read(session, &request, bind.principal.as_ref(), &bind.scope)
        .await
    {
        Ok(Some(answer)) => send_json_response(writer, ver, channel, epoch, corr, &answer).await,
        Ok(None) => {
            send_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                NOT_SERVED_ON_NEW_STORAGE,
                "a tag read is served only by a handler with the session store",
            )
            .await
        }
        Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
    }
}
