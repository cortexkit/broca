//! Application requests on an inbound route.

use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_route_request_inner<H>(
    req: RouteRequest,
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    session: &BrocaBindIdentity,
    bind: &BindObservation,
    handler: &Arc<H>,
    serves: Option<&crate::serves::Source>,
) -> Result<(), ServeIoError>
where
    H: ServeHandler + 'static,
{
    if req.method == OP_SEND {
        if let Err(refusal) =
            broca_core::scope_admission::require_flow_scope(bind.principal.as_ref(), &bind.scope)
        {
            return send_serve_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                &ServeError::ScopeRefused(refusal),
            )
            .await;
        }
    }
    if matches!(
        req.method.as_str(),
        OP_SEND | cortexkit_role_llm_runner::ops::SESSION_REFRESH
    ) {
        if let Some(plan) = req
            .params
            .get("plan")
            .and_then(serde_json::Value::as_object)
        {
            if let Err(field) =
                cortexkit_role_llm_runner::attribution::check_plan(plan, &[] as &[&str])
            {
                return send_invalid_params(writer, ver, channel, epoch, corr, Some(field), "sender_attribution is not declared; the plan's attribution member is unsupported").await;
            }
        }
    }
    match req.method.as_str() {
        cortexkit_role_llm_runner::ops::SESSION_MEDIA_READ => {
            match handler.media_read(session, req.params).await {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        "session.upload_begin"
        | "session.upload_append"
        | "session.upload_commit"
        | "session.upload_status" => {
            match handler
                .upload(
                    session,
                    bind.principal.as_ref(),
                    &bind.scope,
                    &req.method,
                    req.params,
                )
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_SEND_STATUS | OP_SESSION_SEND_STATUS => {
            let params: broca_wire::SendStatusRequest =
                match serde_json::from_value(req.params.clone()) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            broca_wire::failing_field::<broca_wire::SendStatusRequest>(
                                &req.params,
                                &error,
                            ),
                            &error.to_string(),
                        )
                        .await
                    }
                };
            let run_id = run_id_for_send(session, Some(&params.send_id));
            match handler
                .send_status(session, run_id, &params.send_id, bind.principal.as_ref())
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_SEND => {
            if req
                .params
                .get("seed_from")
                .is_some_and(|seed_from| !seed_from.is_null())
            {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "unsupported_seed_from",
                    "seed_from is reserved for lineage forks and is not supported in v1",
                )
                .await;
            }
            if let Err(message) = broca_wire::check_upload_references(&req.params) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("content".into()),
                    &message,
                )
                .await;
            }
            // Surface what this send carries that broca does not act on: an ignored key, or a
            // variant that is recorded but not yet applied. Warn only; never refuse here.
            crate::send_audit::warn_send_notices(
                crate::send_audit::global_send_auditor(),
                &req.params,
                &session.harness,
            );
            if let Err((field, message)) = broca_wire::check_tool_replies(&req.params) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field),
                    &message,
                )
                .await;
            }
            if let Err(field) = broca_wire::check_run_parameters(&req.params) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.clone()),
                    &format!("invalid {field}"),
                )
                .await;
            }
            let mut send_params = req.params.clone();
            let service_tier_level = if broca_protocol::service_tier_enabled() {
                match cortexkit_role_llm_runner::service_tier::parse_request(
                    req.params.get("service_tier"),
                ) {
                    Ok(level) => {
                        if let Some(object) = send_params.as_object_mut() {
                            object.remove("service_tier");
                        }
                        level.map(|level| level.as_str().to_owned())
                    }
                    Err(problem) => {
                        return send_error_body(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            ErrorBody::new(
                                "invalid_params",
                                format!("unsupported service_tier {}", req.params["service_tier"]),
                            )
                            .with_detail(problem.detail()),
                        )
                        .await;
                    }
                }
            } else {
                None
            };
            if let Some(body) = crate::cache_routing::send_cache_refusal(&req.params) {
                let error = ErrorBody::new(body.code, body.message).with_detail(body.detail);
                return send_error_body(writer, ver, channel, epoch, corr, error).await;
            }
            let mut params: SendParams = match serde_json::from_value(send_params) {
                Ok(p) => p,
                Err(e) => {
                    let field = broca_wire::send_budget_refusal_field(&req.params)
                        .or_else(|| broca_wire::send_structured_output_refusal_field(&req.params))
                        .or_else(|| broca_wire::failing_field::<SendParams>(&req.params, &e));
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &e.to_string(),
                    )
                    .await;
                }
            };
            if params
                .auth
                .as_ref()
                .is_some_and(crate::wire::auth_pin_hint_conflict)
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("auth.account".into()),
                    crate::wire::AUTH_PIN_HINT_CONFLICT,
                )
                .await;
            }
            if params.template.is_some()
                && (req.params.get("prompt").is_some() || req.params.get("prompt_blocks").is_some())
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("template".into()),
                    "a template send must not also carry prompt or prompt_blocks",
                )
                .await;
            }
            let order = match params.delivery_order() {
                Ok(order) => order,
                Err((field, message)) => {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some(field.into()),
                        &message,
                    )
                    .await;
                }
            };
            if let Err(field) = params.check_prompt_for(true) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.into()),
                    "prompt must contain text or media; a template send must omit prompt",
                )
                .await;
            }
            let describe = broca_wire::broca_role_describe_with_writers(
                env!("CARGO_PKG_VERSION"),
                handler.compaction_supported(),
                crate::RETENTION_WRITER_ENABLED,
                handler.hook_plane_available(),
            )
            .role;
            if let Some(refusal) = delivery_list_refusal(params.delivery.as_ref(), &describe) {
                return send_error_body(writer, ver, channel, epoch, corr, refusal).await;
            }
            // broca keeps a send's client_message_id only in the send's identity digest, which
            // exists for a send that names its `send_id` (an unkeyed send's run id never
            // repeats, so broca records no identity for it). An id on an unkeyed send would
            // therefore be dropped; it is refused instead, naming the field.
            if params.client_message_id.is_some() && params.send_id.is_none() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("client_message_id".into()),
                    "client_message_id requires send_id: broca stores it with the send's identity",
                )
                .await;
            }
            let tools = req.params.get("tools").is_some_and(|tools| {
                !tools.is_null() && tools.as_array().is_none_or(|items| !items.is_empty())
            });
            let planned_tools = req
                .params
                .pointer("/plan/tool_items")
                .is_some_and(|items| items.as_array().is_none_or(|items| !items.is_empty()));
            if let Some(field) = broca_core::scope_admission::model_only_scope_tool_field(
                &bind.scope,
                tools,
                planned_tools,
            ) {
                let flow = matches!(&bind.scope, broca_protocol::SendScope::Stamped(stamp) if stamp.scope.flow_id.is_some());
                let message = match (flow, field) {
                    (true, "tools") => "flow sends cannot carry tools",
                    (true, _) => "flow fetch plans cannot carry plan.tool_items",
                    (false, "tools") => "agent-run sends cannot carry tools",
                    (false, _) => "agent-run fetch plans cannot carry plan.tool_items",
                };
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.into()),
                    message,
                )
                .await;
            }
            if let Some(plan) = req
                .params
                .get("plan")
                .and_then(serde_json::Value::as_object)
            {
                let supported = match handler
                    .plan_readers_supported(session, params.session_class.as_deref())
                {
                    Ok(supported) => supported,
                    Err(error) => {
                        return send_serve_error(writer, ver, channel, epoch, corr, &error).await
                    }
                };
                if let Err(error) = broca_module::reader_admission::check_plan(plan, supported) {
                    return send_serve_error(writer, ver, channel, epoch, corr, &error).await;
                }
            }
            use broca_module::executor::CreatingPlanSelectionError;
            let plan_selection =
                match send_plan::creating_plan_selection(&**handler, session, &req.params).await {
                    Ok(selection) => selection,
                    Err(CreatingPlanSelectionError::Invalid(problem)) => {
                        let body = send_plan::invalid_plan_selection(problem);
                        return send_error_body(writer, ver, channel, epoch, corr, body).await;
                    }
                    Err(CreatingPlanSelectionError::CredentialsUnavailable) => {
                        let error = ServeError::CredentialsUnavailable;
                        return send_serve_error(writer, ver, channel, epoch, corr, &error).await;
                    }
                };
            if params.model.is_none() {
                if let Some(selection) = &plan_selection {
                    params.model = Some(crate::wire::ModelParams {
                        provider: selection.provider.clone(),
                        model: selection.model.clone(),
                        variant: selection.variant.clone(),
                    });
                }
            }
            // Deliveries that can join an episode take omitted runner parameters from its
            // frozen config, so they render the session's own request prefix. Admission resolves
            // them against its history snapshot; this preflight only classifies log presence.
            // Deliveries that can only start a new run take defaults and must name a model.
            let mut inherit_frozen: std::collections::BTreeSet<
                broca_protocol::request::InheritedParam,
            > = if match params.delivery.as_ref() {
                Some(broca_wire::SendDelivery::Mode(_)) => matches!(
                    params.legacy_delivery(),
                    broca_protocol::Delivery::Steer | broca_protocol::Delivery::Interrupt
                ),
                // Only modes that can join an episode need its frozen settings. Starting or
                // queueing a new run takes defaults even when expressed as an ordered list.
                Some(broca_wire::SendDelivery::Order(modes)) => modes.iter().any(|mode| {
                    matches!(
                        mode,
                        broca_protocol::DeliveryMode::Steer
                            | broca_protocol::DeliveryMode::ToolResult
                            | broca_protocol::DeliveryMode::TurnSystem
                    )
                }),
                None => false,
            } {
                broca_protocol::request::InheritedParam::ALL
                    .iter()
                    .filter(|(_, member)| {
                        req.params
                            .get(*member)
                            .is_none_or(serde_json::Value::is_null)
                    })
                    .map(|(param, _)| *param)
                    .collect()
            } else {
                Default::default()
            };
            let session_written = if inherit_frozen.is_empty() && params.model.is_some() {
                false
            } else {
                // The presence probe can stat the live session log and read archive containers,
                // so it runs on the blocking pool rather than on this request's runtime worker.
                let probe_handler = Arc::clone(handler);
                let probe_session = session.clone();
                let probed = broca_diagnostics::spawn_blocking(
                    broca_diagnostics::JobKind::WalRead,
                    move || probe_handler.session_written(&probe_session),
                )
                .await
                .unwrap_or_else(|error| {
                    Err(ServeError::Open(
                        format!("session presence probe failed: {error}; retry the send"),
                        Some(broca_protocol::ErrorClass::Transient),
                    ))
                });
                match probed {
                    Ok(written) => written,
                    Err(error) => {
                        return send_serve_error(writer, ver, channel, epoch, corr, &error).await
                    }
                }
            };
            // Keep caller omission distinct from the value resolved from the creating plan.
            let model_inherited = req
                .params
                .get("model")
                .is_none_or(serde_json::Value::is_null);
            if params.model.is_none() {
                let may_inherit = session_written;
                if !may_inherit {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some("model".into()),
                        "model is required unless plan.model selects it or the session has a recorded selection",
                    )
                    .await;
                }
                // An empty placeholder, replaced with the session's frozen model before the send
                // is recorded as a queued send or admitted (`InheritedParam::Model`), so it is
                // never rendered and never written to the log.
                params.model = Some(crate::wire::ModelParams {
                    provider: String::new(),
                    model: String::new(),
                    variant: None,
                });
                inherit_frozen.insert(broca_protocol::request::InheritedParam::Model);
            }
            // A session never written has no frozen values, so its first send keeps the
            // defaults for everything it omits.
            let inherit_frozen = if !session_written {
                if model_inherited && plan_selection.is_some() {
                    std::collections::BTreeSet::from([
                        broca_protocol::request::InheritedParam::Model,
                    ])
                } else {
                    Default::default()
                }
            } else {
                inherit_frozen
            };
            if let Some(detail) = execute_class_root_refusal(session).await {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_project_root",
                    &detail,
                )
                .await;
            }
            // A send carrying a fetch plan is checked before it reaches the session and, for a
            // session with no episode yet, every provider the plan names is fetched now. A
            // refusal here is answered before anything is written to the session's log. The
            // session's actor checks the plan again against the session's history as it admits
            // the send, in case an episode was written in between.
            if !handler.hook_plane_available() && handler.frozen_plan_requires_hooks(session).await
            {
                return send_serve_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    &ServeError::PlanRefused(broca_fetch_plan::Refusal::InvalidParams {
                        detail: serde_json::json!({"field":"plan.step_transform_items"}),
                    }),
                )
                .await;
            }
            if !matches!(&bind.scope, broca_protocol::SendScope::Stamped(_))
                && handler.frozen_plan_requires_approval(session).await
            {
                return send_error_body(writer, ver, channel, epoch, corr, approve_scope_error())
                    .await;
            }
            let plan = match req.params.get("plan").filter(|plan| !plan.is_null()) {
                None => None,
                Some(plan) => {
                    let scope = match &bind.scope {
                        broca_protocol::SendScope::Stamped(stamp) => Some(&stamp.scope),
                        _ => None,
                    };
                    match admit_send_plan(
                        handler.as_ref(),
                        session,
                        &req.params,
                        plan,
                        scope,
                        // An inherited model is known only to the session's actor, which checks
                        // the plan's models against it; the placeholder must never be compared.
                        if model_inherited && plan_selection.is_none() {
                            None
                        } else {
                            params.model.as_ref()
                        },
                        |senders, owner| {
                            if params.urgent_yield {
                                broca_module::serve::check_urgent_yield(
                                    owner,
                                    bind.principal.as_ref(),
                                    params.legacy_delivery(),
                                    order.as_ref(),
                                    senders,
                                )
                            } else {
                                Ok(())
                            }
                        },
                    )
                    .await
                    {
                        Ok(plan) => Some(plan),
                        Err(refusal) => {
                            return send_error_body(
                                writer,
                                ver,
                                channel,
                                epoch,
                                corr,
                                refusal.wire_body(),
                            )
                            .await
                        }
                    }
                }
            };
            // A storage policy is decoded and checked before anything reaches the session. A send
            // carries it or the older `retention` seconds, never both.
            let retention_policy = match req
                .params
                .get("retention_policy")
                .filter(|policy| !policy.is_null())
            {
                None => None,
                Some(_) if params.retention.is_some() => {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some("retention_policy".into()),
                        "a send carries retention_policy or retention, never both",
                    )
                    .await;
                }
                Some(policy) => {
                    match cortexkit_role_llm_runner::session_retention::RetentionPolicy::from_value(
                        policy,
                    ) {
                        Ok(policy) => Some(policy),
                        Err(field) => {
                            return send_invalid_params(
                                writer,
                                ver,
                                channel,
                                epoch,
                                corr,
                                Some(field),
                                "the send's retention policy is not valid",
                            )
                            .await;
                        }
                    }
                }
            };
            let run_id = run_id_for_send(session, params.send_id.as_deref());
            let own_run_id = run_id.clone();
            let bound_session = session.clone();
            let mut send_req = match params.into_send_request(session.clone(), run_id) {
                Ok(request) => request,
                Err(broca_core::RunError::Config(message)) => {
                    return send_error(writer, ver, channel, epoch, corr, "config", &message).await;
                }
                Err(error) => unreachable!("send parameter validation returned {error}"),
            };
            send_req.request.project_id = bind.project_id.clone();
            send_req.request.retention_policy = retention_policy;
            send_req.request.message_marks = ["speaks_for", "notice", "keep"]
                .into_iter()
                .filter_map(|field| {
                    req.params
                        .get(field)
                        .map(|value| (field.into(), value.clone()))
                })
                .collect();
            send_req.request.inherit_frozen = inherit_frozen;
            if broca_protocol::service_tier_enabled() {
                send_req.request.service_tier_level = service_tier_level;
                send_req
                    .request
                    .inherit_frozen
                    .retain(|param| *param != broca_protocol::request::InheritedParam::ServiceTier);
            }
            send_req.stop_sequences_named =
                req.params.pointer("/generation/stop_sequences").is_some();
            send_req.request.principal = bind.principal.clone();
            send_req.request.scope = bind.scope.clone();
            send_req.plan = plan;
            // Measured here, after every serve-side refusal, so only sends that reach the
            // session are counted. Compare against the resolved storage identity.
            if matches!(&bind.scope, broca_protocol::SendScope::Stamped(_)) {
                crate::scope_workdir::observe_scoped_send(
                    session,
                    bind.stamped_project.as_ref(),
                    bind.project_id.as_deref(),
                );
            }
            let refusal_session_ref = send_req.session.session.clone();
            let refusal_send_id = send_req.send_id.clone();
            let refusal_delivery_mode = send_req.delivery.as_str();
            match handler.send(send_req).await {
                Ok(resp) => {
                    let (result, delivered) = match resp {
                        broca_module::SendResponse::Finished {
                            run_id,
                            reason,
                            delivered,
                        } => (
                            SendResult::Finished {
                                run_id,
                                reason: reason.into(),
                            },
                            delivered,
                        ),
                        broca_module::SendResponse::Paused {
                            run_id,
                            pause_reason,
                            delivered,
                        } => (
                            SendResult::Paused {
                                run_id,
                                pause_reason: pause_reason.into(),
                            },
                            delivered,
                        ),
                        broca_module::SendResponse::Active { run_id, delivered } => {
                            (SendResult::Active { run_id }, delivered)
                        }
                        broca_module::SendResponse::Pending { submission_id } => {
                            (SendResult::Pending { submission_id }, None)
                        }
                    };
                    // The reply to the send that admitted the session (its first episode), and to
                    // every retry of that send, carries the session's baseline (see
                    // `session.baseline`); any other send's reply carries none. A send admitted
                    // the session when the first episode runs under the send's own run id, so a
                    // steer that joined that episode (and answers its run id) did not. The
                    // baseline comes from what the session's actor already holds, so producing
                    // it reads nothing.
                    let baseline = match &result {
                        SendResult::Active { run_id } | SendResult::Finished { run_id, .. }
                            if *run_id == own_run_id.0 =>
                        {
                            handler.admission_baseline(
                                &bound_session,
                                &broca_protocol::RunId(run_id.clone()),
                            )
                        }
                        _ => None,
                    };
                    let reply = broca_wire::SendReplyWire {
                        result,
                        baseline,
                        delivered,
                    };
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Err(error) => {
                    log_refused_send(
                        &refusal_session_ref,
                        refusal_send_id.as_deref(),
                        refusal_delivery_mode,
                        &error,
                    );
                    if let ServeError::Upload(upload) = &error {
                        return send_error_body(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            upload_error_body(upload, true),
                        )
                        .await;
                    }
                    send_serve_error(writer, ver, channel, epoch, corr, &error).await
                }
            }
        }
        OP_IMPORT if req.params.get("source").is_some() => {
            // A head's history is read from a file exported by another host into a session
            // created by the session store. An import carrying its messages inline is handled
            // by the inline import below.
            let params: broca_wire::head_import::FileImportParams =
                match serde_json::from_value(req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &error.to_string(),
                        )
                        .await
                    }
                };
            let Some(principal) = bind.principal.clone() else {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "principal_required",
                    "an import needs the daemon's attested principal on its route",
                )
                .await;
            };
            // An import's retention is checked against the same limits as a send's. A repeat of
            // the import is matched against its receipt, which compares the retention too.
            if let Err(detail) =
                broca_module::retention::validate_retention(params.retention, None, false)
            {
                let error = ServeError::RetentionInvalid(detail);
                return send_serve_error(writer, ver, channel, epoch, corr, &error).await;
            }
            // The roots belong to the session store's runtime, which opens at the module's start
            // or, when that open failed, once the deployment key loads.
            let Some(roots) = handler.import_roots() else {
                let error = ServeError::Transient(
                    "the session store hasn't opened yet; retry once it has".into(),
                );
                return send_serve_error(writer, ver, channel, epoch, corr, &error).await;
            };
            let command = broca_module::store_import::StoreCommand::Import(Box::new(
                broca_module::store_import::ImportCommand {
                    params,
                    principal,
                    scope: match &bind.scope {
                        broca_protocol::SendScope::Stamped(stamped) => Some(stamped.scope.clone()),
                        _ => None,
                    },
                    roots,
                    // Wall-clock time: it stamps the seeded run's records, and retention counts
                    // the session's deadline from it.
                    now_ms: broca_core::clock::epoch_millis().unwrap_or(0),
                },
            ));
            send_store_outcome(
                handler.as_ref(),
                session,
                command,
                writer,
                ver,
                channel,
                epoch,
                corr,
            )
            .await
        }
        OP_RESTORE_FROM_BACKUP => {
            // Only the operator's own connection restores: restore rewrites a session's head and
            // cuts its log.
            if !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct)) {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "operator_required",
                    "session.restore_from_backup is for the operator only",
                )
                .await;
            }
            let all = match restore_all(&req.params) {
                Ok(all) => all,
                Err(message) => {
                    return send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "invalid_params",
                        &message,
                    )
                    .await
                }
            };
            match handler.restore_from_backup(session, all).await {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(refusal) => {
                    let mut body = ErrorBody::new(refusal.code, refusal.message);
                    if let Some(detail) = refusal.detail {
                        body = body.with_detail(detail);
                    }
                    send_error_body(writer, ver, channel, epoch, corr, body).await
                }
            }
        }
        OP_TAG_MAP => {
            // Checked again here so no path reaches the read without the operator's own
            // connection.
            if !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct)) {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "operator_required",
                    "session.tag_map is for the operator only",
                )
                .await;
            }
            let params: broca_wire::tag_map::TagMapParams = if req.params.is_null() {
                Default::default()
            } else {
                match serde_json::from_value(req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &error.to_string(),
                        )
                        .await
                    }
                }
            };
            match handler.tag_map(session, &params).await {
                Ok(Some(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(None) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        NO_SESSION_STORE,
                        "session.tag_map is served only by a handler with the session store",
                    )
                    .await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_REQUEST_DUMP => {
            // Checked again here so no path reaches the rebuild without the operator's own
            // connection.
            if !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct)) {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "operator_required",
                    "session.request_dump is for the operator only",
                )
                .await;
            }
            let params: broca_wire::request_dump::RequestDumpParams =
                match serde_json::from_value(req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &error.to_string(),
                        )
                        .await
                    }
                };
            match handler.request_dump(session, &params.run_id).await {
                Ok(Some(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(None) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        NO_SESSION_STORE,
                        "session.request_dump is served only by a handler with the session store",
                    )
                    .await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_SESSION_DELETE => {
            // Checked again here so no path reaches the deletion without the operator's own
            // connection.
            if !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct)) {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "operator_required",
                    "session.delete is for the operator only",
                )
                .await;
            }
            let paused_run = match serde_json::from_value::<broca_wire::session_delete::DeleteParams>(
                req.params,
            ) {
                Ok(params) => params.paused_run,
                Err(error) => {
                    let message = error.to_string();
                    let code = "invalid_params";
                    return send_error(writer, ver, channel, epoch, corr, code, &message).await;
                }
            };
            match handler.delete_session(session, paused_run).await {
                Ok(Some(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(None) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        NO_SESSION_STORE,
                        "session.delete is served only by a handler with the session store",
                    )
                    .await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_INSTALL_VIEW => {
            let params: broca_wire::head_import::InstallViewParams =
                match serde_json::from_value(req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &error.to_string(),
                        )
                        .await
                    }
                };
            let Some(principal) = bind.principal.clone() else {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "principal_required",
                    "installing a view needs the daemon's attested principal on its route",
                )
                .await;
            };
            let command = broca_module::store_import::StoreCommand::InstallView(Box::new(
                broca_module::store_import::InstallViewCommand {
                    view: params.view,
                    principal,
                },
            ));
            send_store_outcome(
                handler.as_ref(),
                session,
                command,
                writer,
                ver,
                channel,
                epoch,
                corr,
            )
            .await
        }
        OP_IMPORT => {
            let params: ImportParams = match serde_json::from_value(req.params) {
                Ok(params) => params,
                Err(error) => {
                    return send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "invalid_params",
                        &error.to_string(),
                    )
                    .await
                }
            };
            if let Some(detail) = execute_class_root_refusal(session).await {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_project_root",
                    &detail,
                )
                .await;
            }
            let run_id = run_id_for_import(session, &params.import_id);
            let import_req = SessionImportRequest {
                project_id: bind.project_id.clone(),
                principal: bind.principal.clone(),
                scope: bind.scope.clone(),
                session: session.clone(),
                run_id,
                messages: params.messages,
                source_ref: params.origin.source_ref,
            };
            match handler.import(import_req).await {
                Ok(response) => {
                    let result: ImportResult = response;
                    send_json_response(writer, ver, channel, epoch, corr, &result).await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_RETRACT => {
            let params: RetractParams = match serde_json::from_value(req.params) {
                Ok(p) => p,
                Err(e) => {
                    return send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "invalid_params",
                        &e.to_string(),
                    )
                    .await
                }
            };
            let sid = broca_protocol::SubmissionId(params.submission_id);
            match handler.retract(session, sid).await {
                Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_CANCEL => {
            if req
                .params
                .get("at")
                .is_some_and(|at| !matches!(at.as_str(), Some("now" | "step_boundary")))
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("at".into()),
                    "run.cancel.at must be now or step_boundary",
                )
                .await;
            }
            let params: CancelParams = match serde_json::from_value(req.params) {
                Ok(p) => p,
                Err(e) => {
                    return send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "invalid_params",
                        &e.to_string(),
                    )
                    .await
                }
            };
            let at = match params.at.as_deref() {
                None | Some("now") => broca_records::record::RunCancelAt::Now,
                Some("step_boundary") => broca_records::record::RunCancelAt::StepBoundary,
                _ => {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some("at".into()),
                        "run.cancel.at is not a supported cancellation point",
                    )
                    .await
                }
            };
            match handler
                .cancel_from(
                    session,
                    broca_protocol::RunId(params.run_id),
                    at,
                    bind.principal.as_ref(),
                    &bind.scope,
                )
                .await
            {
                Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_RUN_ADMISSION => {
            let params: broca_wire::RunAdmissionParams =
                match serde_json::from_value(req.params.clone()) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            Some("run_id".into()),
                            &error.to_string(),
                        )
                        .await
                    }
                };
            match handler
                .run_admission(session, broca_protocol::RunId(params.run_id))
                .await
            {
                Ok(Some(response)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &response).await
                }
                Ok(None) => {
                    send_json_response(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        &broca_wire::RunStatusResponse::Unknown,
                    )
                    .await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_STATUS => {
            // run.status with an optional run_id: absent → the session's status; present →
            // that run's status.
            let params: StatusParams = if req.params.is_null() {
                StatusParams::default()
            } else {
                match serde_json::from_value(req.params) {
                    Ok(p) => p,
                    Err(e) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &e.to_string(),
                        )
                        .await
                    }
                }
            };
            match params.run_id {
                None => match handler.status(session).await {
                    Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                    Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
                },
                Some(run_id) => {
                    match handler
                        .run_status_reply(session, broca_protocol::RunId(run_id))
                        .await
                    {
                        Ok(resp) => {
                            send_json_response(writer, ver, channel, epoch, corr, &resp).await
                        }
                        Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
                    }
                }
            }
        }
        OP_RUN_EVENT_GET => {
            let params: broca_wire::RunEventGetParams = match decode_params(&req.params) {
                Ok(params) => params,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await
                }
            };
            if params.event_key.len() != 64
                || !params
                    .event_key
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("event_key".into()),
                    "run.event.get.event_key must be a 64-character lowercase hex key",
                )
                .await;
            }
            match handler.run_event_get(session, &params.event_key).await {
                Ok(response) => {
                    send_json_response(writer, ver, channel, epoch, corr, &response).await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_HISTORY => {
            let request = match decode_params::<cortexkit_role_llm_runner::history::HistoryRequest>(
                &req.params,
            ) {
                Ok(request) => request,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await
                }
            };
            match broca_module::history::read(handler.as_ref(), session, &request).await {
                Ok(page) => send_json_response(writer, ver, channel, epoch, corr, &page).await,
                Err(broca_module::history::HistoryError::Serve(error)) => {
                    send_serve_error(writer, ver, channel, epoch, corr, &error).await
                }
                Err(broca_module::history::HistoryError::Unrepresentable { ordinal, field }) => {
                    let body = serde_json::json!({
                        "code":"history_unrepresentable", "class":"permanent",
                        "message":format!("session.history cannot represent message {ordinal}: no retained {field}; use session.read for raw recovery"),
                        "detail":{"ordinal":ordinal,"field":field},
                    });
                    let bytes = serde_json::to_vec(&body)
                        .map_err(|error| ServeIoError(error.to_string()))?;
                    send_error_bytes(writer, ver, channel, epoch, corr, bytes).await
                }
                Err(broca_module::history::HistoryError::Refused { code, message }) => {
                    send_error(writer, ver, channel, epoch, corr, code, &message).await
                }
            }
        }
        broca_wire::plan_view::OP_SESSION_PLAN_VIEW => {
            if let Err((field, message)) =
                decode_params::<broca_wire::plan_view::PlanViewParams>(&req.params)
            {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            match handler.plan_view(session).await {
                Ok(Some(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(None) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        NO_SESSION_STORE,
                        "session.plan_view is served only by a handler with the session store",
                    )
                    .await
                }
                Err(broca_module::store_plan_view::Error::NoPlan) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "no_plan",
                        "the session has no plan manifest",
                    )
                    .await
                }
                Err(broca_module::store_plan_view::Error::TooLarge) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "too_large",
                        "the plan view exceeds the transport frame limit",
                    )
                    .await
                }
                Err(broca_module::store_plan_view::Error::Serve(error)) => {
                    send_serve_error(writer, ver, channel, epoch, corr, &error).await
                }
            }
        }
        OP_READ => {
            if req.params.get("tag").is_some() {
                let params = &req.params;
                return serve_tag_read::tag_read(
                    writer, ver, channel, epoch, corr, session, bind, params, handler,
                )
                .await;
            }
            let params: broca_wire::ReadParams = match decode_params(&req.params) {
                Ok(p) => p,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await
                }
            };
            let media = params.media.unwrap_or_default();
            // Tool media references ride only on a media_refs read.
            if params.tool_media.is_some()
                && media != cortexkit_role_llm_runner::read::MediaMode::MediaRefs
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("tool_media".into()),
                    "tool_media is valid only with media: \"media_refs\"",
                )
                .await;
            }
            if params.after_mid.is_some() {
                let refusal = if params.history_version.is_none() {
                    Some((
                        "history_version",
                        "after_mid needs the history_version it was read under",
                    ))
                } else if params.from_ordinal.is_some() {
                    Some((
                        "from_ordinal",
                        "after_mid and from_ordinal are two cursors; send one",
                    ))
                } else {
                    None
                };
                if let Some((field, message)) = refusal {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some(field.into()),
                        message,
                    )
                    .await;
                }
            }
            let read_req = SessionReadRequest {
                late_after: params.late_after,
                session: session.clone(),
                from_ordinal: params.from_ordinal,
                limit: params.limit,
                include_tools: params.include_tools,
                include_rich_ui: params.include_rich_ui,
                include_display_facts: params.include_display_facts,
                after_mid: params.after_mid,
                history_version: params.history_version,
                lineage_id: None,
                max_bytes: params.max_bytes,
                media,
                tool_media: params.tool_media.is_some(),
            };
            match handler.read(read_req).await {
                Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_ROLE_DESCRIBE => {
            trace_describe("authorized", channel, epoch, corr);
            let describe = crate::serves::describe(handler.as_ref(), serves).await;
            trace_describe("response_ready", channel, epoch, corr);
            let sent = send_json_response(writer, ver, channel, epoch, corr, &describe).await;
            if sent.is_ok() {
                trace_describe("response_queued", channel, epoch, corr);
            }
            sent
        }
        OP_HEAD => {
            if let Err((field, message)) = decode_params::<broca_wire::HeadParams>(&req.params) {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            match handler
                .head_for(session, bind.principal.as_ref(), &bind.scope)
                .await
            {
                Ok(head) => send_json_response(writer, ver, channel, epoch, corr, &head).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_RUN_RESULT => {
            let params: broca_wire::RunResultParams =
                match serde_json::from_value(req.params.clone()) {
                    Ok(p) => p,
                    Err(e) => {
                        let field = broca_wire::failing_field::<broca_wire::RunResultParams>(
                            &req.params,
                            &e,
                        );
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            field,
                            &e.to_string(),
                        )
                        .await;
                    }
                };
            match handler
                .run_result(session, broca_protocol::RunId(params.run_id))
                .await
            {
                Ok(result) => send_json_response(writer, ver, channel, epoch, corr, &result).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_RUN_TOOL_CALLS => {
            let request: cortexkit_role_llm_runner::tool_calls::ToolCallsRequest =
                match serde_json::from_value(req.params.clone()) {
                    Ok(request) => request,
                    Err(error) => {
                        let field = broca_wire::failing_field::<
                            cortexkit_role_llm_runner::tool_calls::ToolCallsRequest,
                        >(&req.params, &error);
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            field,
                            &error.to_string(),
                        )
                        .await;
                    }
                };
            if let Err(field) = request.check() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.into()),
                    "run.tool_calls.limit must be greater than zero",
                )
                .await;
            }
            match handler.run_tool_calls(session, request).await {
                Ok(page) => send_json_response(writer, ver, channel, epoch, corr, &page).await,
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_BASELINE => {
            if let Err((field, message)) = decode_params::<broca_wire::BaselineParams>(&req.params)
            {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            match handler
                .baseline(session, bind.principal.as_ref(), &bind.scope)
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_REFRESH => {
            let advertised = handler
                .plan_admitter()
                .is_some_and(|admitter| admitter.admits_output_refs());
            let request = match serve_refresh::decode(&req.params, advertised) {
                Ok(request) => request,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await;
                }
            };
            match handler
                .session_refresh(
                    session,
                    bind.principal.as_ref(),
                    &bind.scope,
                    request.plan,
                    request.policy,
                    request.generation,
                    request.refetch.unwrap_or_default(),
                    request.system_note,
                )
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => {
                    send_session_change_error(writer, ver, channel, epoch, corr, error).await
                }
            }
        }
        OP_REFRESH_POLICY => {
            let request: cortexkit_role_llm_runner::session_change::RefreshPolicyRequest =
                match serde_json::from_value(req.params.clone()) {
                    Ok(request) => request,
                    Err(error) => {
                        let field = broca_wire::failing_field::<
                            cortexkit_role_llm_runner::session_change::RefreshPolicyRequest,
                        >(&req.params, &error);
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            field,
                            &error.to_string(),
                        )
                        .await;
                    }
                };
            if let Some((field, message)) = serve_refresh::policy_refusal(&request.policy) {
                let field = Some(field.into());
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            // `send_id` is required: a missing or empty key is `invalid_params`, not a reuse.
            if request.send_id.is_empty() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("send_id".into()),
                    "send_id is required",
                )
                .await;
            }
            match handler
                .session_refresh_policy(
                    session,
                    bind.principal.as_ref(),
                    &bind.scope,
                    request.generation,
                    request.policy,
                    request.send_id,
                )
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => {
                    send_session_change_error(writer, ver, channel, epoch, corr, error).await
                }
            }
        }
        OP_FLUSH_PREFIX => {
            let request: cortexkit_role_llm_runner::session_change::FlushPrefixRequest =
                match serde_json::from_value(req.params.clone()) {
                    Ok(request) => request,
                    Err(error) => {
                        let field = broca_wire::failing_field::<
                            cortexkit_role_llm_runner::session_change::FlushPrefixRequest,
                        >(&req.params, &error);
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            field,
                            &error.to_string(),
                        )
                        .await;
                    }
                };
            if request.send_id.is_empty() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("send_id".into()),
                    "send_id is required",
                )
                .await;
            }
            match handler
                .session_flush_prefix(
                    session,
                    bind.principal.as_ref(),
                    &bind.scope,
                    request.send_id,
                )
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => {
                    send_session_change_error(writer, ver, channel, epoch, corr, error).await
                }
            }
        }
        OP_RELEASE => {
            if let Err((field, message)) = decode_params::<
                cortexkit_role_llm_runner::session_retention::ReleaseRequest,
            >(&req.params)
            {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            match handler
                .session_release(session, bind.principal.as_ref())
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => {
                    send_session_change_error(writer, ver, channel, epoch, corr, error).await
                }
            }
        }
        OP_WARM => {
            let params = match decode_params::<broca_wire::WarmParams>(&req.params) {
                Ok(params) => params,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await;
                }
            };
            match handler.warm(session, params.lineage_id).await {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_FORK_PREPARE => {
            type Request = cortexkit_role_llm_runner::fork_answer::ForkPrepareRequest;
            if let Err((field, message)) = decode_params::<Request>(&req.params) {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            let started = std::time::Instant::now();
            let result = handler.fork_prepare(session, bind.principal.as_ref()).await;
            let fork_id = result.as_ref().ok().map(|reply| reply.fork_id.as_str());
            log_fork_op("prepare", session, fork_id, None, None, &result, started);
            match result {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_fork_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_FORK_ASK => {
            type Request = cortexkit_role_llm_runner::fork_answer::ForkAskRequest;
            let request: Request = match serde_json::from_value(req.params.clone()) {
                Ok(request) => request,
                Err(error) => {
                    let field = broca_wire::failing_field::<Request>(&req.params, &error);
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &error.to_string(),
                    )
                    .await;
                }
            };
            // Broca declares `fork_effort` but not `fork_tools`: a tool list or framing is unknown.
            let undeclared =
                request.check_effort(&[cortexkit_role_llm_runner::capabilities::FORK_EFFORT]);
            if let Err(field) = undeclared.and_then(|()| request.check_tools::<&str>(&[])) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.to_owned()),
                    "this runner takes no tools or framing on session.fork_ask",
                )
                .await;
            }
            let started = std::time::Instant::now();
            let (fork_id, ask_id) = (request.fork_id.clone(), request.ask_id.clone());
            let result = handler
                .fork_ask(session, bind.principal.as_ref(), request)
                .await;
            // How the answer finished and its length, never its text.
            let answered = result.as_ref().ok().map(|reply| {
                format!(
                    "finish={:?} answer_chars={}",
                    reply.finish,
                    reply.answer.chars().count()
                )
            });
            log_fork_op(
                "ask",
                session,
                Some(&fork_id),
                Some(&ask_id),
                answered.as_deref(),
                &result,
                started,
            );
            match result {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_fork_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_FORK_END => {
            type Request = cortexkit_role_llm_runner::fork_answer::ForkEndRequest;
            let request: Request = match serde_json::from_value(req.params.clone()) {
                Ok(request) => request,
                Err(error) => {
                    let field = broca_wire::failing_field::<Request>(&req.params, &error);
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &error.to_string(),
                    )
                    .await;
                }
            };
            let started = std::time::Instant::now();
            let fork_id = request.fork_id.clone();
            let result = handler
                .fork_end(session, bind.principal.as_ref(), request)
                .await;
            log_fork_op("end", session, Some(&fork_id), None, None, &result, started);
            match result {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_fork_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REFS => {
            type Request = cortexkit_role_llm_runner::output_refs::ListRequest;
            let request: Request = match serde_json::from_value(req.params.clone()) {
                Ok(request) => request,
                Err(error) => {
                    let field = broca_wire::failing_field::<Request>(&req.params, &error);
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &error.to_string(),
                    )
                    .await;
                }
            };
            match handler.output_refs(session, &request).await {
                Ok(Ok(page)) => send_json_response(writer, ver, channel, epoch, corr, &page).await,
                Ok(Err(refusal)) => {
                    send_output_ref_refusal(writer, ver, channel, epoch, corr, refusal).await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REF_READ => {
            type Request = cortexkit_role_llm_runner::output_refs::ReadRequest;
            let request: Request = match serde_json::from_value(req.params.clone()) {
                Ok(request) => request,
                Err(error) => {
                    let field = broca_wire::failing_field::<Request>(&req.params, &error);
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &error.to_string(),
                    )
                    .await;
                }
            };
            match handler.output_ref_read(session, &request).await {
                Ok(Ok(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(Err(refusal)) => {
                    send_output_ref_refusal(writer, ver, channel, epoch, corr, refusal).await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_SUBSCRIBE => unreachable!("prepare_route_request handles session.subscribe"),
        // Broca declares no `send_promote`; the contract answers its op `unsupported` on such a
        // runner (a known op it doesn't serve), not `unknown_method`.
        cortexkit_role_llm_runner::ops::SESSION_PROMOTE => {
            send_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                cortexkit_role_llm_runner::errors::UNSUPPORTED,
                "this runner does not serve session.promote (send_promote undeclared)",
            )
            .await
        }
        other => {
            send_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "unknown_method",
                &format!("unknown method '{other}'"),
            )
            .await
        }
    }
}
