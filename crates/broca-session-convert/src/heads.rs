//! The `heads` check: for every log, `session.head` built from today's WAL against the head the
//! new storage serves for the converted session (`broca_module::store_head`), read from the open
//! log and again after sealing it at its newest cut.
//!
//! Today's side folds the log's records as today's head does when it reads a log by itself with
//! no live actor: `HeadProjection` for the model, plan choice, usage, Rich UI stamp and sender
//! rule, the newest page of one message for the latest run's state and the transcript's length,
//! the newest titled record, and the latest run's frozen render config for the input profile.
//! The store side reads the same facts from the converted session's folds and setup parts. Both
//! become a reply through the same `store_head::head_reply`, which the module's comparison tests
//! check against today's own `session.head` path.
//!
//! The comparison takes out what each storage keeps for itself, as the module's comparison tests
//! do: the incarnation, the history version and the subscribe cursor; `updated_at` is compared
//! as present or absent. The input profile is compared as its source, since only a running
//! module can quote the catalog, and the sender is compared for every principal the log's runs
//! recorded, every module its plan names as a sender, an anonymous caller and an unknown module.
//! A run the log leaves running is read as a live actor would see it, as today's head reads it.

use std::collections::BTreeMap;

use broca_core::head::HeadProjection;
use broca_protocol::{BindPrincipal, SendScope};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalRecord};
use futures::executor::block_on;
use serde::Serialize;

use broca_module::store_head::{HeadFacts, InputSource};

use super::{convert_log, each_log, triple, write_report, Args, Item};

#[derive(Default, Serialize)]
struct HeadsReport {
    logs: u64,
    /// Logs whose head equals today's, from the open log, and again after sealing.
    equal_open: u64,
    equal_sealed: u64,
    /// Logs sealed at the first message of one of their runs, which moves the transcript before
    /// it out of the open log too, and logs where no such cut was possible, so the seal moved
    /// only the ledger (the run, step and send records, as opposed to the messages).
    sealed_at_cut: u64,
    sealed_ledger_only: u64,
    /// Of today's heads, the latest run's state, so an equal count covers each.
    states: BTreeMap<String, u64>,
    /// Of today's heads, how many carry each member.
    with_title: u64,
    with_usage: u64,
    with_plan_model: u64,
    with_rich_ui: u64,
    with_run_error: u64,
    with_interrupt_reason: u64,
    mode3: u64,
    /// Of today's heads, the input profile's source.
    inputs: BTreeMap<String, u64>,
    /// Senders compared, and how many of them today grants anything.
    senders_compared: u64,
    senders_granted: u64,
    /// The first difference of each mismatching log, by where it is.
    mismatches: BTreeMap<String, u64>,
    /// A few mismatching logs per difference, to read by hand.
    examples: BTreeMap<String, Vec<String>>,
    failures: BTreeMap<String, u64>,
    /// Logs the store's head refuses `session_time_unknown` by design: no record of them carries
    /// a time (empty logs among them), where today's head serves the log file's own time.
    refused_time_unknown: u64,
    /// Logs written before lineage ids. Today's head serves them `attachments: "no"` whatever
    /// their runs froze, since it reads the frozen media profile only for a log with a lineage;
    /// the store, which keeps no lineage, applies the rule every other log is read by. Counted,
    /// and those whose ended run makes the store quote the catalog, where the two can differ.
    logs_without_lineage: u64,
    catalog_input_without_lineage: u64,
}

impl HeadsReport {
    fn note(&mut self, key: String, name: &str) {
        let key: String = key.chars().take(100).collect();
        *self.mismatches.entry(key.clone()).or_default() += 1;
        let examples = self.examples.entry(key).or_default();
        if examples.len() < 5 {
            examples.push(name.to_owned());
        }
    }

    fn fail(&mut self, what: &str, error: String, name: &str) {
        let key: String = format!("{what}: {error}").chars().take(100).collect();
        *self.failures.entry(key.clone()).or_default() += 1;
        let examples = self.examples.entry(key).or_default();
        if examples.len() < 5 {
            examples.push(name.to_owned());
        }
    }
}

/// A head, as compared: the reply with what each storage keeps for itself taken out, its input
/// source, and the sender each principal is told.
#[derive(Debug, PartialEq)]
struct Compared {
    reply: serde_json::Value,
    input: InputSource,
    senders: Vec<serde_json::Value>,
}

/// The reply as JSON without the incarnation, history version and subscribe cursor, with
/// `updated_at` as whether it's present and `mode3`, which isn't on the wire, under its name.
fn comparable(reply: &impl Serialize, mode3: bool) -> serde_json::Value {
    let mut value = serde_json::to_value(reply).expect("a head serializes");
    let object = value.as_object_mut().expect("a head is an object");
    for member in ["incarnation", "history_version", "head"] {
        object.remove(member);
    }
    if let Some(at) = object.get_mut("updated_at") {
        *at = serde_json::json!(!at.is_null());
    }
    object.insert("mode3".into(), serde_json::json!(mode3));
    value
}

/// Each caller whose sender is compared: an anonymous one, every principal the log's runs
/// recorded, every module a plan of the log names as a sender, and a module nothing names.
fn principals(framed: &[FramedRecord]) -> Vec<Option<BindPrincipal>> {
    let mut out = vec![None];
    let mut add = |principal: BindPrincipal| {
        if !out.contains(&Some(principal.clone())) {
            out.push(Some(principal));
        }
    };
    for frame in framed {
        if let WalRecord::RunStarted {
            principal,
            manifest,
            ..
        } = &frame.record
        {
            if let Some(principal) = principal {
                add(principal.clone());
            }
            for sender in manifest
                .as_deref()
                .map(broca_core::head::head_senders)
                .unwrap_or_default()
            {
                add(BindPrincipal::Reserved {
                    module_id: sender.module,
                });
            }
        }
    }
    add(BindPrincipal::Reserved {
        module_id: "a-module-nothing-names".into(),
    });
    out
}

/// The reason the latest end of `run_id` recorded for interrupting it, as today's head reads it
/// from the log (`recorded_interrupt_reason` in the module's serve code).
fn recorded_interrupt_reason(
    framed: &[FramedRecord],
    run_id: &broca_protocol::RunId,
) -> Option<String> {
    let mut current = None;
    let mut reason = None;
    for frame in framed {
        match &frame.record {
            WalRecord::RunStarted { run_id, .. } => current = Some(run_id),
            WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Interrupted,
                interrupt_reason,
                ..
            } if current == Some(run_id) => reason = interrupt_reason.clone(),
            _ => {}
        }
    }
    reason
}

/// The `media` member of the render config the latest run froze, or the one a resume refroze
/// after it, as today's head reads the frozen media profile.
fn frozen_media(framed: &[FramedRecord]) -> Option<serde_json::Value> {
    let mut media = None;
    for frame in framed {
        match &frame.record {
            WalRecord::RunStarted { config, .. } => {
                media = config.frozen_render_config.get("media").cloned();
            }
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => media = frozen_render_config.get("media").cloned(),
            _ => {}
        }
    }
    media
}

/// Today's head of the log `framed`, and the fold it was built from.
fn today_head(framed: &[FramedRecord]) -> Result<(Compared, HeadProjection), String> {
    let mut facts = HeadProjection::default();
    for frame in framed {
        facts.observe(&frame.record);
    }
    let page = broca_core::read_session_page(
        &broca_core::StorePageSnapshot::empty(),
        framed,
        None,
        Some(1),
    )
    .map_err(|e| format!("today's page: {e}"))?;
    let state = page.lineage_state;
    let interrupt_reason = match (&state.last_run_id, state.state) {
        (Some(run_id), broca_core::SessionReadStateKind::Interrupted) => {
            recorded_interrupt_reason(framed, run_id)
        }
        _ => None,
    };
    let input = broca_module::store_head::input_source::<String>(
        state.last_run_id.as_ref().map(|_| state.state.as_str()),
        || Ok(frozen_media(framed)),
    )?;
    let mode3 = facts
        .mode3
        .unwrap_or_else(|| broca_core::is_mode3_log(framed.iter().map(|frame| &frame.record)));
    let reply = broca_module::store_head::head_reply(HeadFacts {
        lineage_state: state,
        prompt_len: page.prompt_len,
        interrupt_reason,
        selection: facts.selection.clone(),
        run_model: facts.model.clone(),
        rich_ui: facts.last_run_rich_ui.clone(),
        usage: facts.usage.clone(),
        title: framed
            .iter()
            .rev()
            .find_map(|frame| frame.record.title())
            .map(str::to_owned),
        mode3,
        // Today's head always serves a time: the log file's own when no record carries one.
        updated_at: Some(0),
    });
    let compared = Compared {
        reply: comparable(&reply, reply.mode3),
        input,
        senders: Vec::new(),
    };
    Ok((compared, facts))
}

/// What the store's builder refuses a session with when no record of it carries a time.
const TIME_UNKNOWN: &str = "session_time_unknown";

/// The head the new storage serves for `session`, with the senders `principals` are told.
async fn store_head(
    session: &Session<&MemoryBackend>,
    parts: &broca_session_loop::parts::MemoryParts,
    principals: &[Option<BindPrincipal>],
) -> Result<Compared, String> {
    let built = broca_module::store_head::build(session, parts, true)
        .await
        .map_err(|e| match e {
            broca_module::store_head::StoreHeadError::NoTime => TIME_UNKNOWN.to_owned(),
            e => format!("{e:?}"),
        })?;
    let senders = principals
        .iter()
        .map(|principal| {
            serde_json::to_value(built.sender(principal.as_ref(), &SendScope::default()))
                .expect("a sender serializes")
        })
        .collect();
    Ok(Compared {
        reply: comparable(&built.reply, built.reply.mode3),
        input: built.input.clone(),
        senders,
    })
}

/// Records where `got` first differs from `expected`, under `when` (open or sealed); true when
/// they're equal.
fn compare(
    report: &mut HeadsReport,
    name: &str,
    when: &str,
    got: &Compared,
    expected: &Compared,
) -> bool {
    if got.reply != expected.reply {
        let path =
            broca_session_convert::inverse::first_difference(&got.reply, &expected.reply, "")
                .unwrap_or_else(|| "?".into());
        report.note(format!("{when}: reply {path}"), name);
        return false;
    }
    if got.input != expected.input {
        report.note(format!("{when}: input"), name);
        return false;
    }
    if got.senders != expected.senders {
        report.note(format!("{when}: sender"), name);
        return false;
    }
    true
}

/// The views a seal must carry: the compaction views the session names, and the record naming
/// the applied one, as the run loop carries them.
fn carried_views(
    records: &[Record],
    operations: &broca_session_store::fold::operations::Operations,
) -> Vec<Record> {
    let named = |version: u64, checksum: &str| {
        [
            operations.view_accepted.as_ref(),
            operations
                .view_applied
                .as_ref()
                .map(|applied| &applied.view),
        ]
        .into_iter()
        .flatten()
        .any(|slot| slot.version == version && slot.checksum == checksum)
    };
    records
        .iter()
        .filter(|record| match record {
            Record::View(view) => named(view.version, &view.checksum),
            Record::ViewApplied(applied) => operations.view_applied.as_ref().is_some_and(|slot| {
                slot.view.version == applied.version && slot.view.checksum == applied.checksum
            }),
            _ => false,
        })
        .cloned()
        .collect()
}

pub(super) fn run(args: &Args) {
    let mut report = HeadsReport::default();
    let deployment = DeploymentKey::new(1, &[0x5a; 32]);
    each_log(args, |item| {
        let (name, old) = match item {
            Item::Log {
                name, old: Ok(old), ..
            } => (name, old),
            Item::Log {
                name, old: Err(e), ..
            } => return report.fail("read", e, &name),
            Item::Note(..) => return,
        };
        report.logs += 1;
        let framed: Vec<FramedRecord> = old.iter().cloned().collect();
        let (mut today, facts) = match today_head(&framed) {
            Ok(today) => today,
            Err(e) => return report.fail("today", e, &name),
        };
        let principals = principals(&framed);
        today.senders = principals
            .iter()
            .map(|principal| {
                serde_json::to_value(facts.sender(
                    principal.as_ref(),
                    &SendScope::default(),
                    broca_module::store_head::DELIVERY_MODES,
                ))
                .expect("a sender serializes")
            })
            .collect();
        let reply = &today.reply;
        let state = reply["last_run_state"]["state"]
            .as_str()
            .unwrap_or("no run")
            .to_owned();
        *report.states.entry(state).or_default() += 1;
        report.with_title += u64::from(reply.get("title").is_some());
        report.with_usage += u64::from(reply.get("usage").is_some());
        report.with_plan_model += u64::from(facts.selection.is_some());
        report.with_rich_ui += u64::from(reply.get("last_run_rich_ui").is_some());
        report.with_run_error += u64::from(reply.get("last_run_error").is_some());
        report.with_interrupt_reason += u64::from(
            reply["last_run_state"]["state"] == "interrupted"
                && reply["last_run_state"].get("reason").is_some(),
        );
        report.mode3 += u64::from(reply["mode3"] == true);
        let input = match &today.input {
            InputSource::Unknown => "unknown",
            InputSource::Catalog => "catalog",
            InputSource::Frozen(None) => "frozen without media",
            InputSource::Frozen(Some(_)) => "frozen media",
        };
        *report.inputs.entry(input.into()).or_default() += 1;
        if old.lineage.is_none() {
            report.logs_without_lineage += 1;
            report.catalog_input_without_lineage += u64::from(today.input == InputSource::Catalog);
        }
        report.senders_compared += today.senders.len() as u64;
        report.senders_granted += today
            .senders
            .iter()
            .filter(|sender| !sender.is_null())
            .count() as u64;
        let converted = match convert_log(&old) {
            Ok(converted) => converted,
            Err(e) => return report.fail("convert", e.0, &name),
        };
        // `HEADS_SHOW=<log name>` prints that log's record kinds both ways, each with whether it
        // carries a time, and nothing of what the records say.
        if std::env::var("HEADS_SHOW").is_ok_and(|show| show == name) {
            let today: Vec<String> = framed
                .iter()
                .map(|frame| {
                    let kind = serde_json::to_value(&frame.record)
                        .ok()
                        .and_then(|value| value["kind"].as_str().map(str::to_owned))
                        .unwrap_or_default();
                    let timed = broca_core::record_time_ms(&frame.record).is_some();
                    format!("{kind}{}", if timed { "@" } else { "" })
                })
                .collect();
            let store: Vec<String> = converted
                .records
                .iter()
                .map(|record| {
                    let timed = record.activity_ms().is_some();
                    format!("{}{}", record.name(), if timed { "@" } else { "" })
                })
                .collect();
            eprintln!("today (@ = timed): {today:?}\nstore (@ = timed): {store:?}");
        }
        let parts = broca_session_loop::parts::MemoryParts {
            parts: converted.parts.clone(),
        };
        let served = block_on(async {
            let backend = MemoryBackend::default();
            let addr = SessionAddr(name.clone());
            let triple = triple(&name);
            let mut session =
                Session::create(&backend, addr.clone(), triple.clone(), &deployment, false)
                    .await
                    .map_err(|e| format!("create: {e:?}"))?;
            let mut run_inputs = Vec::new();
            for record in &converted.records {
                let seq = session
                    .append(std::slice::from_ref(record), false)
                    .await
                    .map_err(|e| format!("append {}: {e:?}", record.name()))?;
                if matches!(record, Record::Input(input) if input.placement == broca_session_store::record::Placement::RunInput)
                {
                    run_inputs.push(seq);
                }
            }
            let open = store_head(&session, &parts, &principals)
                .await
                .map_err(|e| format!("open: {e}"))?;
            let carried = carried_views(&converted.records, &session.operations);
            let carry = broca_session_store::session::Carry {
                records: &carried,
                window_repetition: None,
            };
            let mut at_cut = false;
            for cut in run_inputs.iter().rev() {
                match session.seal_carrying(Some(*cut), &carry, 0).await {
                    Ok(_) => {
                        at_cut = true;
                        break;
                    }
                    Err(broca_session_store::session::SessionError::BadCut(_)) => continue,
                    Err(e) => return Err(format!("seal: {e:?}")),
                }
            }
            if !at_cut {
                session
                    .seal_carrying(None, &carry, 0)
                    .await
                    .map_err(|e| format!("seal: {e:?}"))?;
            }
            session.close().await.map_err(|e| format!("close: {e:?}"))?;
            let view = Session::view(&backend, addr, &triple, &deployment)
                .await
                .map_err(|e| format!("view: {e:?}"))?;
            let sealed = store_head(&view, &parts, &principals)
                .await
                .map_err(|e| format!("sealed: {e}"))?;
            Ok::<_, String>((open, at_cut, sealed))
        });
        let (open, at_cut, sealed) = match served {
            Ok(served) => served,
            Err(e) if e.ends_with(TIME_UNKNOWN) => {
                report.refused_time_unknown += 1;
                let examples = report.examples.entry(TIME_UNKNOWN.into()).or_default();
                if examples.len() < 5 {
                    examples.push(name.clone());
                }
                return;
            }
            Err(e) => return report.fail("store", e, &name),
        };
        if at_cut {
            report.sealed_at_cut += 1;
        } else {
            report.sealed_ledger_only += 1;
        }
        let equal_open = compare(&mut report, &name, "open", &open, &today);
        report.equal_open += u64::from(equal_open);
        let equal_sealed = compare(&mut report, &name, "sealed", &sealed, &today);
        report.equal_sealed += u64::from(equal_sealed);
    });
    write_report(&report, args.out_file.as_deref());
}
