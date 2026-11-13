//! Reader-facing waits for `broca_session_store` sessions. Every surface folds the same run
//! records here; legacy `broca_wal` sessions deliberately keep their existing bodies.

use std::collections::BTreeMap;

use broca_protocol::{PauseReason, RunFinishReason};
use broca_session_store::record::Record;
use cortexkit_role_llm_runner::{
    awaits::{AwaitAction, AwaitParty, AwaitPerson, RunAwaits},
    run::RunReason,
};

#[derive(Clone, Default)]
pub struct Labels(pub(crate) Option<std::sync::Arc<dyn crate::executor::RunExecutor>>);

impl std::fmt::Debug for Labels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Labels(cache-only)")
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunWait {
    labels: Labels,
    selection: Option<broca_wal::AuthSelection>,
    message: Option<cortexkit_role_llm_runner::awaits::AwaitMessage>,
    label: Option<String>,
    pause: Option<(PauseReason, Option<u64>, Option<u64>)>,
    held: BTreeMap<(String, String), u64>,
    end: Option<(
        RunFinishReason,
        Option<u64>,
        Option<cortexkit_role_llm_runner::run::RunError>,
    )>,
}

pub(crate) struct Snapshot {
    pub state: &'static str,
    pub reason: Option<String>,
    pub awaits: Option<RunAwaits>,
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl RunWait {
    pub(crate) fn set_labels(&mut self, labels: Labels) {
        self.labels = labels;
    }
    pub(crate) fn resumed(&mut self) {
        self.pause = None;
    }
    pub(crate) fn record(&mut self, record: &Record) -> Result<(), String> {
        match record {
            Record::RunStart(start) => {
                let labels = self.labels.clone();
                *self = Self::default();
                self.labels = labels;
                self.selection = start
                    .auth_selection
                    .as_ref()
                    .and_then(|selection| serde_json::from_value(selection.clone()).ok());
                self.label = start.auth_selection.as_ref().and_then(account_label);
            }
            Record::AuthRefrozen(auth) => {
                self.selection = serde_json::from_value(auth.auth_selection.clone()).ok();
                self.label = account_label(&auth.auth_selection);
            }
            Record::StepStart(_) => self.pause = None,
            Record::Pause(pause) => {
                self.message = pause
                    .error
                    .as_ref()
                    .and_then(|error| {
                        serde_json::from_value::<broca_protocol::ProviderError>(error.clone()).ok()
                    })
                    .as_ref()
                    .and_then(broca_protocol::provider::await_message);
                self.pause = Some((
                    pause.reason,
                    pause.ts_ms,
                    pause
                        .account_limit
                        .as_ref()
                        .and_then(|limit| limit.get("resume_not_before_ms"))
                        .and_then(serde_json::Value::as_u64),
                ));
            }
            Record::ToolHeld(held) => {
                self.held.insert(
                    (held.batch_id.clone(), held.tool_call_id.0.clone()),
                    held.held_at_ms,
                );
            }
            Record::ToolOutcome(outcome) => {
                self.held
                    .remove(&(outcome.batch_id.clone(), outcome.tool_call_id.0.clone()));
            }
            Record::RunEnd(end) => {
                let error = end
                    .error
                    .as_ref()
                    .map(|error| {
                        serde_json::from_value::<broca_protocol::ProviderError>(error.clone())
                    })
                    .transpose()
                    .map_err(|error| format!("a run end's error: {error}"))?;
                self.message = error
                    .as_ref()
                    .and_then(broca_protocol::provider::await_message);
                let error = match error {
                    Some(error) => Some(
                        serde_json::to_value(broca_protocol::caller::SharedProviderError::from(
                            error,
                        ))
                        .and_then(serde_json::from_value)
                        .map_err(|error| format!("a run end's error: {error}"))?,
                    ),
                    None => None,
                };
                self.end = Some((end.reason, end.ts_ms, error));
                self.pause = None;
                self.held.clear();
            }
            Record::Retraction(retraction) if retraction.failure.is_some() => {
                let labels = self.labels.clone();
                *self = Self::default();
                self.labels = labels;
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn snapshot(
        &self,
        now: u64,
        retry: Option<&RunAwaits>,
        deadline_ms: Option<u64>,
        account_label: Option<&str>,
    ) -> Snapshot {
        if let Some((reason, ended_at, error)) = &self.end {
            let awaits = if *reason == RunFinishReason::Error {
                ended_at.zip(error.as_ref()).map(|(time, error)| {
                    let mut wait = cortexkit_role_llm_runner::errors::ending_awaits(error, time);
                    wait.message = self.message.clone();
                    wait.next_ms = wait.next_ms.filter(|next| *next > now);
                    wait
                })
            } else {
                None
            };
            return Snapshot {
                state: "terminal",
                reason: None,
                awaits,
            };
        }
        if let Some((reason, since, next)) = self
            .pause
            .filter(|(reason, _, _)| *reason != PauseReason::ToolHeld)
        {
            let awaits = since.map(|since| {
                let party = if reason == PauseReason::AuthRequired {
                    AwaitParty::Person
                } else {
                    AwaitParty::Runner
                };
                let mut wait = RunAwaits::new(party, since);
                wait.message = self.message.clone();
                wait.reason = Some(RunReason::from(reason.as_str().to_owned()));
                if reason == PauseReason::AccountLimit {
                    wait.next_ms = next.filter(|next| *next > now);
                }
                if reason == PauseReason::AuthRequired {
                    let mut person = AwaitPerson::new(AwaitAction::SignIn);
                    let email = self
                        .labels
                        .0
                        .as_ref()
                        .zip(self.selection.as_ref())
                        .and_then(|(executor, selection)| executor.cached_account_email(selection));
                    person.on = account_label
                        .or(self.label.as_deref())
                        .or(email.as_deref())
                        .map(str::trim)
                        .filter(|label| (1..=120).contains(&label.chars().count()))
                        .map(str::to_owned);
                    wait.person = Some(person);
                }
                wait
            });
            return Snapshot {
                state: "paused",
                reason: Some(reason.as_str().into()),
                awaits,
            };
        }
        if let Some(since) = self.held.values().min().copied() {
            let mut wait = RunAwaits::new(AwaitParty::Runner, since);
            wait.reason = Some(RunReason::from("tool_held".to_owned()));
            wait.deadline_ms = deadline_ms;
            return Snapshot {
                state: "active",
                reason: Some("tool_held".into()),
                awaits: Some(wait),
            };
        }
        let awaits = broca_protocol::provider::visible_retry_awaits(retry, now);
        let reason = awaits
            .as_ref()
            .and_then(|wait| wait.reason.clone())
            .map(String::from);
        Snapshot {
            state: "active",
            reason,
            awaits,
        }
    }
}

fn account_label(selection: &serde_json::Value) -> Option<String> {
    let id = selection.get("credential_id")?.as_str()?;
    let mut parts = id.splitn(3, ':');
    let method = parts.next()?;
    parts.next()?;
    if !matches!(method, "oauth" | "chatgpt" | "apikey" | "antigravity") {
        return None;
    }
    let label = parts.next()?.trim();
    (1..=120)
        .contains(&label.chars().count())
        .then(|| label.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_awaits_past_floor_is_omitted_without_changing_the_recorded_floor() {
        let wait = RunWait {
            pause: Some((PauseReason::AccountLimit, Some(1000), Some(2000))),
            ..Default::default()
        };
        assert_eq!(
            wait.snapshot(1500, None, None, None)
                .awaits
                .unwrap()
                .next_ms,
            Some(2000)
        );
        assert_eq!(
            wait.snapshot(2001, None, None, None)
                .awaits
                .unwrap()
                .next_ms,
            None
        );
        assert_eq!(wait.pause.unwrap().2, Some(2000));
    }

    #[test]
    fn run_awaits_held_deadline_is_the_top_level_deadline() {
        let mut wait = RunWait::default();
        wait.held.insert(("batch".into(), "call".into()), 1000);
        let snapshot = wait.snapshot(2000, None, Some(3000), None);
        assert_eq!(snapshot.state, "active");
        assert_eq!(snapshot.awaits.unwrap().deadline_ms, Some(3000));
    }

    #[test]
    fn run_awaits_short_retry_is_hidden_until_one_second_and_never_shows_a_past_next() {
        let mut retry = RunAwaits::new(AwaitParty::Runner, 1000);
        retry.next_ms = Some(1500);
        let wait = RunWait::default();
        assert!(wait
            .snapshot(1200, Some(&retry), None, None)
            .awaits
            .is_none());
        assert_eq!(
            wait.snapshot(2001, Some(&retry), None, None)
                .awaits
                .unwrap()
                .next_ms,
            None
        );
        retry.next_ms = Some(2500);
        assert!(wait
            .snapshot(1000, Some(&retry), None, None)
            .awaits
            .is_some());
        assert!(wait
            .snapshot(1800, Some(&retry), None, None)
            .awaits
            .is_some());
    }

    #[test]
    fn run_awaits_account_labels_never_fall_back_to_a_vault_id() {
        assert_eq!(
            account_label(&serde_json::json!({"credential_id":"oauth:anthropic:ufuk2"})).as_deref(),
            Some("ufuk2")
        );
        assert_eq!(
            account_label(&serde_json::json!({"credential_id":"oauth:anthropic"})),
            None
        );
        assert_eq!(
            account_label(
                &serde_json::json!({"credential_id":"opaque:provider:uuid", "auth_method":"oauth"})
            ),
            None
        );
    }
}
