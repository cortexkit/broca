//! Session-local send-key lookup built from frames already held by a projection.
use broca_protocol::BindPrincipal;
use broca_wal::{FramedRecord, WalRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NoticeSendKeys {
    through: u64,
    complete: bool,
    ordered_senders: HashMap<String, Option<BindPrincipal>>,
    // Keep the small full key beside its hash only to verify hash hits, never as message content.
    client_keys: HashMap<String, (String, String)>,
    submissions: HashMap<String, (Option<String>, Option<String>)>,
    #[serde(default)]
    collision: bool,
}

fn sender(principal: Option<&BindPrincipal>) -> String {
    match principal {
        Some(BindPrincipal::Reserved { module_id }) => module_id.clone(),
        _ => "broca".into(),
    }
}

impl NoticeSendKeys {
    /// Whether the index has observed every record through its watermark.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    fn hash(sender: &str, send_id: &str) -> String {
        format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(sender, send_id)).unwrap())
        )
    }

    fn insert_key(&mut self, sender: String, id: String) {
        let key = (sender, id);
        let hash = Self::hash(&key.0, &key.1);
        if let Some(previous) = self.client_keys.get(&hash) {
            self.collision |= previous != &key;
        } else {
            self.client_keys.insert(hash, key);
        }
    }

    pub fn origin(&self, submission: &str) -> Option<(String, Option<String>)> {
        let (id, sender) = self.submissions.get(submission)?;
        Some((
            id.clone().unwrap_or_else(|| submission.to_owned()),
            sender.clone(),
        ))
    }

    pub fn observe(&mut self, frames: &[FramedRecord]) {
        for frame in frames {
            if frame.seq <= self.through {
                continue;
            }
            if self.through == 0 {
                self.complete = frame.seq == 1;
            } else if frame.seq != self.through + 1 {
                self.complete = false;
            }
            self.through = frame.seq;
            match &frame.record {
                WalRecord::DeliveryOrdered {
                    submission_id,
                    sent_by,
                    ..
                } => {
                    self.ordered_senders
                        .insert(submission_id.0.clone(), sent_by.clone());
                }
                WalRecord::Queued {
                    submission_id,
                    send_id,
                    sent_by,
                    principal,
                    ..
                } => {
                    let principal = sent_by
                        .as_ref()
                        .or_else(|| {
                            self.ordered_senders
                                .get(&submission_id.0)
                                .and_then(Option::as_ref)
                        })
                        .or(principal.as_ref());
                    let module = match principal {
                        Some(BindPrincipal::Reserved { module_id }) => Some(module_id.clone()),
                        _ => None,
                    };
                    self.submissions
                        .insert(submission_id.0.clone(), (send_id.clone(), module));
                    if let Some(id) = send_id {
                        self.insert_key(sender(principal), id.clone());
                    }
                }
                WalRecord::RunStarted {
                    send_id: Some(id),
                    sent_by,
                    principal,
                    submission_id,
                    ..
                } => {
                    let principal = sent_by.as_ref().or(principal.as_ref());
                    self.insert_key(sender(principal), id.clone());
                    if let Some(submission) = submission_id {
                        self.submissions.insert(
                            submission.0.clone(),
                            (
                                Some(id.clone()),
                                match principal {
                                    Some(BindPrincipal::Reserved { module_id }) => {
                                        Some(module_id.clone())
                                    }
                                    _ => None,
                                },
                            ),
                        );
                    }
                }
                _ => {}
            }
        }
    }

    pub fn allows(&self, submission_id: &str, sender: &str, send_id: &str) -> Result<bool, String> {
        if self.collision {
            return Err("notice_send_key_hash_collision: send-key index is ambiguous".into());
        }
        if self
            .submissions
            .get(submission_id)
            .is_some_and(|(id, _)| id.is_some())
        {
            return Ok(true);
        }
        if send_id != submission_id {
            return Ok(true);
        }
        if !self.complete {
            return Err("notice_send_keys_unavailable: complete send-key index required".into());
        }
        let key = (sender.to_owned(), send_id.to_owned());
        let found = self.client_keys.get(&Self::hash(sender, send_id));
        if found.is_some_and(|found| found != &key) {
            return Err("notice_send_key_hash_collision: full send key does not match".into());
        }
        Ok(found.is_none())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn queued(seq: u64, submission: &str, client: Option<&str>, module: &str) -> FramedRecord {
        FramedRecord { seq, fence:1, record:serde_json::from_value(json!({"type":"queued","submission_id":submission,"send_id":client,"sent_by":{"kind":"reserved","module_id":module},"request":{"model":{"provider_module_id":"fake","model_id":"fake"},"input":[]}})).unwrap() }
    }

    #[test]
    fn minted_collision_is_suppressed_but_the_client_key_and_other_senders_are_not() {
        let frames = [
            queued(1, "keyed", Some("sub-unkeyed"), "m"),
            queued(2, "sub-unkeyed", None, "m"),
        ];
        let mut keys = NoticeSendKeys::default();
        keys.observe(&frames);
        assert!(!keys.allows("sub-unkeyed", "m", "sub-unkeyed").unwrap());
        assert!(keys.allows("keyed", "m", "sub-unkeyed").unwrap());
        let mut other = NoticeSendKeys::default();
        other.observe(&[
            queued(1, "keyed", Some("sub-unkeyed"), "other"),
            frames[1].clone(),
        ]);
        assert!(other.allows("sub-unkeyed", "m", "sub-unkeyed").unwrap());
    }

    #[test]
    fn a_key_outside_the_held_window_never_becomes_a_false_noncollision() {
        let frames = [
            queued(1, "keyed", Some("sub-unkeyed"), "m"),
            queued(2, "sub-unkeyed", None, "m"),
        ];
        let mut window = NoticeSendKeys::default();
        window.observe(&frames[1..]);
        assert!(window
            .allows("sub-unkeyed", "m", "sub-unkeyed")
            .unwrap_err()
            .starts_with("notice_send_keys_unavailable:"));
        let mut complete = NoticeSendKeys::default();
        complete.observe(&[queued(1, "sub-unkeyed", None, "m")]);
        assert!(complete.allows("sub-unkeyed", "m", "sub-unkeyed").unwrap());
    }

    #[test]
    fn a_hash_hit_requires_the_full_send_key_to_match() {
        let mut keys = NoticeSendKeys::default();
        keys.observe(&[queued(1, "unkeyed", None, "m")]);
        keys.client_keys.insert(
            NoticeSendKeys::hash("m", "unkeyed"),
            ("other".into(), "different".into()),
        );
        assert!(keys
            .allows("unkeyed", "m", "unkeyed")
            .unwrap_err()
            .starts_with("notice_send_key_hash_collision:"));
    }
}
