//! Send state notices (`event.broca.send_state.v1.{sender}`): one per change of a keyed send's
//! state, published on the account's event stream for the module that sent it.
//!
//! A notice says only that a send changed. A reader re-reads `session.send_status` for the
//! send's state, so a send's details, and any provider error text it carries, never sit in a
//! stored stream. `state` is the state the change moved the send to, as a hint for logs; a
//! reader decides nothing from it.

use cortexkit_bus_naming::{AccountNames, NamingError};
use serde::Serialize;

/// The limit is exclusive: a notice this size or larger is refused, not shortened.
pub const MAX_NOTICE_BYTES: usize = 512;

/// The event name and version of a send state notice.
pub const SEND_STATE_EVENT: &str = "send_state";
pub const SEND_STATE_VERSION: u32 = 1;

/// One change of one keyed send's state.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SendStateNotice {
    pub v: u8,
    /// The session the send went to, as run events name it.
    pub session_ref: String,
    /// The caller's own key for the send, as it reads it back through `session.send_status`.
    pub send_id: String,
    /// The seq of the record that changed the send: increasing within the session's log, so
    /// the changes of one send arrive in order.
    pub seq: u64,
    /// The state the change moved the send to: `pending`, `active`, `paused`, `finished`,
    /// `superseded`, `expired` or `retracted`.
    pub state: &'static str,
}

impl SendStateNotice {
    /// The subject the notice is published on: the send state event of broca, ending with the
    /// sending module's id, so each sender's durable reads only its own sends.
    pub fn subject(&self, account: &AccountNames, sender: &str) -> Result<String, NamingError> {
        let event = account.event_subject("broca", SEND_STATE_EVENT, SEND_STATE_VERSION)?;
        // `event_publish_grant` checks `sender` is a well-formed module id, the token the bus
        // contract allows after an event's version to name the module meant to act on it.
        account.event_publish_grant(sender)?;
        Ok(format!("{event}.{sender}"))
    }

    /// The bus message id: unique per change across every session, so the stream's duplicate
    /// window drops only a true resend. A caller's send ids are unique only within a session.
    pub fn message_id(&self) -> String {
        format!("{}:{}:{}", self.session_ref, self.send_id, self.seq)
    }

    /// The notice's canonical bytes (RFC 8785 JSON canonicalization, as run events use),
    /// refused at [`MAX_NOTICE_BYTES`] or more.
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let bytes = serde_jcs::to_vec(self).map_err(|error| error.to_string())?;
        if bytes.len() >= MAX_NOTICE_BYTES {
            return Err(format!(
                "a send state notice of {} bytes is over the {MAX_NOTICE_BYTES}-byte limit",
                bytes.len()
            ));
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notice() -> SendStateNotice {
        SendStateNotice {
            v: 1,
            session_ref: "ses_ref".into(),
            send_id: "send-1".into(),
            seq: 7,
            state: "pending",
        }
    }

    #[test]
    fn a_notice_names_its_sender_after_the_version() {
        let account = AccountNames::derive("box_test").unwrap();
        assert_eq!(
            notice().subject(&account, "aft").unwrap(),
            "ck.box_test.event.broca.send_state.v1.aft"
        );
        assert!(notice().subject(&account, "not a module").is_err());
    }

    #[test]
    fn the_body_is_canonical_and_the_id_names_the_session() {
        assert_eq!(
            String::from_utf8(notice().to_bytes().unwrap()).unwrap(),
            r#"{"send_id":"send-1","seq":7,"session_ref":"ses_ref","state":"pending","v":1}"#
        );
        assert_eq!(notice().message_id(), "ses_ref:send-1:7");
        let long = SendStateNotice {
            send_id: "x".repeat(600),
            ..notice()
        };
        assert!(long.to_bytes().is_err());
    }
}
