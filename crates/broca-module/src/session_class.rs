//! A session's label and its WAL placement are fixed by its first admitted send.
//!
//! A labelled session refuses a later, different label. An unlabelled session accepts a later
//! label but ignores it: its placement was decided when it was created and cannot move. This
//! matters because sessions created before broca supported labels are unlabelled, and the
//! caller sends the label on every send, including follow-ups to those sessions.

/// What admission decided about a send's label.
#[derive(Debug, PartialEq, Eq)]
pub enum Admitted {
    /// The session's label after this send (absent for an unlabelled session).
    Class(Option<String>),
    /// The send carried a label, but the session was created unlabelled, so the label is ignored.
    IgnoredOnUnlabelled(String),
}

pub fn admit(requested: Option<&str>, frozen: Option<Option<&str>>) -> Result<Admitted, String> {
    if let Some(class) = requested {
        broca_protocol::request::validate_session_class(class).map_err(str::to_owned)?;
        match frozen {
            Some(None) => return Ok(Admitted::IgnoredOnUnlabelled(class.to_owned())),
            Some(Some(current)) if current != class => {
                return Err("session_class differs from the session's first send".into());
            }
            _ => {}
        }
    }
    Ok(Admitted::Class(
        frozen.unwrap_or(requested).map(str::to_owned),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(value: &str) -> Admitted {
        Admitted::Class(Some(value.to_owned()))
    }

    #[test]
    fn class_freezes_at_first_send() {
        assert_eq!(admit(Some("head"), None).unwrap(), class("head"));
        assert_eq!(admit(None, Some(Some("head"))).unwrap(), class("head"));
        // The caller repeats the label on every send; the same label stays accepted.
        assert_eq!(
            admit(Some("head"), Some(Some("head"))).unwrap(),
            class("head")
        );
        assert!(admit(Some("child"), Some(Some("head")))
            .unwrap_err()
            .contains("session_class"));
        assert_eq!(admit(None, Some(None)).unwrap(), Admitted::Class(None));
    }

    #[test]
    fn a_label_on_an_unlabelled_session_is_accepted_and_ignored() {
        assert_eq!(
            admit(Some("head"), Some(None)).unwrap(),
            Admitted::IgnoredOnUnlabelled("head".into())
        );
        // Validation still applies to an ignored label.
        assert!(admit(Some(""), Some(None)).is_err());
    }
}
