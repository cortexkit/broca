//! Session labels are fixed by the first admitted send, including an absent label.

pub fn admit(
    requested: Option<&str>,
    frozen: Option<Option<&str>>,
) -> Result<Option<String>, String> {
    if let Some(class) = requested {
        broca_protocol::request::validate_session_class(class).map_err(str::to_owned)?;
        if frozen.is_some_and(|current| current != Some(class)) {
            return Err("session_class differs from the session's first send".into());
        }
    }
    Ok(frozen.unwrap_or(requested).map(str::to_owned))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_freezes_at_first_send_including_absence() {
        assert_eq!(admit(Some("head"), None).unwrap().as_deref(), Some("head"));
        assert_eq!(
            admit(None, Some(Some("head"))).unwrap().as_deref(),
            Some("head")
        );
        assert!(admit(Some("head"), Some(Some("head"))).is_ok());
        assert!(admit(Some("child"), Some(Some("head")))
            .unwrap_err()
            .contains("session_class"));
        assert!(admit(Some("head"), Some(None))
            .unwrap_err()
            .contains("session_class"));
        assert_eq!(admit(None, Some(None)).unwrap(), None);
    }
}
