//! Billing identities are independent of log position and retry counters, including after restore.

use broca_wal::record::{AttemptId, CorrectionId};

/// Mint all 128 bits from the OS random source before building a reservation.
/// Entropy failures propagate to the caller: there is no counter or timestamp fallback.
pub fn mint_attempt_id() -> Result<AttemptId, getrandom::Error> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes)?;
    Ok(AttemptId::from_bytes(bytes))
}

/// Mint an independent identity for one complete replacement of an attempt's evidence.
/// Multiple corrections of the same attempt must not share their deduplication key.
pub fn mint_correction_id() -> Result<CorrectionId, getrandom::Error> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes)?;
    Ok(CorrectionId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{RunId, StepId};
    use broca_wal::record::{
        AttemptCredential, AttemptEnvelope, AttemptReservation, AttemptRun, AttemptSession,
        WalRecord,
    };
    use std::collections::BTreeSet;

    #[test]
    fn correction_ids_are_distinct_for_repeated_corrections_of_one_attempt() {
        let attempt = mint_attempt_id().unwrap();
        let mut corrections = BTreeSet::new();
        for _ in 0..64 {
            let correction = mint_correction_id().unwrap();
            assert_ne!(correction.as_str(), attempt.as_str());
            assert_eq!(correction.as_str().len(), 32);
            assert!(correction
                .as_str()
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
            assert!(corrections.insert(correction));
        }
        assert_eq!(corrections.len(), 64);
    }

    fn reservation() -> AttemptReservation {
        AttemptReservation {
            run_id: RunId("repeated-send-id".into()),
            step_id: StepId(1),
            attempt: 0,
            send_index: 1,
            warm: false,
            reserved_at_ms: 42,
            provider: "provider".into(),
            endpoint_host: "example.com".into(),
            model_sent: "model".into(),
            credential: AttemptCredential {
                credential_ref: None,
                account_ref: None,
                auth_selection: None,
                auth_method: None,
                provider_ids: None,
                plan_tier: None,
                plan_tier_source: None,
            },
            session: serde_json::from_value::<AttemptSession>(serde_json::json!({
                "project_root":"/project", "harness":"runner", "session":"s", "session_ref":"ref"
            }))
            .unwrap(),
            run: AttemptRun::default(),
            request_body_bytes: 10,
            compressed: false,
            input_tokens_estimate: None,
        }
    }

    #[test]
    fn recovery_terminal_does_not_change_output_reservation_measurements() {
        let record = WalRecord::AttemptFinished {
            attempt_id: mint_attempt_id().unwrap(),
            reservation: Box::new(reservation()),
            envelope: Box::new(AttemptEnvelope::default()),
        };
        let model = serde_json::from_value(serde_json::json!({
            "provider_module_id":"provider", "model_id":"model"
        }))
        .unwrap();
        let mut history = crate::output_reservation::OutputReservationHistory::default();
        history.start_run(&model);
        let before = history.clone();
        history.fold(&record);
        assert_eq!(history, before);
        assert_eq!(history.estimate(1000, &model), None);
        history.note_attempt(500);
        let before = history.clone();
        history.fold(&record);
        assert_eq!(history, before);
    }

    #[test]
    fn attempt_ids_do_not_repeat_across_restore_and_send_id_retry() {
        let reservation = reservation();
        let original_id = mint_attempt_id().unwrap();
        let mut branch = vec![WalRecord::AttemptReserved {
            attempt_id: original_id.clone(),
            reservation: Box::new(reservation.clone()),
        }];
        let mut ids = BTreeSet::from([original_id]);
        // Each branch reuses the same run, step, retry ordinal and timestamp. Restoring the
        // earlier prefix must not roll the random identity source back along with the log.
        for _ in 0..3 {
            let restored_prefix = branch.clone();
            for _ in 0..64 {
                let id = mint_attempt_id().unwrap();
                assert_eq!(id.as_str().len(), 32);
                assert!(id
                    .as_str()
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
                assert!(
                    ids.insert(id.clone()),
                    "duplicate reservation across a restored branch"
                );
                branch.push(WalRecord::AttemptReserved {
                    attempt_id: id,
                    reservation: Box::new(reservation.clone()),
                });
            }
            let snapshot = serde_json::to_vec(&restored_prefix).unwrap();
            branch = serde_json::from_slice(&snapshot).unwrap();
        }
        assert_eq!(ids.len(), 193);
    }
}
