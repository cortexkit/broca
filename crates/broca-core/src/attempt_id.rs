//! Billing identities are independent of log position and retry counters, including after restore.

use broca_records::record::AttemptId;

/// Mint all 128 bits from the OS random source before building a reservation.
/// Entropy failures propagate to the caller: there is no counter or timestamp fallback.
pub fn mint_attempt_id() -> Result<AttemptId, getrandom::Error> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes)?;
    Ok(AttemptId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    #[test]
    fn attempt_ids_do_not_repeat_across_restore_and_send_id_retry() {
        let original_id = mint_attempt_id().unwrap();
        let mut branch = vec![original_id.clone()];
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
                branch.push(id);
            }
            let snapshot = serde_json::to_vec(&restored_prefix).unwrap();
            branch = serde_json::from_slice(&snapshot).unwrap();
        }
        assert_eq!(ids.len(), 193);
    }
}
