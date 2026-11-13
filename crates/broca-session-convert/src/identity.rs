//! Resolve a re-keyed legacy log without treating its historical run identity as its address.

use broca_core::SessionRefResolver;
use broca_protocol::BindIdentity;

#[derive(Debug)]
pub enum IdentityError {
    Unresolved(String),
    Inventory(String),
}

impl IdentityError {
    pub fn class(&self) -> &'static str {
        match self {
            Self::Unresolved(_) => "identity_unresolved",
            Self::Inventory(_) => "io_failed",
        }
    }
}

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unresolved(detail) | Self::Inventory(detail) => f.write_str(detail),
        }
    }
}

/// Keep matching identities unchanged. A re-key requires one distinct identity in a complete
/// authoritative inventory, with both its recorded address and its hash matching the source.
pub fn effective_identity(
    recorded: &BindIdentity,
    address: &str,
    inventory: Option<&dyn SessionRefResolver>,
) -> Result<BindIdentity, IdentityError> {
    if broca_core::session_addr(recorded) == address {
        return Ok(recorded.clone());
    }
    let unresolved =
        |reason: &str| IdentityError::Unresolved(format!("source {address}: {reason}"));
    let inventory = inventory.ok_or_else(|| unresolved("no inventory row for session"))?;
    // Bound decoding without mistaking a full page for proof of uniqueness.
    let found = inventory
        .named_refs(&recorded.session, 101)
        .map_err(IdentityError::Inventory)?;
    if !found.complete {
        return Err(unresolved(
            "inventory is incomplete; cannot establish a unique identity",
        ));
    }
    if found.sessions.len() >= 101 {
        return Err(unresolved("several inventory rows; candidate page is full"));
    }
    let mut candidates: Vec<_> = found
        .sessions
        .into_values()
        .filter(|(stored, identity)| {
            stored == address && broca_core::session_addr(identity) == address
        })
        .collect();
    candidates.sort_by(|a, b| {
        (&a.1.project_root, &a.1.harness, &a.1.session, &a.0).cmp(&(
            &b.1.project_root,
            &b.1.harness,
            &b.1.session,
            &b.0,
        ))
    });
    candidates.dedup();
    let (inventoried_address, identity) = match candidates.as_slice() {
        [] => return Err(unresolved("no inventory row hashes to the source address")),
        [candidate] => candidate,
        _ => {
            return Err(unresolved(
                "several inventory identities or addresses for session",
            ))
        }
    };
    debug_assert_eq!(inventoried_address, address);
    Ok(identity.clone())
}

/// Prove an otherwise unservable live filename's identity from independently inventoried roots.
/// Only runner/broca harnesses and names in its records or named inventory rows are candidates.
/// Until publication, the caller must hold the selected session's lock excluding live writers
/// and must recheck that no new-storage head exists. Returned inventory identities record
/// provenance, not serving identities.
pub fn old_root_identity(
    recorded: &[BindIdentity],
    address: &str,
    roots: (&[String], bool),
    inventory: &dyn SessionRefResolver,
    occupied: bool,
) -> Result<(BindIdentity, Vec<BindIdentity>), IdentityError> {
    let unresolved =
        |reason: &str| IdentityError::Unresolved(format!("source {address}: {reason}"));
    if occupied {
        return Err(unresolved(
            "a new-storage head already occupies the source address",
        ));
    }
    if !roots.1 {
        return Err(unresolved("inventory roots are incomplete"));
    }
    let mut names: std::collections::BTreeSet<_> = recorded
        .iter()
        .map(|identity| identity.session.clone())
        .collect();
    let mut provenance = Vec::new();
    for name in names.clone() {
        let found = inventory
            .named_refs(&name, 101)
            .map_err(IdentityError::Inventory)?;
        if !found.complete || found.sessions.len() >= 101 {
            return Err(unresolved(
                "named inventory is incomplete or its candidate page is full",
            ));
        }
        for (_, identity) in found.sessions.into_values() {
            names.insert(identity.session.clone());
            provenance.push(identity);
        }
    }
    let mut candidates = Vec::new();
    for root in roots.0 {
        for harness in ["runner", "broca"] {
            for name in &names {
                let candidate = BindIdentity::new(root, harness, name);
                if broca_core::session_addr(&candidate) == address {
                    candidates.push(candidate);
                }
            }
        }
    }
    let sort = |identities: &mut Vec<BindIdentity>| {
        identities.sort_by(|a, b| {
            (&a.project_root, &a.harness, &a.session).cmp(&(
                &b.project_root,
                &b.harness,
                &b.session,
            ))
        });
        identities.dedup();
    };
    sort(&mut candidates);
    sort(&mut provenance);
    match candidates.as_slice() {
        [identity] => Ok((identity.clone(), provenance)),
        [] => Err(unresolved(
            "no inventory-root candidate hashes to the source address",
        )),
        _ => Err(unresolved(
            "several inventory-root candidates hash to the source address",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::SessionRefs;

    struct Inventory {
        identities: Vec<BindIdentity>,
        complete: bool,
    }

    impl SessionRefResolver for Inventory {
        fn resolve_refs(&self, _: &[&str]) -> Result<SessionRefs, String> {
            unreachable!()
        }

        fn named_refs(&self, name: &str, limit: u32) -> Result<SessionRefs, String> {
            assert_eq!(name, "s");
            assert_eq!(limit, 101);
            Ok(SessionRefs {
                sessions: self
                    .identities
                    .iter()
                    .enumerate()
                    .map(|(index, identity)| {
                        (
                            index.to_string(),
                            (broca_core::session_addr(identity), identity.clone()),
                        )
                    })
                    .collect(),
                complete: self.complete,
            })
        }
    }

    #[test]
    fn unrelated_inventory_names_do_not_hide_the_unique_source_identity() {
        let recorded = BindIdentity::new("/old", "runner", "s");
        let effective = BindIdentity::new("/new", "broca", "s");
        let inventory = Inventory {
            identities: vec![recorded.clone(), effective.clone()],
            complete: true,
        };
        assert_eq!(
            effective_identity(
                &recorded,
                &broca_core::session_addr(&effective),
                Some(&inventory)
            )
            .unwrap(),
            effective
        );
    }

    #[test]
    fn old_root_candidates_require_complete_inventory_an_exact_hash_and_no_head() {
        let recorded = BindIdentity::new("/embedded", "runner", "s");
        let effective = BindIdentity::new("/old", "broca", "s");
        let provenance = BindIdentity::new("/new", "broca", "s");
        let mut inventory = Inventory {
            identities: vec![provenance.clone()],
            complete: true,
        };
        let roots = vec!["/new".into(), "/old".into(), "/old".into()];
        let address = broca_core::session_addr(&effective);
        assert_eq!(
            old_root_identity(
                std::slice::from_ref(&recorded),
                &address,
                (&roots, true),
                &inventory,
                false
            )
            .unwrap(),
            (effective, vec![provenance])
        );
        for (complete, occupied) in [(false, false), (true, true)] {
            assert!(old_root_identity(
                std::slice::from_ref(&recorded),
                &address,
                (&roots, complete),
                &inventory,
                occupied
            )
            .is_err());
        }
        assert!(old_root_identity(
            std::slice::from_ref(&recorded),
            "0123456789abcdef",
            (&roots, true),
            &inventory,
            false
        )
        .is_err());
        inventory.complete = false;
        assert!(old_root_identity(
            std::slice::from_ref(&recorded),
            &address,
            (&roots, true),
            &inventory,
            false
        )
        .is_err());
    }

    #[test]
    fn matching_record_identity_needs_no_inventory() {
        let recorded = BindIdentity::new("/old", "runner", "s");
        assert_eq!(
            effective_identity(&recorded, &broca_core::session_addr(&recorded), None).unwrap(),
            recorded
        );
    }

    #[test]
    fn rekey_requires_a_unique_complete_inventory_identity_at_the_source_address() {
        let recorded = BindIdentity::new("/old", "runner", "s");
        for effective in [
            BindIdentity::new("/old", "broca", "s"),
            BindIdentity::new("/new", "broca", "s"),
        ] {
            let address = broca_core::session_addr(&effective);
            // Legacy and current aliases for one identity are not ambiguous ownership.
            let mut inventory = Inventory {
                identities: vec![effective.clone(), effective.clone()],
                complete: true,
            };
            assert_eq!(
                effective_identity(&recorded, &address, Some(&inventory)).unwrap(),
                effective
            );
            inventory.identities.push(recorded.clone());
            assert_eq!(
                effective_identity(&recorded, &address, Some(&inventory)).unwrap(),
                effective
            );
            inventory.identities.clear();
            assert!(effective_identity(&recorded, &address, Some(&inventory))
                .unwrap_err()
                .to_string()
                .contains("no inventory row"));
            inventory.identities.push(recorded.clone());
            assert!(effective_identity(&recorded, &address, Some(&inventory))
                .unwrap_err()
                .to_string()
                .contains("no inventory row hashes"));
            inventory.identities = vec![effective];
            inventory.complete = false;
            assert!(effective_identity(&recorded, &address, Some(&inventory))
                .unwrap_err()
                .to_string()
                .contains("inventory is incomplete"));
            assert!(effective_identity(&recorded, &address, None)
                .unwrap_err()
                .to_string()
                .contains("no inventory row"));
        }
    }
}
