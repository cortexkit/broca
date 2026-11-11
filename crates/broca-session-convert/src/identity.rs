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
    let mut candidates: Vec<_> = found.sessions.into_values().collect();
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
        [] => return Err(unresolved("no inventory row for session")),
        [candidate] => candidate,
        _ => {
            return Err(unresolved(
                "several inventory identities or addresses for session",
            ))
        }
    };
    let derived = broca_core::session_addr(identity);
    if inventoried_address != address || derived != address {
        return Err(unresolved(&format!(
            "inventory identity address {derived} (inventory row {inventoried_address}) differs from source address"
        )));
    }
    Ok(identity.clone())
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
            let error = effective_identity(&recorded, &address, Some(&inventory)).unwrap_err();
            assert_eq!(error.class(), "identity_unresolved");
            assert!(error.to_string().contains("several inventory identities"));
            inventory.identities.clear();
            assert!(effective_identity(&recorded, &address, Some(&inventory))
                .unwrap_err()
                .to_string()
                .contains("no inventory row"));
            inventory.identities.push(recorded.clone());
            assert!(effective_identity(&recorded, &address, Some(&inventory))
                .unwrap_err()
                .to_string()
                .contains("differs from source address"));
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
