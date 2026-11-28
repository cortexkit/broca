use cortexkit_log::{Crates, LevelFilter};

const OWN_CRATES: &[&str] = &[
    env!("CARGO_CRATE_NAME"),
    "broca_catalog",
    "broca_ck",
    "broca_core",
    "broca_diagnostics",
    "broca_fetch_plan",
    "broca_heap_profile",
    "broca_memory_relief",
    "broca_module",
    "broca_module_serve",
    "broca_owner_only",
    "broca_protocol",
    "broca_provider",
    "broca_records",
    "broca_replay",
    "broca_session_loop",
    "broca_session_store",
    "broca_store",
    "broca_subc",
    "broca_wire",
    "ck_session_import",
];

pub(crate) fn for_broca() -> Crates {
    Crates::new()
        .own(OWN_CRATES.iter().copied())
        // The bus connector reports outages and handshake refusals itself.
        .ceiling("async_nats", LevelFilter::WARN)
        .ceiling("async_nats::connector", LevelFilter::OFF)
}

#[cfg(test)]
mod tests {
    use super::OWN_CRATES;
    use serde::Deserialize;
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::process::Command;

    #[derive(Deserialize)]
    struct Metadata {
        workspace_members: HashSet<String>,
        packages: Vec<Package>,
        resolve: Resolve,
    }

    #[derive(Deserialize)]
    struct Package {
        id: String,
        name: String,
        targets: Vec<Target>,
    }

    #[derive(Deserialize)]
    struct Target {
        name: String,
        kind: Vec<String>,
    }

    #[derive(Deserialize)]
    struct Resolve {
        nodes: Vec<Node>,
    }

    #[derive(Deserialize)]
    struct Node {
        id: String,
        deps: Vec<Dependency>,
    }

    #[derive(Deserialize)]
    struct Dependency {
        pkg: String,
        dep_kinds: Vec<DependencyKind>,
    }

    #[derive(Deserialize)]
    struct DependencyKind {
        kind: Option<String>,
    }

    #[test]
    fn log_policy_owns_every_linked_workspace_crate() {
        // Include optional normal dependencies and all target platforms so a
        // feature or platform build cannot silently lose its own INFO records.
        let output = Command::new(env!("CARGO"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args([
                "metadata",
                "--locked",
                "--offline",
                "--all-features",
                "--format-version",
                "1",
            ])
            .output()
            .expect("run cargo metadata");
        assert!(
            output.status.success(),
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let metadata: Metadata =
            serde_json::from_slice(&output.stdout).expect("read cargo metadata graph");
        let package = metadata
            .packages
            .iter()
            .find(|p| {
                p.name == env!("CARGO_PKG_NAME") && metadata.workspace_members.contains(&p.id)
            })
            .expect("find ck-broca's package");
        let graph: HashMap<_, _> = metadata
            .resolve
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node))
            .collect();

        let mut linked = HashSet::new();
        let mut pending = vec![package.id.as_str()];
        while let Some(id) = pending.pop() {
            if linked.insert(id) {
                // A null Cargo dependency kind means a normal, linked
                // dependency, not a build script or a test dependency.
                pending.extend(
                    graph[id]
                        .deps
                        .iter()
                        .filter(|dep| dep.dep_kinds.iter().any(|kind| kind.kind.is_none()))
                        .map(|dep| dep.pkg.as_str()),
                );
            }
        }

        let missing: BTreeSet<_> = metadata
            .packages
            .iter()
            .filter(|p| {
                linked.contains(p.id.as_str()) && metadata.workspace_members.contains(&p.id)
            })
            .flat_map(|p| &p.targets)
            .filter(|target| {
                target
                    .kind
                    .iter()
                    .any(|kind| matches!(kind.as_str(), "lib" | "rlib" | "dylib"))
            })
            .map(|target| target.name.replace('-', "_"))
            .filter(|name| !OWN_CRATES.contains(&name.as_str()))
            .collect();
        assert!(
            missing.is_empty(),
            "linked workspace libraries missing from OWN_CRATES: {missing:?}"
        );
    }
}
