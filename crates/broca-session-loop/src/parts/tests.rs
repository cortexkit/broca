//! The decoded-part cache: one decode per part and type while any holder keeps the value.

use std::sync::Arc;

use broca_protocol::request::Tool;
use serde_json::{json, Value};

use super::*;

/// A shared store in memory holding a tool list and a render config, and the parts naming them.
fn store() -> (SharedParts, PartRef, PartRef) {
    let mut parts = SharedParts::new(Box::new(MemoryParts::default()));
    let tools = parts
        .put(
            PartKind::Tools,
            &json!([{"name": "read", "description": "reads", "input_schema": {"type": "object"}}]),
        )
        .unwrap();
    let render_config = parts
        .put(
            PartKind::RenderConfig,
            &json!({
                "family": "test",
                "structured_output": {
                    "mode": "final_text",
                    "name": "result",
                    "schema_jcs": "{}",
                    "enforcement": "enforced",
                    "delivery": "strict",
                },
            }),
        )
        .unwrap();
    (parts, tools, render_config)
}

#[test]
fn a_part_in_use_is_decoded_once_and_shared() {
    let (parts, tools, _) = store();
    let before = decodes_on_this_thread();
    let first = shared::<Vec<Tool>, _>(&parts, &tools).unwrap();
    let second = shared::<Vec<Tool>, _>(&parts, &tools).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(decodes_on_this_thread() - before, 1);
    // Another handle on the same store shares the same value.
    let other = SharedParts::new(Box::new(parts.clone()));
    assert!(Arc::ptr_eq(
        &first,
        &shared::<Vec<Tool>, _>(&other, &tools).unwrap()
    ));
    assert_eq!(decodes_on_this_thread() - before, 1);
}

#[test]
fn a_part_decoded_as_two_types_is_two_entries() {
    let (parts, tools, _) = store();
    let before = decodes_on_this_thread();
    let typed = shared::<Vec<Tool>, _>(&parts, &tools).unwrap();
    let value = shared::<Value, _>(&parts, &tools).unwrap();
    assert_eq!(decodes_on_this_thread() - before, 2);
    assert_eq!(typed.len(), 1);
    assert!(value.is_array());
    assert_eq!(parts.decoded().unwrap().len(), 2);
}

#[test]
fn a_value_is_freed_with_its_last_holder_and_decoded_once_more_when_read_again() {
    let (parts, tools, render_config) = store();
    let before = decodes_on_this_thread();
    let first = shared::<Vec<Tool>, _>(&parts, &tools).unwrap();
    let freed = Arc::downgrade(&first);
    drop(first);
    assert!(
        freed.upgrade().is_none(),
        "the value is freed with its holder"
    );
    // The dead entry stays until the next insert removes it.
    let cache = parts.decoded().unwrap();
    assert_eq!(cache.len(), 1);
    let config = shared::<Value, _>(&parts, &render_config).unwrap();
    assert_eq!(cache.len(), 1, "the insert removed the dead entry");
    let again = shared::<Vec<Tool>, _>(&parts, &tools).unwrap();
    assert_eq!(decodes_on_this_thread() - before, 3);
    assert!(Arc::ptr_eq(
        &again,
        &shared::<Vec<Tool>, _>(&parts, &tools).unwrap()
    ));
    assert_eq!(decodes_on_this_thread() - before, 3);
    drop(config);
}

#[test]
fn the_deployments_parts_are_shared_by_every_handle_on_them() {
    let mut deployment = DeploymentParts::default();
    let tools = deployment
        .put(
            PartKind::Tools,
            &json!([{"name": "read", "description": "reads", "input_schema": {}}]),
        )
        .unwrap();
    // Two sessions' actors, each with its own handle on the deployment's parts.
    let first = SharedParts::new(Box::new(deployment.clone()));
    let second = SharedParts::new(Box::new(deployment.clone()));
    let before = decodes_on_this_thread();
    let held = shared::<Vec<Tool>, _>(&first, &tools).unwrap();
    assert!(Arc::ptr_eq(
        &held,
        &shared::<Vec<Tool>, _>(&second, &tools).unwrap()
    ));
    assert_eq!(decodes_on_this_thread() - before, 1);
}

#[test]
fn a_store_without_a_cache_decodes_on_every_read() {
    let mut parts = MemoryParts::default();
    let tools = parts.put(PartKind::Tools, &json!([])).unwrap();
    let before = decodes_on_this_thread();
    let first = shared::<Vec<Tool>, _>(&parts, &tools).unwrap();
    let second = shared::<Vec<Tool>, _>(&parts, &tools).unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert_eq!(decodes_on_this_thread() - before, 2);
}

/// A run that downgrades its render config takes a value of its own: a second run reading the
/// same part gets the config as decoded, whether a third holder keeps the shared value alive
/// (the downgrade copies it) or the downgrading run was its only holder (the downgrade takes
/// the value out of the cache, and the second read decodes it again).
#[test]
fn a_downgraded_render_config_leaves_the_shared_value_as_decoded() {
    for third_holder in [true, false] {
        let (parts, _, render_config) = store();
        let decoded = parts.get(&render_config).unwrap();
        let mut downgrading = shared::<Value, _>(&parts, &render_config).unwrap();
        let third = third_holder.then(|| Arc::clone(&downgrading));
        crate::run::downgrade_render_config(&mut downgrading);
        assert_ne!(
            *downgrading, decoded,
            "the downgrade changed the run's config"
        );
        let before = decodes_on_this_thread();
        let second = shared::<Value, _>(&parts, &render_config).unwrap();
        assert_eq!(*second, decoded, "third holder: {third_holder}");
        assert!(!Arc::ptr_eq(&second, &downgrading));
        match &third {
            Some(third) => {
                assert!(Arc::ptr_eq(&second, third));
                assert_eq!(decodes_on_this_thread(), before);
            }
            None => assert_eq!(decodes_on_this_thread() - before, 1),
        }
    }
}
