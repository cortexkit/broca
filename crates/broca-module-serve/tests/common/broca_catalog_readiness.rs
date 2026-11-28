pub(super) fn registered_for_bind(
    entries: &[subc_control::CatalogEntry],
    target: &str,
) -> Result<(), String> {
    let entry = entries
        .iter()
        .find(|entry| entry.module_id == target)
        .ok_or_else(|| format!("{target} has not registered in catalog.list"))?;
    if !entry.control_ops.iter().any(|op| op == "route.bind") {
        return Err(format!(
            "{target} registered without the route.bind grant: {:?}",
            entry.control_ops
        ));
    }
    if !entry.ready {
        return Err(format!(
            "{target} registered with route.bind but is not ready: {:?}",
            entry.not_ready
        ));
    }
    Ok(())
}
