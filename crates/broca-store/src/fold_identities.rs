/// Read only: even preview must not create or migrate a store. An absent database
/// supplies no identities, while a database read failure must not masquerade as absence.
pub fn fold_identities(
    root: &std::path::Path,
) -> Result<
    std::collections::BTreeMap<String, broca_protocol::BindIdentity>,
    Box<dyn std::error::Error>,
> {
    let database = root.join("run-index.db");
    let mut identities = std::collections::BTreeMap::new();
    if !database.try_exists()? {
        return Ok(identities);
    }
    let conn = rusqlite::Connection::open_with_flags(
        database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut statement = conn.prepare("SELECT session FROM meta")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let raw = row?;
        let fields: Vec<_> = raw.split('\u{1f}').collect();
        let [project, harness, session] = fields.as_slice() else {
            continue;
        };
        let identity = broca_protocol::BindIdentity::new(*project, *harness, *session);
        if identity.validate_keyable().is_err() {
            continue;
        }
        identities.insert(broca_core::session_addr(&identity), identity);
    }
    Ok(identities)
}
