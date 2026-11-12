/// Restore the account routing will consult before it makes an admission decision. A tail
/// that cannot be read never becomes a claim that the session has no cache to protect.
async fn seed_store_accepted_send<W: broca_session_loop::writer::Writer>(
    client: &broca_provider::ProviderHttpClient,
    writer: &W,
    bind: &broca_protocol::BindIdentity,
    account: &str,
    frozen: &serde_json::Value,
    now_ms: u64,
) -> Result<broca_core::provider::AcceptedSendEvidence, broca_session_store::session::SessionError> {
    use broca_core::provider::AcceptedSendEvidence;
    let mut evidence = client.session_accepted_send_evidence(bind, account);
    if evidence == AcceptedSendEvidence::NotLoaded {
        let lifetime = broca_core::session_account_cache_expiry_ms(Some(0), frozen).unwrap_or(0);
        evidence = writer.accepted_send(account, now_ms.saturating_sub(lifetime)).await?;
        client.seed_session_accepted_send(bind, account, evidence);
        tracing::debug!(target: "provider", account, ?evidence, "loaded session accepted-send evidence before admission");
    }
    Ok(evidence)
}
