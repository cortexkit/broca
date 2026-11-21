//! An opener the serve tests build a registry over when no session may open.

use broca_protocol::BindIdentity as BrocaBindIdentity;

/// Refuses every open: a registry over it keeps no sessions.
pub(super) struct RefusingOpener;

#[async_trait::async_trait]
impl broca_module::registry::LineageOpener for RefusingOpener {
    /// A session store whose runtime never opens: this opener keeps no sessions.
    fn store_slot(&self) -> &std::sync::Arc<broca_module::store_open::StoreSlot> {
        static SLOT: std::sync::OnceLock<std::sync::Arc<broca_module::store_open::StoreSlot>> =
            std::sync::OnceLock::new();
        SLOT.get_or_init(|| {
            std::sync::Arc::new(broca_module::store_open::StoreSlot::awaiting_key(
                std::path::Path::new("/nonexistent-broca-test-store"),
                1,
            ))
        })
    }
    async fn open(
        &self,
        _key: &BrocaBindIdentity,
        _gen: u64,
        _registry: broca_module::registry::RegistryHandle,
    ) -> Result<broca_module::registry::ActorHandle, broca_module::registry::OpenError> {
        Err(broca_module::registry::OpenError::Open(
            "this test opens no lineage".into(),
        ))
    }
}
