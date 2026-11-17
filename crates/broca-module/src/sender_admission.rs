use broca_core::SessionOwnershipV1;

pub(crate) fn interrupt_granted(ownership: &SessionOwnershipV1, module: &str) -> bool {
    ownership
        .effective_senders()
        .iter()
        .any(|sender| broca_protocol::same_module(&sender.module, module) && sender.interrupt)
}
