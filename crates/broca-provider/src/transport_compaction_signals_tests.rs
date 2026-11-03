use super::*;
use broca_core::compaction_provider::notice::{AdvanceNotice, HistoryEditCost};

struct SignalFamily {
    inner: Box<dyn WireFamily>,
    order: Arc<Mutex<Vec<&'static str>>>,
}
impl WireFamily for SignalFamily {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value { self.inner.shape(frozen, opts) }
    fn render_with_provenance(&self, frozen: &FrozenRenderConfig, opts: &CallOptions, sink: Option<&mut broca_protocol::render_provenance::RenderProvenance>) -> Vec<u8> {
        if sink.is_some() { self.order.lock().unwrap().push("render"); }
        self.inner.render_with_provenance(frozen, opts, sink)
    }
    fn endpoint(&self, frozen: &FrozenRenderConfig, auth: &AuthHandle, body: Vec<u8>) -> HttpRequest { self.inner.endpoint(frozen, auth, body) }
    fn normalizer(&self, frozen: &FrozenRenderConfig, step: StepId) -> Box<dyn StreamNormalizer> { self.inner.normalizer(frozen, step) }
}

#[test]
fn compaction_signals_notice_header_for_each_family_and_live_route_change() {
    use crate::spec::WireFamilyId::*;
    for (id, expected) in [(AnthropicMessages,"suffix"),(BedrockConverse,"suffix"),(OpenAiChat,"suffix"),(OpenAiResponses,"suffix"),(OpenAiResponsesChatgpt,"full_resend"),(Gemini,"suffix"),(GoogleCodeAssist,"suffix")] {
        let mut frozen = crate::families::frozen_config_for_family(id, "model");
        if id == OpenAiResponsesChatgpt {
            frozen.chatgpt_wire = Some(serde_json::from_str(include_str!("../tests/fixtures/chatgpt_codex_0_163_identity.json")).unwrap());
        }
        let provider = LiveProvider::new(SignalFamily { inner: crate::registry::wire_family(id).unwrap(), order: Arc::default() }, frozen, AuthHandle::None);
        let notice = AdvanceNotice::new("a".repeat(32),1,"lineage",0,"b".repeat(64),12,"model")
            .with_history_edit_cost(provider.history_edit_cost().unwrap());
        notice.check().unwrap();
        assert_eq!(notice.to_headers()["history_edit_cost"], expected, "{id:?}");
        if id == OpenAiResponsesChatgpt {
            provider.websocket.switch_to_https();
            assert_eq!(provider.history_edit_cost(), Some(HistoryEditCost::Suffix), "the same provider follows its current transport, not the session's initial route");
        }
    }
}

#[tokio::test]
async fn compaction_signals_active_socket_age_is_decided_before_the_next_render() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let provider = codex_provider(&mock);
    provider.bind_run_transport(&bind("active-age"));
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "next question");
    provider.websocket.advance_clock(Duration::from_secs(40 * 60));
    assert!(provider.prepare_chain_reset());
    let second = step(&provider, 2, &next).await;
    assert!(!codex_trace(&second, 0).continuation);
    assert_eq!(codex_trace(&second, 0).full_input_reason.as_deref(), Some("new_connection_age"));
    assert!(!provider.prepare_chain_reset(), "the fresh completed chain must not announce the previous retirement again");
}
