//! Which earlier renderer versions each wire family still reproduces byte-identically, and the
//! proof behind each such declaration.
//!
//! A resume of a run frozen at an earlier renderer version is accepted only when the family
//! declares that version `Reproducible` in `families::earlier_renderer_versions`. These tests
//! hold that table to three rules:
//! - every version from 1 up to the current one is listed, so a bump cannot leave the old
//!   version undecided;
//! - a version declared reproducible has a pinned config and prompt from that version which the
//!   current code renders to the pinned bytes, so a declaration cannot exist without its proof;
//! - the live provider accepts exactly the declared versions and, for one of them, states the
//!   blob the run recorded, so the resume comparison stays exact.

use std::collections::BTreeSet;

use broca_protocol::CallOptions;
use broca_provider::{
    families::{
        earlier_renderer_versions, renderer_version_for, reproduces_renderer_version,
        AnthropicMessages, BedrockConverse, EarlierRendererVersion, Gemini, GoogleCodeAssist,
        OpenAiChat, OpenAiResponses, OpenAiResponsesChatgpt,
    },
    spec::WireFamilyId,
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::Value;

use crate::provider_state_legacy::{legacy_call_options, LEGACY_PINS};

/// Every wire family. The match keeps this list complete: a new family does not compile until
/// it is added here, and so until its renderer-version history is checked.
fn all_families() -> [WireFamilyId; 7] {
    let listed = |family: WireFamilyId| match family {
        WireFamilyId::AnthropicMessages
        | WireFamilyId::OpenAiChat
        | WireFamilyId::OpenAiResponses
        | WireFamilyId::Gemini
        | WireFamilyId::BedrockConverse
        | WireFamilyId::GoogleCodeAssist
        | WireFamilyId::OpenAiResponsesChatgpt => (),
    };
    let families = [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::Gemini,
        WireFamilyId::BedrockConverse,
        WireFamilyId::GoogleCodeAssist,
        WireFamilyId::OpenAiResponsesChatgpt,
    ];
    families.iter().copied().for_each(listed);
    families
}

fn renderer(family: WireFamilyId) -> &'static dyn WireFamily {
    match family {
        WireFamilyId::AnthropicMessages => &AnthropicMessages,
        WireFamilyId::OpenAiChat => &OpenAiChat,
        WireFamilyId::OpenAiResponses => &OpenAiResponses,
        WireFamilyId::Gemini => &Gemini,
        WireFamilyId::BedrockConverse => &BedrockConverse,
        WireFamilyId::GoogleCodeAssist => &GoogleCodeAssist,
        WireFamilyId::OpenAiResponsesChatgpt => &OpenAiResponsesChatgpt,
    }
}

/// A render golden: its request, its frozen config, its bytes and the renderer version they
/// were pinned at.
struct Golden {
    request: CallOptions,
    frozen: FrozenRenderConfig,
    bytes: &'static str,
    version: &'static str,
}

/// The family's main render golden, where one exists for a family that declares a reproducible
/// version. Its bytes add a system prompt, tools and a tool round trip to what the legacy pins
/// cover.
fn golden(family: WireFamilyId) -> Option<Golden> {
    use crate::{
        gemini_render_golden as gemini, google_codeassist_render_golden as codeassist,
        openai_responses_chatgpt_render_golden as chatgpt, openai_responses_golden as responses,
    };
    Some(match family {
        WireFamilyId::OpenAiResponses => Golden {
            request: responses::fixed_request(),
            frozen: responses::golden_frozen(),
            bytes: responses::GOLDEN,
            version: responses::GOLDEN_VERSION,
        },
        WireFamilyId::OpenAiResponsesChatgpt => Golden {
            request: chatgpt::fixed_request(),
            frozen: chatgpt::golden_frozen(),
            bytes: chatgpt::GOLDEN,
            version: chatgpt::GOLDEN_VERSION,
        },
        WireFamilyId::Gemini => Golden {
            request: gemini::fixed_request(),
            frozen: gemini::golden_frozen(),
            bytes: gemini::GOLDEN,
            version: gemini::GOLDEN_VERSION,
        },
        WireFamilyId::GoogleCodeAssist => Golden {
            request: codeassist::fixed_request(),
            frozen: codeassist::golden_frozen(),
            bytes: codeassist::GOLDEN,
            version: codeassist::GOLDEN_VERSION,
        },
        WireFamilyId::AnthropicMessages
        | WireFamilyId::OpenAiChat
        | WireFamilyId::BedrockConverse => return None,
    })
}

fn legacy_pins() -> Vec<Value> {
    serde_json::from_str(LEGACY_PINS).expect("legacy pins are JSON")
}

fn declared_reproducible(family: WireFamilyId) -> Vec<u32> {
    earlier_renderer_versions(family)
        .iter()
        .filter(|(_, disposition)| *disposition == EarlierRendererVersion::Reproducible)
        .map(|(version, _)| *version)
        .collect()
}

/// The guard on renderer-version bumps: each family lists every earlier version exactly once,
/// oldest first, each either reproducible or not reproducible with a named reason. Bumping a
/// family's version without adding the version it leaves behind fails here.
#[test]
fn every_renderer_version_bump_decides_whether_the_old_version_is_reproducible() {
    for family in all_families() {
        let current = renderer_version_for(family);
        let listed: Vec<u32> = earlier_renderer_versions(family)
            .iter()
            .map(|(version, _)| *version)
            .collect();
        assert_eq!(
            listed,
            (1..current).collect::<Vec<_>>(),
            "{family:?} is at renderer version {current}: earlier_renderer_versions must list \
             every earlier version, oldest first, each marked Reproducible (with a byte-equality \
             proof) or NotReproducible (with the reason)"
        );
        for (version, disposition) in earlier_renderer_versions(family) {
            if let EarlierRendererVersion::NotReproducible(reason) = disposition {
                assert!(
                    !reason.trim().is_empty(),
                    "{family:?} v{version} is marked not reproducible without a reason"
                );
            }
        }
    }
}

/// Each reproducible declaration renders a pinned config and prompt from the declared version
/// under the current code to the bytes pinned for it. A declaration with no pin fails: the
/// declaration must not exist without its proof.
#[test]
fn every_reproducible_declaration_renders_its_pinned_old_bytes() {
    let mut pins = legacy_pins();
    // Version 6 bounded explicit output by the tier ceiling, not the frozen model maximum.
    // Keep that request separate from the pre-provider-text-state fixtures, which predate it.
    pins.extend(
        serde_json::from_str::<Vec<Value>>(include_str!("../fixtures/google_codeassist_v6.json"))
            .expect("Code Assist v6 pins are JSON"),
    );
    let opts = legacy_call_options();
    let mut proven = BTreeSet::new();
    for family in all_families() {
        for version in declared_reproducible(family) {
            let family_id = serde_json::to_value(family).unwrap();
            let cases: Vec<&Value> = pins
                .iter()
                .filter(|case| {
                    case["frozen"]["family_id"] == family_id
                        && case["frozen"]["renderer_version"] == version
                })
                .collect();
            assert!(
                !cases.is_empty(),
                "{family:?} declares renderer version {version} reproducible, but no pinned \
                 config and bytes from that version prove it"
            );
            for case in cases {
                let frozen: FrozenRenderConfig =
                    serde_json::from_value(case["frozen"].clone()).unwrap();
                // Read back and written again by the current code, the recorded config gains no
                // field, so the resume's blob comparison sees the recorded blob unchanged.
                assert_eq!(serde_json::to_value(&frozen).unwrap(), case["frozen"]);
                assert_eq!(
                    String::from_utf8(renderer(family).render(&frozen, &opts)).unwrap(),
                    case["body"].as_str().unwrap(),
                    "{family:?} v{version}: the current code no longer renders the pinned bytes"
                );
            }

            // The golden bytes were pinned at a version no later than the declared one and the
            // golden test has held them unchanged since, so they are what the declared version
            // sent for the golden request. The declared version's config is the golden's config
            // as it was frozen then: without provider text state.
            if let Some(golden) = golden(family) {
                let pinned_at: u32 = golden.version.trim().parse().unwrap();
                if pinned_at <= version {
                    let mut frozen = golden.frozen;
                    frozen.renderer_version = version;
                    frozen.provider_text_state = false;
                    assert_eq!(
                        String::from_utf8(renderer(family).render(&frozen, &golden.request))
                            .unwrap(),
                        golden.bytes.trim_end_matches('\n'),
                        "{family:?} v{version}: the golden request renders different bytes"
                    );
                }
            }
            proven.insert((format!("{family:?}"), version));
        }
    }
    assert!(
        !proven.is_empty(),
        "no reproducible declaration was checked"
    );
}

/// The table answers only for what it declares: the current version and the declared earlier
/// versions are reproduced; every other version, including a later one, is not.
#[test]
fn only_declared_versions_are_reproduced() {
    for family in all_families() {
        let current = renderer_version_for(family);
        assert!(reproduces_renderer_version(family, current));
        assert!(!reproduces_renderer_version(family, current + 1));
        assert!(!reproduces_renderer_version(family, 0));
        for (version, disposition) in earlier_renderer_versions(family) {
            assert_eq!(
                reproduces_renderer_version(family, *version),
                *disposition == EarlierRendererVersion::Reproducible,
                "{family:?} v{version}"
            );
        }
    }
}

#[cfg(feature = "live")]
mod live {
    use broca_core::provider::Provider;
    use broca_provider::{
        families::{
            gemini_frozen_config, google_codeassist_frozen_config,
            openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, Gemini,
            GoogleCodeAssist, OpenAiResponses, OpenAiResponsesChatgpt,
        },
        spec::WireFamilyId,
        transport::LiveProvider,
        wire::{AuthHandle, FrozenRenderConfig, WireFamily},
    };
    use serde_json::Value;

    use super::{legacy_pins, renderer_version_for};

    fn provider(frozen: FrozenRenderConfig) -> Box<dyn Provider> {
        fn live<F: WireFamily + 'static>(
            family: F,
            frozen: FrozenRenderConfig,
        ) -> Box<dyn Provider> {
            Box::new(LiveProvider::new(family, frozen, AuthHandle::None))
        }
        match frozen.family_id {
            WireFamilyId::OpenAiResponses => live(OpenAiResponses, frozen),
            WireFamilyId::OpenAiResponsesChatgpt => live(OpenAiResponsesChatgpt, frozen),
            WireFamilyId::Gemini => live(Gemini, frozen),
            WireFamilyId::GoogleCodeAssist => live(GoogleCodeAssist, frozen),
            other => panic!("no legacy pin for {other:?}"),
        }
    }

    /// The config the current code builds for a pin's run on resume: the family's current
    /// helper config with the pin's model and project, and the recorded absence of provider
    /// text state restored as false.
    fn current_config(pin: &FrozenRenderConfig, provider_text_state: bool) -> FrozenRenderConfig {
        let mut frozen = match pin.family_id {
            WireFamilyId::OpenAiResponses => openai_responses_frozen_config(&pin.wire_model_id),
            WireFamilyId::OpenAiResponsesChatgpt => {
                openai_responses_chatgpt_frozen_config(&pin.wire_model_id)
            }
            WireFamilyId::Gemini => gemini_frozen_config(&pin.wire_model_id),
            WireFamilyId::GoogleCodeAssist => google_codeassist_frozen_config(&pin.wire_model_id),
            other => panic!("no legacy pin for {other:?}"),
        };
        frozen.project_id = pin.project_id.clone();
        frozen.provider_text_state = provider_text_state;
        frozen
    }

    /// Each legacy pin (`pre_provider_text_state.json`: a frozen config recorded before the
    /// `provider_text_state` flag existed, at its family's previous renderer version) is
    /// accepted by a live provider at the current version, which states exactly the pinned blob
    /// for the pinned version. If the provider instead enables `provider_text_state` (the
    /// default for new runs, which echoes message phases or text signatures and so changes the
    /// bytes), that flag is the only difference, and the resume is right to refuse.
    #[test]
    fn live_provider_accepts_a_declared_version_and_states_its_recorded_blob() {
        for case in legacy_pins() {
            let pin: FrozenRenderConfig = serde_json::from_value(case["frozen"].clone()).unwrap();
            let current = renderer_version_for(pin.family_id);
            assert_ne!(pin.renderer_version, current);

            let restored = provider(current_config(&pin, false));
            assert_eq!(restored.renderer_version(), current);
            assert!(restored.reproduces_renderer_version(pin.renderer_version));
            assert!(!restored.reproduces_renderer_version(pin.renderer_version - 1));
            assert_eq!(
                restored.render_config_blob_for_version(pin.renderer_version),
                case["frozen"],
                "{:?}: the blob stated for the recorded version must equal the recorded blob",
                pin.family_id
            );
            // Only the version moves: at its own version the provider states its own blob.
            let mut own = case["frozen"].clone();
            own["renderer_version"] = current.into();
            assert_eq!(restored.render_config_blob(), own);
            assert_eq!(
                restored.render_config_blob_for_version(current),
                restored.render_config_blob()
            );
            // An undeclared version is never given a substituted blob.
            assert_eq!(
                restored.render_config_blob_for_version(pin.renderer_version - 1),
                restored.render_config_blob()
            );

            let unrestored = provider(current_config(&pin, true));
            let mut differing = unrestored
                .render_config_blob_for_version(pin.renderer_version)
                .as_object()
                .unwrap()
                .clone();
            differing.retain(|key, value| case["frozen"].get(key) != Some(&*value));
            assert_eq!(
                Value::Object(differing),
                serde_json::json!({"provider_text_state": true}),
                "{:?}",
                pin.family_id
            );
        }
    }

    /// A provider not at its family's current version (one rebuilt from a recorded config)
    /// reproduces only its own version: the declarations describe the current code only.
    #[test]
    fn a_provider_rebuilt_at_an_old_version_reproduces_only_that_version() {
        let mut frozen = openai_responses_chatgpt_frozen_config("model");
        frozen.renderer_version = 6;
        frozen.provider_text_state = false;
        let rebuilt = provider(frozen);
        assert!(rebuilt.reproduces_renderer_version(6));
        assert!(!rebuilt.reproduces_renderer_version(7));
        assert!(!rebuilt.reproduces_renderer_version(8));
    }
}
