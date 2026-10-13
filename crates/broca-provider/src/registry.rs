//! Maps a `WireFamilyId` (declarative data) to its `WireFamily` implementation
//! (behavior). This is the single place that knows the closed set of families, so
//! a `ProviderSpec` loaded from the catalog can be turned into a renderer without
//! the rest of the code matching on the id.
//!
//! Families are added HERE as they land; an id with no implementation yet returns
//! `None` rather than panicking, so a catalog listing a not-yet-built family is a
//! graceful "unsupported", not a crash.

use crate::families::{
    AnthropicMessages, BedrockConverse, Gemini, GoogleCodeAssist, OpenAiChat, OpenAiResponses,
    OpenAiResponsesChatgpt,
};
use crate::spec::WireFamilyId;
use crate::wire::WireFamily;

/// Return the `WireFamily` implementation for `id`. Every family in the closed set
/// is now implemented.
pub fn wire_family(id: WireFamilyId) -> Option<Box<dyn WireFamily>> {
    match id {
        WireFamilyId::AnthropicMessages => Some(Box::new(AnthropicMessages)),
        WireFamilyId::OpenAiChat => Some(Box::new(OpenAiChat)),
        WireFamilyId::OpenAiResponses => Some(Box::new(OpenAiResponses)),
        WireFamilyId::Gemini => Some(Box::new(Gemini)),
        WireFamilyId::BedrockConverse => Some(Box::new(BedrockConverse)),
        WireFamilyId::GoogleCodeAssist => Some(Box::new(GoogleCodeAssist)),
        WireFamilyId::OpenAiResponsesChatgpt => Some(Box::new(OpenAiResponsesChatgpt)),
    }
}
