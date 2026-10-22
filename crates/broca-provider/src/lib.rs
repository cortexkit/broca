//! broca provider module — library surface.
//!
//! The vendor SSE → canonical stream-parts NORMALIZE path ([`normalize`]) plus
//! minimal SSE framing ([`sse`]). Pure, no IO — driven identically by a live
//! connection and by a captured fixture, so the loop's tests stay
//! deterministic/offline while using real event shapes.
//!
//! The live HTTP/SSE transport leg lives behind the `live` feature and the binary
//! subcommands, not in this library surface.
#![forbid(unsafe_code)]

mod cache_hints;
pub mod cache_markers;
pub mod cache_warm;
#[cfg(any(feature = "live", test))]
mod chatgpt_continuation;
pub mod chatgpt_wire;
pub mod claude_code_persona;
pub mod conformance;
pub mod credential_refresh;
pub mod families;
pub mod hash;
pub mod normalize;
mod opencode_go;
pub mod policy;
pub mod reasoning;
pub mod registry;
pub mod request_dump;
mod request_invalid;
mod safe_provider_code;
pub mod service_tier;
pub mod spec;
pub mod sse;
pub mod structured_output;
pub mod tool_id_alias;
pub mod tool_name_alias;
pub mod variant;
pub mod wire;

#[cfg(feature = "live")]
pub mod factory;
#[cfg(feature = "live")]
pub mod live;
#[cfg(feature = "live")]
pub mod transport;

pub use broca_protocol::provider::NormalizedEvent;
pub use normalize::{AnthropicNormalizer, NormalizeError};
pub use request_invalid::safe_provider_subcode;
pub use safe_provider_code::safe_provider_code;

#[cfg(feature = "live")]
pub use factory::{live_provider, live_provider_with_clock_and_refresher, UnsupportedFamily};
#[cfg(feature = "live")]
pub use live::{LiveAnthropicProvider, LiveAuthError};
#[cfg(feature = "live")]
pub use transport::{build_provider_http_client, LiveProvider, ProviderHttpClient};
mod content_progress;
