//! Single-item model tools on a sessionless tool-provider route. Provider execution and
//! paid outcomes use a separate System One instance so tool waits cannot starve classifiers.

mod admission;
pub mod consent;
pub mod held;
mod held_images;
#[cfg(test)]
mod held_images_tests;
pub mod permissions;
mod private_content;
mod route;
pub(crate) mod spend;

pub use private_content::{Asks, SubcAsks};
pub use route::{ConsumerFormat, LlmTools, NoPrivateContent, CLASSIFY_TOOL_NAME, TOOL_RESULT_V1};

/// No answer is released when its price cannot be measured. The contract tells the caller
/// to charge its request ceiling against its budget rather than pretending the call was free.
pub(crate) fn unmeasured_cost_refusal() -> cortexkit_llm_tools::errors::Refusal {
    cortexkit_llm_tools::errors::refusal(
        cortexkit_llm_tools::errors::COST_UNMEASURED,
        "the provider omitted input tokens; the billed cost is unmeasured; charge max_cost_usd against the caller budget",
    ).expect("the contract classifies cost_unmeasured")
}
