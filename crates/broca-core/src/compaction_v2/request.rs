//! Request-boundary orchestration. Applying views, flushing notices, and the
//! usable-window check all finish before the caller can make a model request.

use broca_protocol::{MediaRef, Message};
use cortexkit_role_compaction::{
    notice::{Conditions, Event},
    read::Entry,
    ride::{RideCause, RideRequest},
    view::{AppliedView, InsertedImage, View},
};

use super::{
    conditions::{self, SendContext},
    render,
    runtime::{Plane, Runtime, CALL_DEADLINE, COLD_RIDE_DEADLINE},
    state::{Boundary, Journal, State},
    views,
};

pub struct At<'a> {
    pub step: u64,
    pub preset: &'a str,
    pub entries: &'a [Entry],
    pub rides: &'a super::entries::RideEnvelopes,
    pub stored: &'a [Message],
    pub conditions: &'a Conditions,
    pub previous: Option<&'a SendContext>,
    pub current: &'a SendContext,
    /// The prompt cache has provably expired (see `conditions::cache_proven_expired`).
    pub cache_expired: bool,
    /// The session's current requests ask for a cache lifetime that can prove expiry, so Setup
    /// lists `cache_expired` as a cause broca detects.
    pub detects_cache_expired: bool,
    /// How many entries the runner can read cheaply for an `after` page, including look-ahead.
    pub after_page_entry_budget: usize,
    pub chain_reset: bool,
    pub now: u64,
    pub next_mid: &'a str,
    /// What the run's earlier renders kept of the entries they rendered, for this request's
    /// renders to reuse and bring up to date (see `render::RenderCache`); `None` renders every
    /// entry.
    pub render: Option<&'a render::RenderCache>,
    /// The model provider refused this step's request as too long, so it is prepared again
    /// after an `over_limit` ride.
    pub overflow: Option<Overflow>,
}

/// A model provider's refusal of a step's request as too long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Overflow {
    /// The refused request's size in tokens, when the refusal states it.
    pub request_tokens: Option<u64>,
}

#[derive(Debug)]
pub enum Prepared {
    Ready {
        prompt: Vec<Message>,
        applied: Option<AppliedView>,
        request_tokens: u64,
        /// Stored messages the render read back whole, by position: content released from
        /// memory that the applied view sends again. The caller holds them from now on.
        restored: Vec<(usize, Message)>,
        /// The blocks of the stored prompt, by position and block index, whose content the
        /// request's render didn't read (`render::releasable`), for the caller to release.
        releasable: Vec<(usize, Vec<usize>)>,
        /// The bytes of the images the applied view inserted, which `prompt` names by reference
        /// only (`host::inserted_media`).
        media: broca_protocol::request::ViewMedia,
    },
    CompactionUnavailable,
    ContextCeiling {
        request_tokens: u64,
        usable_window: u64,
    },
}

/// Called before StepStarted. A resumed, already frozen step cannot adopt a
/// view accepted after that step began; a ride remains the explicit exception.
pub async fn apply_boundary(
    state: &tokio::sync::Mutex<State>,
    runtime: &Runtime,
    journal: &impl Journal,
    boundary: Boundary<'_>,
) -> Result<(), String> {
    let mut state = state.lock().await;
    if !state.step_is_frozen(boundary.step) {
        for event in state.apply(journal, boundary).await? {
            runtime.event(event);
        }
    } else {
        for event in state.recover_marker(journal, boundary).await? {
            runtime.event(event);
        }
    }
    Ok(())
}

/// Reads back whole the stored prompt's messages whose released content a render needs (see
/// `render::released_but_read`).
#[allow(async_fn_in_trait)]
pub trait ReadBack {
    /// The messages at `positions` of `stored`, each with its released blocks read back.
    async fn read_back(
        &self,
        stored: &[Message],
        positions: &[usize],
    ) -> Result<Vec<Message>, String>;
}

/// Renders `at.stored` under `view`, first reading back through `read_back` any stored message
/// whose released content the view sends. `restored` collects what was read back, and a later
/// render of the same step reuses it. Returns the prompt and the blocks it didn't read.
async fn render_reading_back(
    at: &At<'_>,
    view: Option<&View>,
    restored: &mut Vec<(usize, Message)>,
    image: &mut impl FnMut(&InsertedImage) -> Result<MediaRef, String>,
    read_back: &impl ReadBack,
) -> Result<(Vec<Message>, Vec<(usize, Vec<usize>)>), String> {
    let (edits, plan) = render::plan_for_render(
        at.entries,
        view,
        &at.conditions.rules,
        super::host::view_floor(at.conditions),
    )?;
    let mut stored = std::borrow::Cow::Borrowed(at.stored);
    patch(&mut stored, restored);
    let missing = render::released_but_read(&stored, at.entries, &plan);
    if !missing.is_empty() {
        let whole = read_back.read_back(&stored, &missing).await?;
        if whole.len() != missing.len() {
            return Err("a read back returned another number of messages than asked".into());
        }
        for (position, message) in missing.into_iter().zip(whole) {
            restored.retain(|(at, _)| *at != position);
            restored.push((position, message));
        }
        patch(&mut stored, restored);
    }
    let releasable = render::releasable(&stored, at.entries, &plan);
    let prompt = render::prompt_planned(
        &stored, at.entries, at.rides, &edits, plan, image, at.render,
    )?;
    Ok((prompt, releasable))
}

/// `stored` with each message `restored` names in its place.
fn patch(stored: &mut std::borrow::Cow<'_, [Message]>, restored: &[(usize, Message)]) {
    if restored.is_empty() {
        return;
    }
    let stored = stored.to_mut();
    for (position, message) in restored {
        if let Some(slot) = stored.get_mut(*position) {
            *slot = message.clone();
        }
    }
}

/// Prepares the step's request (see the module's notes), reading back through `read_back` the
/// stored messages whose released content the applied view sends.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_reading_back(
    state: &tokio::sync::Mutex<State>,
    runtime: &Runtime,
    journal: &impl Journal,
    plane: &dyn Plane,
    at: At<'_>,
    read_back: &impl ReadBack,
    mut image: impl FnMut(&InsertedImage) -> Result<MediaRef, String>,
    mut estimate: impl FnMut(&[Message]) -> Result<u64, String>,
) -> Result<Prepared, String> {
    let mut restored = Vec::new();
    let (mut view, frozen) = {
        let mut state = state.lock().await;
        let frozen = state.step_is_frozen(at.step);
        state.start_step(at.step);
        (state.view_for_step(at.step).cloned(), frozen)
    };
    let mut setup = conditions::setup_request(
        at.preset,
        at.conditions,
        at.now,
        at.detects_cache_expired,
        at.after_page_entry_budget,
    );
    setup.applied_view = view.as_ref().map(View::applied);
    setup.newest = views::newest(at.entries);
    runtime.observe(at.entries);
    // With no view to fall back on, an unanswered Setup is waited out for a bounded time before
    // the run ends; a refused one ends it at once.
    if !runtime.ensure_setup(plane, &setup).await
        && view.is_none()
        && !runtime.wait_out_unanswered_setup(plane, &setup).await
    {
        return Ok(Prepared::CompactionUnavailable);
    }
    // A resumed step that already applied a view keeps exactly the bytes it may have sent, so
    // it never rides. A resumed step with no view applied rides as a fresh step would: the ride
    // it began before the crash left nothing durable, and only its answer can give the step the
    // view the uninterrupted run sent with.
    let frozen = frozen && view.is_some();
    // Claimed only where the accepted Setup listed it.
    let cache_expired = at.cache_expired && runtime.detects_cache_expired().await;
    let (mut prompt, mut releasable) =
        render_reading_back(&at, view.as_ref(), &mut restored, &mut image, read_back).await?;
    let mut tokens = estimate(&prompt)?;
    let mut causes = if frozen {
        Vec::new()
    } else {
        conditions::ride_causes(
            at.previous,
            at.current,
            cache_expired,
            at.chain_reset,
            tokens,
            at.conditions,
        )
    };
    // A request the model provider refused as too long rides for a smaller view even on a step
    // that already applied one: the refused bytes were never processed, so no cache they could
    // have written is lost by changing them. The ride names the provider's own count.
    let mut ride_tokens = Some(tokens);
    if let Some(overflow) = at.overflow {
        if !causes.contains(&RideCause::OverLimit) {
            causes.push(RideCause::OverLimit);
        }
        ride_tokens = overflow.request_tokens;
    }
    let usable = conditions::usable_window(at.conditions);
    let mut rode_over_limit = causes.contains(&RideCause::OverLimit);
    if !causes.is_empty() {
        ride(
            state,
            runtime,
            journal,
            plane,
            &mut setup,
            &at,
            (causes, ride_tokens),
        )
        .await?;
        view = state.lock().await.view_for_step(at.step).cloned();
        (prompt, releasable) =
            render_reading_back(&at, view.as_ref(), &mut restored, &mut image, read_back).await?;
        tokens = estimate(&prompt)?;
    }
    // A cache-rewrite ride can return a larger view. It still owes an over-limit
    // opportunity before refusal, just as a request initially over the limit does.
    if tokens > usable && !rode_over_limit && !frozen {
        ride(
            state,
            runtime,
            journal,
            plane,
            &mut setup,
            &at,
            (vec![RideCause::OverLimit], Some(tokens)),
        )
        .await?;
        rode_over_limit = true;
        view = state.lock().await.view_for_step(at.step).cloned();
        (prompt, releasable) =
            render_reading_back(&at, view.as_ref(), &mut restored, &mut image, read_back).await?;
        tokens = estimate(&prompt)?;
    }
    runtime
        .flush(
            plane,
            &setup,
            at.conditions,
            view.as_ref().map(View::applied).as_ref(),
        )
        .await;
    if tokens > usable {
        debug_assert!(rode_over_limit || frozen);
        return Ok(Prepared::ContextCeiling {
            request_tokens: tokens,
            usable_window: usable,
        });
    }
    Ok(Prepared::Ready {
        prompt,
        applied: view.as_ref().map(View::applied),
        request_tokens: tokens,
        restored,
        releasable,
        media: view
            .as_ref()
            .map(super::host::inserted_media)
            .unwrap_or_default(),
    })
}

async fn ride(
    state: &tokio::sync::Mutex<State>,
    runtime: &Runtime,
    journal: &impl Journal,
    plane: &dyn Plane,
    setup: &mut cortexkit_role_compaction::setup::SetupRequest,
    at: &At<'_>,
    opportunity: (Vec<RideCause>, Option<u64>),
) -> Result<(), String> {
    let (causes, tokens) = opportunity;
    let deadline = if causes.contains(&RideCause::CacheExpired) {
        COLD_RIDE_DEADLINE
    } else {
        CALL_DEADLINE
    };
    let mut request = RideRequest::new(causes, deadline.as_millis() as u64, at.conditions.clone());
    request.applied_view = state.lock().await.view_for_step(at.step).map(View::applied);
    if request.causes.contains(&RideCause::OverLimit) {
        request.request_tokens = tokens;
    }
    let Some(received) = runtime.ride(plane, setup, &request).await else {
        return Ok(());
    };
    let version = received.view().version;
    let identity = received.view().applied();
    let publication = super::diagnostics::Publication::from_view(received.view());
    let mut state = state.lock().await;
    let answer = state
        .accept(
            journal,
            at.entries,
            received,
            &at.conditions.rules,
            super::host::view_floor(at.conditions),
        )
        .await
        .inspect_err(|_| journal.view_refused(&publication, "internal_error"))?;
    // A crash can leave a ride's view accepted but not applied. Resumed, the step rides again,
    // and the provider re-offers that same view: it is still this step's answer, so it applies
    // now instead of being refused as not newer.
    let reoffered = answer.is_err()
        && state
            .latest()
            .is_some_and(|latest| latest.applied() == identity);
    match answer {
        Ok(()) => apply_ride_answer(&mut state, runtime, journal, setup, at).await?,
        Err(_) if reoffered => apply_ride_answer(&mut state, runtime, journal, setup, at).await?,
        Err(refusal) => {
            journal.view_refused(&publication, &super::diagnostics::refusal_code(&refusal));
            runtime.event(Event::ViewRefused { version, refusal });
        }
    }
    Ok(())
}

/// Apply the view a ride answered with at this step's boundary.
async fn apply_ride_answer(
    state: &mut State,
    runtime: &Runtime,
    journal: &impl Journal,
    setup: &mut cortexkit_role_compaction::setup::SetupRequest,
    at: &At<'_>,
) -> Result<(), String> {
    let boundary = Boundary {
        step: at.step,
        entries: at.entries,
        rules: &at.conditions.rules,
        floor: super::host::view_floor(at.conditions),
        next_mid: at.next_mid,
    };
    for event in state.apply(journal, boundary).await? {
        runtime.event(event);
    }
    setup.applied_view = state.applied_identity();
    Ok(())
}
