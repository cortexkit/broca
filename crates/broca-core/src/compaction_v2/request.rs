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
    pub chain_reset: bool,
    pub now: u64,
    pub next_mid: &'a str,
}

#[derive(Debug)]
pub enum Prepared {
    Ready {
        prompt: Vec<Message>,
        applied: Option<AppliedView>,
        request_tokens: u64,
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
    journal: &dyn Journal,
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

pub async fn prepare(
    state: &tokio::sync::Mutex<State>,
    runtime: &Runtime,
    journal: &dyn Journal,
    plane: &dyn Plane,
    at: At<'_>,
    mut image: impl FnMut(&InsertedImage) -> Result<MediaRef, String>,
    mut estimate: impl FnMut(&[Message]) -> Result<u64, String>,
) -> Result<Prepared, String> {
    let (mut view, frozen) = {
        let mut state = state.lock().await;
        let frozen = state.step_is_frozen(at.step);
        state.start_step(at.step);
        (state.view_for_step(at.step).cloned(), frozen)
    };
    let mut setup =
        conditions::setup_request(at.preset, at.conditions, at.now, at.detects_cache_expired);
    setup.applied_view = view.as_ref().map(View::applied);
    setup.newest = views::newest(at.entries);
    runtime.observe(at.entries);
    if !runtime.ensure_setup(plane, &setup).await && view.is_none() {
        return Ok(Prepared::CompactionUnavailable);
    }
    // A resumed step that already applied a view keeps exactly the bytes it may have sent, so
    // it never rides. A resumed step with no view applied rides as a fresh step would: the ride
    // it began before the crash left nothing durable, and only its answer can give the step the
    // view the uninterrupted run sent with.
    let frozen = frozen && view.is_some();
    // Claimed only where the accepted Setup listed it.
    let cache_expired = at.cache_expired && runtime.detects_cache_expired().await;
    let mut prompt = render::prompt_view_with_rides(
        at.stored,
        at.entries,
        at.rides,
        view.as_ref(),
        &at.conditions.rules,
        &mut image,
    )?;
    let mut tokens = estimate(&prompt)?;
    let causes = if frozen {
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
            (causes, tokens),
        )
        .await?;
        view = state.lock().await.view_for_step(at.step).cloned();
        prompt = render::prompt_view_with_rides(
            at.stored,
            at.entries,
            at.rides,
            view.as_ref(),
            &at.conditions.rules,
            &mut image,
        )?;
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
            (vec![RideCause::OverLimit], tokens),
        )
        .await?;
        rode_over_limit = true;
        view = state.lock().await.view_for_step(at.step).cloned();
        prompt = render::prompt_view_with_rides(
            at.stored,
            at.entries,
            at.rides,
            view.as_ref(),
            &at.conditions.rules,
            &mut image,
        )?;
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
    })
}

async fn ride(
    state: &tokio::sync::Mutex<State>,
    runtime: &Runtime,
    journal: &dyn Journal,
    plane: &dyn Plane,
    setup: &mut cortexkit_role_compaction::setup::SetupRequest,
    at: &At<'_>,
    opportunity: (Vec<RideCause>, u64),
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
        request.request_tokens = Some(tokens);
    }
    let Some(received) = runtime.ride(plane, setup, &request).await else {
        return Ok(());
    };
    let version = received.view().version;
    let identity = received.view().applied();
    let mut state = state.lock().await;
    let answer = state
        .accept(
            journal,
            at.entries,
            received,
            &at.conditions.rules,
            at.conditions.accepts_images == Some(true),
        )
        .await?;
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
        Err(refusal) => runtime.event(Event::ViewRefused { version, refusal }),
    }
    Ok(())
}

/// Apply the view a ride answered with at this step's boundary.
async fn apply_ride_answer(
    state: &mut State,
    runtime: &Runtime,
    journal: &dyn Journal,
    setup: &mut cortexkit_role_compaction::setup::SetupRequest,
    at: &At<'_>,
) -> Result<(), String> {
    let boundary = Boundary {
        step: at.step,
        entries: at.entries,
        rules: &at.conditions.rules,
        accepts_images: at.conditions.accepts_images == Some(true),
        next_mid: at.next_mid,
    };
    for event in state.apply(journal, boundary).await? {
        runtime.event(event);
    }
    setup.applied_view = state.applied_identity();
    Ok(())
}
