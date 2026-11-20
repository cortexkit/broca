use super::{StreamFloor, StreamRateWindow};
use broca_protocol::{display::DisplayEvent, MessageId, NormalizedEvent};
use std::time::Duration;

fn floor() -> StreamFloor {
    StreamFloor {
        bytes_per_sec: 8.0,
        window: Duration::from_secs(60),
    }
}

fn text(bytes: usize) -> NormalizedEvent {
    NormalizedEvent::Display(DisplayEvent::TextDelta {
        mid: None,
        part: None,
        message_id: MessageId("m1".to_string()),
        tool_call_id: None,
        delta: "x".repeat(bytes),
    })
}

/// The trap this design exists to avoid: a model that reasons for a long time before emitting
/// anything is SILENT, not slow, and silence belongs to the idle timeout. If the window opened
/// at request time instead of at the first content byte, this is the case that would abort a
/// perfectly healthy run -- so the assertion is that a long quiet stretch produces no reading
/// at all, not merely a passing one.
#[tokio::test(start_paused = true)]
async fn silence_never_opens_the_window() {
    let mut window = StreamRateWindow::new(floor());
    for _ in 0..10 {
        window.observe(&[NormalizedEvent::Display(DisplayEvent::DisplayGap {
            dropped: 1,
        })]);
        tokio::time::advance(Duration::from_secs(60)).await;
    }
    assert_eq!(
        window.breach(),
        None,
        "ten minutes of non-content events must leave the window unopened"
    );
}

/// The signature the floor is FOR: content arriving steadily, well under the floor, for longer
/// than a full window. ~1 byte/sec against a floor of 8.
#[tokio::test(start_paused = true)]
async fn a_steady_trickle_breaches_after_a_full_window() {
    let mut window = StreamRateWindow::new(floor());
    for _ in 0..61 {
        window.observe(&[text(1)]);
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    let rate = window
        .breach()
        .expect("a 1 byte/sec stream is under an 8 byte/sec floor");
    assert!(rate < 8.0, "reported rate {rate} should be below the floor");
}

/// A healthy stream must not trip, and the control matters: without it the trickle test above
/// would pass against an implementation that fires on any stream at all.
#[tokio::test(start_paused = true)]
async fn a_healthy_stream_never_breaches() {
    let mut window = StreamRateWindow::new(floor());
    for _ in 0..61 {
        window.observe(&[text(400)]);
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    assert_eq!(
        window.breach(),
        None,
        "400 bytes/sec is far above the floor"
    );
}

/// A slow start must not fire before a full window has elapsed, or a stream that begins slowly
/// and recovers would be killed on its first seconds.
#[tokio::test(start_paused = true)]
async fn a_partial_window_never_breaches() {
    let mut window = StreamRateWindow::new(floor());
    window.observe(&[text(1)]);
    tokio::time::advance(Duration::from_secs(59)).await;
    assert_eq!(
        window.breach(),
        None,
        "59s of a 60s window is not yet a measurement"
    );
}
