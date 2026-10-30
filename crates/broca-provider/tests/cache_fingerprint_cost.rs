//! Standalone allocator accounting keeps other tests' runtime allocations out of the measurement.

use broca_protocol::{CallOptions, ContentBlock, Message, Role};
use broca_provider::{
    cache_evidence::{compare_fingerprints, fingerprint},
    families::frozen_config_for_family,
    registry::wire_family,
    spec::WireFamilyId,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    time::Instant,
};

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;
fn note() {
    let _ = ACTIVE.try_with(|active| {
        if active.get() {
            ALLOCATIONS.with(|n| n.set(n.get() + 1));
        }
    });
}
// Forward allocations unchanged; only synchronous fingerprint work on this thread is counted.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        note();
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn cpu_ticks() -> Option<u64> {
    // Linux exposes per-thread user/system CPU ticks, excluding time spent descheduled.
    let stat = std::fs::read_to_string("/proc/thread-self/stat").ok()?;
    let fields: Vec<_> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

fn measure(messages: usize, text: &str, repeats: usize, images: usize) -> (usize, f64) {
    let family = WireFamilyId::OpenAiResponsesChatgpt;
    let mut opts = CallOptions::new(
        (0..messages)
            .map(|n| {
                let mut message = Message::new(
                    if n % 2 == 0 {
                        Role::User
                    } else {
                        Role::Assistant
                    },
                    vec![ContentBlock::Text { text: text.into() }],
                );
                message.ordinal = Some(n as u64);
                message
            })
            .collect(),
    );
    let mut frozen = frozen_config_for_family(family, "measurement-model");
    let renderer = wire_family(family).unwrap();
    let mut prepared = None;
    let mut image_base64 = None;
    if images != 0 {
        use base64::Engine;
        let bytes = include_bytes!("../examples/media-live/fixtures/image.png");
        let reference = broca_core::media_validation::validate_media(bytes, "image/png").unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        let profile = broca_provider::media_profile::select_media_profile(
            family,
            None,
            "measurement-model",
            broca_provider::spec::InputModalities::from_value(&serde_json::json!(["image"])),
            Some(1_000_000),
        );
        let carrier = profile.image.user_message;
        frozen.media = Some(profile);
        let mut media = Vec::new();
        for index in 0..images {
            opts.prompt[0]
                .content
                .push(ContentBlock::Media(reference.clone()));
            media.push(broca_provider::media_request::PreparedMedia {
                id: broca_provider::media_request::Occurrence {
                    capture: broca_core::media_request::MediaId {
                        record: 1,
                        message: 0,
                        block: index + 1,
                        part: None,
                    },
                    repetition: 0,
                },
                location: broca_core::media_request::MediaLocation {
                    message: 0,
                    block: index + 1,
                    part: None,
                },
                reference: reference.clone(),
                carrier,
                protected: true,
                tool: false,
                base64: encoded.clone(),
            });
        }
        prepared = Some(broca_provider::media_request::PreparedBundle {
            prompt: opts.prompt.clone(),
            media,
        });
        image_base64 = Some(encoded);
    }
    let mut render =
        |provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>| {
            if let Some(bundle) = prepared.as_mut() {
                broca_provider::media_request::render_prepared_with_provenance(
                    &*renderer,
                    &frozen,
                    &opts,
                    Some(bundle),
                    &|prompt| prompt.to_vec(),
                    provenance,
                )
                .unwrap()
            } else {
                renderer.render_with_provenance(&frozen, &opts, provenance)
            }
        };
    let ticks_per_second = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<f64>().ok());
    let baseline = time(repeats, ticks_per_second, || {
        std::hint::black_box(render(None));
    });
    let traced = time(repeats, ticks_per_second, || {
        let mut provenance = broca_protocol::render_provenance::RenderProvenance::default();
        std::hint::black_box(render(Some(&mut provenance)));
        std::hint::black_box(provenance);
    });
    let mut provenance = broca_protocol::render_provenance::RenderProvenance::default();
    let body = render(Some(&mut provenance));
    assert_eq!(body, render(None));
    if let Some(encoded) = &image_base64 {
        assert_eq!(
            std::str::from_utf8(&body)
                .unwrap()
                .matches(encoded.as_str())
                .count(),
            images,
            "the measured request must contain native image payloads, not markers"
        );
    }
    let previous = fingerprint(family, &body)
        .unwrap()
        .with_provenance(&provenance);
    let evidence = time(repeats, ticks_per_second, || {
        let current = fingerprint(family, &body)
            .unwrap()
            .with_provenance(&provenance);
        std::hint::black_box(
            serde_json::to_value(compare_fingerprints(&previous, &current)).unwrap(),
        );
    });
    let extra_render_allocations = traced
        .allocations
        .checked_sub(baseline.allocations)
        .expect("trace-free rendering must not add allocations");
    let allocations = extra_render_allocations + evidence.allocations;
    let extra_render_cpu = (traced.cpu_ms - baseline.cpu_ms).max(0.0);
    let cpu = extra_render_cpu + evidence.cpu_ms;
    let wall = (traced.wall_ms - baseline.wall_ms).max(0.0) + evidence.wall_ms;
    println!("provenance_and_evidence messages={messages} images={images} body_bytes={} approx_tokens={} baseline_render={baseline:?} traced_render={traced:?} evidence={evidence:?} extra_render_allocations={extra_render_allocations} total_added_allocations={allocations} total_added_cpu_ms={cpu:.3} total_added_wall_ms={wall:.3}", body.len(), messages * text.split_whitespace().count());
    (allocations, cpu)
}

#[test]
fn fingerprint_cost_and_allocation_growth() {
    let measuring_cpu = std::env::var_os("BROCA_MEASURE_FINGERPRINT_CPU").is_some();
    let typical = measure(
        20,
        &" hello".repeat(500),
        if measuring_cpu { 10_000 } else { 1 },
        0,
    );
    let large = measure(
        2000,
        &" hello".repeat(500),
        if measuring_cpu { 20 } else { 1 },
        0,
    );
    // The only growth is vector capacity: 100x more segments must not add 100x allocations.
    assert!(
        large.0 <= typical.0 + 16,
        "per-segment allocation: typical={typical:?}, large={large:?}"
    );
    if measuring_cpu {
        assert!(
            large.1 <= 50.0,
            "large fingerprint exceeds off-send CPU budget: {large:?}"
        );
    }
}

#[derive(Debug)]
struct Timing {
    cpu_ms: f64,
    wall_ms: f64,
    allocations: usize,
}

fn time(repeats: usize, ticks_per_second: Option<f64>, mut operation: impl FnMut()) -> Timing {
    let before = cpu_ticks();
    let start = Instant::now();
    ALLOCATIONS.with(|n| n.set(0));
    ACTIVE.with(|active| active.set(true));
    for _ in 0..repeats {
        operation();
    }
    ACTIVE.with(|active| active.set(false));
    let wall_ms = start.elapsed().as_secs_f64() * 1000.0 / repeats as f64;
    let cpu_ms = before
        .zip(cpu_ticks())
        .zip(ticks_per_second)
        .map_or(wall_ms, |((before, after), hz)| {
            (after - before) as f64 * 1000.0 / hz / repeats as f64
        });
    Timing {
        cpu_ms,
        wall_ms,
        allocations: ALLOCATIONS.with(Cell::get) / repeats,
    }
}

#[test]
fn fingerprint_cost_with_one_million_tokens_and_prepared_images() {
    let measuring_cpu = std::env::var_os("BROCA_MEASURE_FINGERPRINT_CPU").is_some();
    let media = measure(
        2000,
        &" hello".repeat(500),
        if measuring_cpu { 20 } else { 1 },
        4,
    );
    if measuring_cpu {
        assert!(
            media.1 <= 50.0,
            "prepared-media evidence exceeds the CPU budget: {media:?}"
        );
    }
}
