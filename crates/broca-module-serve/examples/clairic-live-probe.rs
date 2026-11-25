//! Probe Cloudflare Workers AI's Clairic extraction model to settle open points in the
//! `extractor/v1` role contract.
//!
//! WHAT IT SENDS. One fixed plan per run, to
//! `POST https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/cloudflare/clairic`.
//! The default plan sends twelve requests: every answer shape (all nine field types, quantiles on),
//! span offsets over non-ASCII text, `decimals` on a number list, a date estimate, a request
//! split across several 8,192-token passes with its one-pass control and its reversed field
//! order, an unknown field type, an unknown option key, a 20,000-character `instructions`, an
//! empty state, and the one-pass control again with one small embedded PNG.
//! `--plan open-readings` sends eight follow-up requests covering a year step and date forecast
//! bounds, image cost across passes and at larger sizes, image spelling and count, and
//! 60,000-character instructions. The sender enforces the selected plan's cap (twelve or eight).
//! Nothing is retried; a failed request is recorded with whatever came back.
//!
//! RUN.
//!   cargo run -p broca-module-serve --example clairic-live-probe -- --dry-run --out DIR
//!   cargo run -p broca-module-serve --example clairic-live-probe -- --live --plan open-readings --out DIR
//! `--dry-run` (the default) checks that the dev-tools handle file names the credential, builds
//! and writes every request body, prints each one's size and the body (a preview for bodies over
//! 1,500 bytes, whose full text is in the written file), and sends nothing. It never contacts the
//! vault. `--live` refuses an output directory a previous live run used, so a second run cannot
//! spend a second set of requests by accident.
//!
//! OUTPUT in `--out`, per request `NN-name`: `NN-name.request.json` (the exact bytes sent),
//! `NN-name.body` (the reply body as received), and `NN-name.json` (HTTP status, reason,
//! response headers minus cookies, the received body's size and SHA-256, and the body embedded
//! as-is when it is JSON, else as text). `summary.md` answers each open question from the
//! captured bytes. Every file passes the credential scrub below before it is written; the
//! summary reports how many replacements that made (expected: none), and only a replacement
//! makes a written body differ from the bytes received.
//!
//! CREDENTIAL. The Workers AI token and the account id come from the credentials module over
//! subc, by the handle stored for `apikey:cloudflare-workers-ai` in broca's
//! `dev-tools-handles.json` under the config home (override: `BROCA_DEVTOOLS_HANDLES`). The
//! token travels only in the `authorization` header. Neither the token nor the account id is
//! printed; the only thing reported about the account id is whether the vault returned a
//! non-empty one. Every file this tool writes, and every per-request outcome line it prints,
//! is checked for both first, and a hit is replaced and counted.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use broca_module_serve::vault_config::load_devtools_handle;
use broca_module_serve::CredentialId;
use clap::{Parser, ValueEnum};
use serde::de::{Deserializer, MapAccess, Visitor};
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

const CREDENTIAL_ID: &str = "apikey:cloudflare-workers-ai";
const MODEL: &str = "@cf/cloudflare/clairic";
/// The original plan's request cap.
const MAX_REQUESTS: usize = 12;
const OPEN_READINGS_MAX_REQUESTS: usize = 8;
/// Clairic's documented per-pass token budget for the state plus the field definitions.
const PASS_TOKENS: usize = 8192;
/// The size of the embedded test image, in pixels per side.
const PNG_SIDE: u32 = 64;
/// The length, in characters, of the long `instructions` probe.
const LONG_INSTRUCTIONS_CHARS: usize = 20_000;
const OPEN_READINGS_INSTRUCTIONS_CHARS: usize = 60_000;
/// Present in an output directory once a live run has started sending from it.
const LIVE_MARKER: &str = "live-run.started";

#[derive(Clone, Copy, ValueEnum)]
enum ProbePlan {
    Original,
    OpenReadings,
    /// The original plan's requests 5 and 6 (the 40-clause body and the one-pass control),
    /// unchanged, to compare today's usage with the first run's.
    Controls,
}

impl ProbePlan {
    fn cap(self) -> usize {
        match self {
            Self::Original => MAX_REQUESTS,
            Self::OpenReadings => OPEN_READINGS_MAX_REQUESTS,
            Self::Controls => CONTROL_NAMES.len(),
        }
    }
}

#[derive(Parser)]
#[command(about = "Probe Clairic with a fixed request plan")]
struct Args {
    /// Build, write and print every request body; send nothing (the default).
    #[arg(long, conflicts_with = "live")]
    dry_run: bool,
    /// Fetch the credential and send the plan.
    #[arg(long)]
    live: bool,
    /// The original twelve probes or the eight contract follow-ups.
    #[arg(long, value_enum, default_value = "original")]
    plan: ProbePlan,
    /// Directory for the request bodies, captured replies and summary. Created if missing.
    #[arg(long)]
    out: PathBuf,
    /// The subc daemon connection file.
    #[arg(long)]
    subc: Option<PathBuf>,
    /// Credentials module id on subc.
    #[arg(long, default_value = "claustrum")]
    credentials_module: String,
    /// Per-request timeout, in seconds.
    #[arg(long, default_value_t = 180)]
    request_timeout_secs: u64,
}

// ---------------------------------------------------------------------------------------------
// Ordered JSON building. serde_json's map sorts keys, but one probe asks whether field ORDER
// changes how Clairic splits fields into passes, so every body is written in an explicit order.

enum J {
    Str(String),
    /// JSON text written exactly as given: a number, or a value rendered earlier.
    Raw(String),
    Bool(bool),
    Obj(Vec<(String, J)>),
    Arr(Vec<J>),
}

fn s(text: impl Into<String>) -> J {
    J::Str(text.into())
}

fn n(number: impl ToString) -> J {
    J::Raw(number.to_string())
}

fn o(pairs: Vec<(&str, J)>) -> J {
    J::Obj(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn write_json_string(text: &str, out: &mut String) {
    out.push_str(&serde_json::to_string(text).expect("a string always serializes"));
}

impl J {
    fn write(&self, out: &mut String) {
        match self {
            J::Str(text) => write_json_string(text, out),
            J::Raw(text) => out.push_str(text),
            J::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
            J::Obj(pairs) => {
                out.push('{');
                for (index, (key, value)) in pairs.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    write_json_string(key, out);
                    out.push(':');
                    value.write(out);
                }
                out.push('}');
            }
            J::Arr(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    item.write(out);
                }
                out.push(']');
            }
        }
    }

    fn text(&self) -> String {
        let mut out = String::new();
        self.write(&mut out);
        out
    }
}

/// A JSON object's members in the order they appear, each kept as its exact source text so the
/// summary quotes the provider's bytes rather than a re-encoding of them.
struct Members(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for Members {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MembersVisitor;
        impl<'de> Visitor<'de> for MembersVisitor {
            type Value = Members;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Members, A::Error> {
                let mut members = Vec::new();
                while let Some(entry) = map.next_entry::<String, Box<RawValue>>()? {
                    members.push(entry);
                }
                Ok(Members(members))
            }
        }
        deserializer.deserialize_map(MembersVisitor)
    }
}

impl Members {
    fn parse(text: &str) -> Option<Self> {
        serde_json::from_str(text).ok()
    }

    fn get(&self, key: &str) -> Option<&RawValue> {
        self.0
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_ref())
    }

    fn object(&self, key: &str) -> Option<Members> {
        self.get(key).and_then(|raw| Members::parse(raw.get()))
    }

    fn int(&self, key: &str) -> Option<i64> {
        self.get(key)
            .and_then(|raw| serde_json::from_str::<i64>(raw.get()).ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(name, _)| name.as_str()).collect()
    }
}

// ---------------------------------------------------------------------------------------------
// The plan.

/// One planned request.
struct Probe {
    /// 1-based position in the plan; also the file name prefix.
    number: usize,
    name: &'static str,
    /// The open question from the role draft this request answers.
    question: &'static str,
    state: String,
    /// The `fields` object exactly as sent.
    fields: String,
    /// The request body exactly as sent.
    body: String,
}

impl Probe {
    fn new(
        number: usize,
        name: &'static str,
        question: &'static str,
        state: &str,
        fields: J,
        options: Option<J>,
    ) -> Self {
        Self {
            number,
            name,
            question,
            state: state.to_string(),
            fields: fields.text(),
            body: body_text(state, &fields, options),
        }
    }

    fn with_images(mut self, images: Vec<J>) -> Self {
        self.body.pop(); // Replace the closing brace with the final `images` member.
        let _ = write!(self.body, ",\"images\":{}}}", J::Arr(images).text());
        self
    }

    fn file_stem(&self) -> String {
        format!("{:02}-{}", self.number, self.name)
    }
}

/// Fixed inputs the summary needs to interpret the replies.
struct Fixture {
    span_state: String,
    span_target: &'static str,
    png: Vec<u8>,
}

/// A short purchase-order state shared by the pass, error, long-instruction and image probes,
/// so their usage figures differ only by what each probe changes.
const SHORT_STATE: &str = "Purchase order PO-7731 from Brightwell Interiors, dated 2026-05-04. \
Three standing desks at $419.95 each and two task chairs at $189.50 each, delivered to the \
Leeds office on 2026-05-18. Payment terms: net 45. Contact: Imogen Hart.";

/// The state of the all-types probe: each field has facts that determine it, and the estimate
/// and forecast have history to work from.
const ALL_TYPES_STATE: &str = "Maintenance log, Harbor Pump Station 3. Technician on duty: \
Mara Okafor. The inspection on 2026-03-14 started at 09:15. The pump went offline at 09:15 and \
was restarted at 16:40 the same day, after 7 hours 25 minutes offline. Parts replaced: impeller, \
shaft seal, bearing housing. Measured flow after the repair: 412.5 litres per minute. Monthly \
water throughput in thousand cubic metres: Oct 2025 118, Nov 2025 121, Dec 2025 125, \
Jan 2026 129, Feb 2026 132. The previous two shaft seals lasted 14 and 18 months before they \
needed replacing.";

const DATE_ESTIMATE_STATE: &str =
    "The kitchen renovation began on 2026-02-02. Demolition and plumbing were expected to \
take about six weeks. The cabinets were ordered on 2026-03-01, and the supplier says \
cabinets usually arrive about two months after ordering; fitting takes two more weeks.";
const DATE_MIN: &str = "2026-03-01";
const DATE_MAX: &str = "2027-03-01";
const FORECAST_HORIZON: usize = 3;
const FORECAST_INTERVAL: &str = "month";

fn completion_date_fields(field_type: &str, options: Vec<(&str, J)>) -> J {
    let mut members = vec![
        ("type", s(field_type)),
        ("min", s(DATE_MIN)),
        ("max", s(DATE_MAX)),
    ];
    members.extend(options);
    members.push((
        "instructions",
        s("Date the kitchen renovation will be finished"),
    ));
    o(vec![("completion_date", o(members))])
}

fn contact_field(chars: usize) -> J {
    o(vec![(
        "contact_name",
        o(vec![
            ("type", s("text")),
            ("instructions", s(long_instructions(chars))),
        ]),
    )])
}

fn include_quantiles() -> J {
    o(vec![("include_quantiles", J::Bool(true))])
}

const SPAN_TARGET: &str = "QK-58213";

/// The span probe's state: a precomposed é (two UTF-8 bytes, one UTF-16 unit, one scalar), an
/// emoji outside the Basic Multilingual Plane (four bytes, two UTF-16 units, one scalar) and
/// four CJK characters (three bytes, one unit, one scalar each) all come before the target, so
/// the three offset units give three different start positions.
fn span_state() -> String {
    format!(
        "Note from the caf\u{e9} \u{1F600} \u{6771}\u{4EAC}\u{652F}\u{793E} team: the tracking \
         code is {SPAN_TARGET}, and the parcel shipped on Monday."
    )
}

/// Long field instructions for the multi-pass probe: about 1,200 characters per field, with
/// numbers that differ per field so no two fields read the same.
fn clause_instructions(clause: usize) -> String {
    let mut text = format!(
        "Clause {clause:02} of the purchase-order audit checklist. Return the exact wording the \
         order uses for this clause, copied from the order text."
    );
    let mut rule = 1;
    while text.len() < 1_200 {
        let _ = write!(
            text,
            " Rule {clause}.{rule}: report it only when the order states it in so many words; \
             when the order gives a range, an approximate figure, or nothing at all, return \
             null rather than estimating, and never combine figures that appear on separate \
             lines of the order (audit reference {}-{}).",
            7_000 + clause * 13 + rule,
            clause * 97 + rule * 31
        );
        rule += 1;
    }
    text
}

/// The multi-pass probe's fields, in plan order. Forty fields of about 1,200 characters each
/// make roughly 50 KB of definitions, well over one 8,192-token pass beside a short state.
const CLAUSES: usize = 40;

fn clause_field(clause: usize) -> (String, J) {
    (
        format!("clause_{clause:02}"),
        o(vec![
            ("type", s("text")),
            ("instructions", s(clause_instructions(clause))),
        ]),
    )
}

/// Exactly `chars` ASCII characters of plausible instructions.
fn long_instructions(chars: usize) -> String {
    let mut text = String::from(
        "Return the full name of the person the order names as its contact, exactly as written.",
    );
    let mut sentence = 1;
    while text.len() < chars {
        let _ = write!(
            text,
            " Note {sentence}: if the order names several people, prefer the one introduced \
             with the word Contact; ignore signatures, sales representatives and delivery \
             drivers, and never invent a name that the order does not contain."
        );
        sentence += 1;
    }
    text.truncate(chars);
    text
}

/// A `PNG_SIDE` x `PNG_SIDE` RGB gradient, encoded in code so the probe needs no image file.
fn test_png() -> Vec<u8> {
    rgb_png(PNG_SIDE, |x, y| [(x * 4) as u8, (y * 4) as u8, 128])
}

fn rgb_png(side: u32, pixel: impl Fn(u32, u32) -> [u8; 3]) -> Vec<u8> {
    use flate2::write::ZlibEncoder;
    use flate2::Compression;
    use std::io::Write as _;

    fn chunk(out: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
        out.extend_from_slice(&(data.len() as u32).to_be_bytes());
        out.extend_from_slice(kind);
        out.extend_from_slice(data);
        let mut crc = flate2::Crc::new();
        crc.update(kind);
        crc.update(data);
        out.extend_from_slice(&crc.sum().to_be_bytes());
    }

    let mut raw = Vec::with_capacity(((side * 3 + 1) * side) as usize);
    for y in 0..side {
        raw.push(0); // filter type: none
        for x in 0..side {
            raw.extend_from_slice(&pixel(x, y));
        }
    }
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&raw)
        .expect("writing to a Vec cannot fail");
    let compressed = encoder.finish().expect("finishing into a Vec cannot fail");

    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&side.to_be_bytes());
    ihdr.extend_from_slice(&side.to_be_bytes());
    ihdr.extend_from_slice(&[8, 2, 0, 0, 0]); // 8-bit depth, RGB, deflate, no filter, no interlace
    let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
    chunk(&mut png, b"IHDR", &ihdr);
    chunk(&mut png, b"IDAT", &compressed);
    chunk(&mut png, b"IEND", &[]);
    png
}

fn png_object(png: &[u8]) -> J {
    o(vec![
        ("content_type", s("image/png")),
        (
            "base64",
            s(base64::engine::general_purpose::STANDARD.encode(png)),
        ),
    ])
}

fn body_text(state: &str, fields: &J, options: Option<J>) -> String {
    let mut members = vec![
        ("model", s(MODEL)),
        ("state", s(state)),
        ("fields", fields.text_as_raw()),
    ];
    if let Some(options) = options {
        members.push(("options", options));
    }
    o(members).text()
}

impl J {
    /// The same value, already rendered; lets a body embed a `fields` object written once.
    fn text_as_raw(&self) -> J {
        J::Raw(self.text())
    }
}

fn plan() -> (Vec<Probe>, Fixture) {
    let mut probes = Vec::new();
    let mut add = |name, question, state: &str, fields: J, options: Option<J>| {
        probes.push(Probe::new(
            probes.len() + 1,
            name,
            question,
            state,
            fields,
            options,
        ));
    };
    let both_options = || {
        Some(o(vec![
            ("include_quantiles", J::Bool(true)),
            ("return_state_text", J::Bool(true)),
        ]))
    };

    // 1. Every answer shape, with quantiles and the state text.
    add(
        "all-types",
        "the answer shape of all nine field types, the forecast steps and the quantiles layout",
        ALL_TYPES_STATE,
        o(vec![
            (
                "technician",
                o(vec![
                    ("type", s("text")),
                    ("instructions", s("Name of the technician on duty")),
                ]),
            ),
            (
                "flow_rate",
                o(vec![
                    ("type", s("number")),
                    ("decimals", n(1)),
                    ("unit", s("litres per minute")),
                    ("instructions", s("Measured flow after the repair")),
                ]),
            ),
            (
                "inspection_date",
                o(vec![
                    ("type", s("date")),
                    ("instructions", s("Date of the inspection")),
                ]),
            ),
            (
                "inspection_start",
                o(vec![
                    ("type", s("time")),
                    ("instructions", s("Time the inspection started")),
                ]),
            ),
            (
                "restarted_at",
                o(vec![
                    ("type", s("datetime")),
                    ("instructions", s("Date and time the pump was restarted")),
                ]),
            ),
            (
                "downtime",
                o(vec![
                    ("type", s("duration")),
                    ("unit", s("minutes")),
                    ("instructions", s("How long the pump was offline")),
                ]),
            ),
            (
                "parts_replaced",
                o(vec![
                    ("type", s("list")),
                    ("items", s("text")),
                    ("instructions", s("Parts replaced during the repair")),
                ]),
            ),
            (
                "seal_life_months",
                o(vec![
                    ("type", s("estimate")),
                    ("min", n(1)),
                    ("max", n(60)),
                    ("step", n(1)),
                    (
                        "instructions",
                        s("Months the newly fitted shaft seal will last before it needs replacing"),
                    ),
                ]),
            ),
            (
                "throughput_forecast",
                o(vec![
                    ("type", s("forecast")),
                    ("min", n(50)),
                    ("max", n(300)),
                    ("horizon", n(FORECAST_HORIZON)),
                    ("interval", s(FORECAST_INTERVAL)),
                    (
                        "instructions",
                        s("Monthly water throughput in thousand cubic metres for the months after Feb 2026"),
                    ),
                ]),
            ),
        ]),
        both_options(),
    );

    // 2. Span offsets over non-ASCII text.
    let span_state = span_state();
    add(
        "span-offsets",
        "the span offset unit, whether a span's end is exclusive, and whether state_text equals the input",
        &span_state,
        o(vec![(
            "tracking_code",
            o(vec![
                ("type", s("text")),
                ("instructions", s("The parcel's tracking code")),
            ]),
        )]),
        Some(o(vec![("return_state_text", J::Bool(true))])),
    );

    // 3. `decimals` on a number list. The readings carry two decimals, so rounding shows.
    add(
        "list-decimals",
        "whether decimals applies to a list with items: number",
        "Daily rainfall readings for the week, in millimetres: 2.34, 0.06, 11.75, 4.5 and 7.18.",
        o(vec![(
            "rainfall_mm",
            o(vec![
                ("type", s("list")),
                ("items", s("number")),
                ("decimals", n(1)),
                (
                    "instructions",
                    s("Every daily rainfall reading, in millimetres"),
                ),
            ]),
        )]),
        None,
    );

    // 4. A date estimate: ISO-date bounds and a month step. Quantiles show the date layout.
    add(
        "date-estimate",
        "whether an estimate with ISO-date min/max and step: month exists, and its answer shape",
        DATE_ESTIMATE_STATE,
        completion_date_fields("estimate", vec![("step", s("month"))]),
        Some(include_quantiles()),
    );

    // 5-7. Passes: many long fields, the one-field control, and the same fields reversed.
    let clauses: Vec<(String, J)> = (1..=CLAUSES).map(clause_field).collect();
    let reversed: Vec<(String, J)> = (1..=CLAUSES).rev().map(clause_field).collect();
    add(
        "multi-pass",
        "usage.passes above 1, and whether a second pass bills the state again",
        SHORT_STATE,
        J::Obj(clauses),
        None,
    );
    add(
        "one-pass-control",
        "the one-pass usage baseline for the multi-pass and image probes",
        SHORT_STATE,
        J::Obj(vec![clause_field(1)]),
        None,
    );
    add(
        "multi-pass-reversed",
        "whether field order changes the pass split or the usage",
        SHORT_STATE,
        J::Obj(reversed),
        None,
    );

    // 8. A deliberate 400.
    add(
        "unknown-type",
        "the error envelope and code for an unknown field type",
        SHORT_STATE,
        o(vec![(
            "desk_colour",
            o(vec![
                ("type", s("colour")),
                ("instructions", s("Colour of the standing desks")),
            ]),
        )]),
        None,
    );

    // 9. An unknown option key on a valid field.
    add(
        "unknown-option",
        "whether an unknown option key on a valid field is ignored or refused",
        SHORT_STATE,
        o(vec![(
            "desk_price",
            o(vec![
                ("type", s("number")),
                ("decimalz", n(2)),
                (
                    "instructions",
                    s("Price of one standing desk in US dollars"),
                ),
            ]),
        )]),
        None,
    );

    // 10. A 20,000-character instructions string.
    add(
        "long-instructions",
        "the provider's bound on instructions, and the usage of a long one",
        SHORT_STATE,
        contact_field(LONG_INSTRUCTIONS_CHARS),
        None,
    );

    // 11. An empty state and no images.
    add(
        "empty-state",
        "the error shape for an empty state with no images",
        "",
        o(vec![(
            "contact_name",
            o(vec![
                ("type", s("text")),
                ("instructions", s("Name of the order's contact")),
            ]),
        )]),
        None,
    );
    drop(add);

    // 12. The one-pass control's exact body plus one embedded PNG, appended last, so the only
    // difference from request 6 is the image.
    let png = test_png();
    probes.push(
        Probe::new(
            12,
            "one-image",
            "the input tokens billed for one embedded image",
            SHORT_STATE,
            J::Obj(vec![clause_field(1)]),
            None,
        )
        .with_images(vec![png_object(&png)]),
    );

    (
        probes,
        Fixture {
            span_state,
            span_target: SPAN_TARGET,
            png,
        },
    )
}

fn open_readings_plan() -> Vec<Probe> {
    let png = test_png();
    let control_with_images = |number, name, question, images| {
        Probe::new(
            number,
            name,
            question,
            SHORT_STATE,
            J::Obj(vec![clause_field(1)]),
            None,
        )
        .with_images(images)
    };
    vec![
        Probe::new(
            1,
            "date-year-step",
            "whether a date estimate accepts step: year",
            DATE_ESTIMATE_STATE,
            completion_date_fields("estimate", vec![("step", s("year"))]),
            Some(include_quantiles()),
        ),
        Probe::new(
            2,
            "forecast-date-bounds",
            "whether a forecast accepts ISO-date min/max bounds and returns dates within them",
            DATE_ESTIMATE_STATE,
            completion_date_fields(
                "forecast",
                vec![
                    ("horizon", n(FORECAST_HORIZON)),
                    ("interval", s(FORECAST_INTERVAL)),
                ],
            ),
            Some(include_quantiles()),
        ),
        Probe::new(
            3,
            "multi-pass-image",
            "whether an image bills once or on every pass",
            SHORT_STATE,
            J::Obj((1..=CLAUSES).map(clause_field).collect()),
            None,
        )
        .with_images(vec![png_object(&png)]),
        control_with_images(
            4,
            "image-512",
            "the token cost of one 512x512 PNG",
            vec![png_object(&rgb_png(512, |_, _| [0; 3]))],
        ),
        control_with_images(
            5,
            "image-1024",
            "the token cost of one 1024x1024 PNG",
            vec![png_object(&rgb_png(1024, |_, _| [0; 3]))],
        ),
        control_with_images(
            6,
            "image-data-url",
            "whether a data URL is accepted in place of the image object",
            vec![s(format!(
                "data:image/png;base64,{}",
                base64::engine::general_purpose::STANDARD.encode(&png)
            ))],
        ),
        control_with_images(
            7,
            "two-images",
            "whether one item can carry two images and what they cost",
            vec![
                png_object(&png),
                png_object(&rgb_png(PNG_SIDE, |_, _| [160, 96, 128])),
            ],
        ),
        Probe::new(
            8,
            "long-instructions-60k",
            "whether the provider accepts 60,000 characters of instructions",
            SHORT_STATE,
            contact_field(OPEN_READINGS_INSTRUCTIONS_CHARS),
            None,
        ),
    ]
}

// ---------------------------------------------------------------------------------------------
// Sending.

/// Counts requests and refuses any beyond the selected plan's cap.
struct Budget {
    cap: usize,
    sent: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self::new(MAX_REQUESTS)
    }
}

impl Budget {
    fn new(cap: usize) -> Self {
        Self { cap, sent: 0 }
    }

    /// Claim one request. Errors, sending nothing, once the cap has been reached.
    fn take(&mut self) -> Result<usize, String> {
        if self.sent >= self.cap {
            return Err(format!(
                "refusing request {}: a run sends at most {} requests",
                self.sent + 1,
                self.cap
            ));
        }
        self.sent += 1;
        Ok(self.sent)
    }
}

struct Secret {
    token: String,
    account_id: Option<String>,
}

/// Replaces the token and the account id in anything written or printed, counting each hit.
struct Scrub {
    secrets: Vec<String>,
    redactions: u64,
}

impl Scrub {
    fn new(secret: Option<&Secret>) -> Self {
        let mut secrets = Vec::new();
        if let Some(secret) = secret {
            secrets.push(secret.token.clone());
            secrets.extend(secret.account_id.clone());
        }
        // A very short value would match unrelated text; neither real value is that short.
        secrets.retain(|value| value.len() >= 8);
        Self {
            secrets,
            redactions: 0,
        }
    }

    fn text(&mut self, text: &str) -> String {
        let mut out = text.to_string();
        for secret in &self.secrets {
            if out.contains(secret.as_str()) {
                out = out.replace(secret.as_str(), "<REDACTED>");
                self.redactions += 1;
            }
        }
        out
    }

    fn bytes(&mut self, bytes: &[u8]) -> Vec<u8> {
        match std::str::from_utf8(bytes) {
            Ok(text) => self.text(text).into_bytes(),
            Err(_) => {
                let lossy = String::from_utf8_lossy(bytes);
                if self.secrets.iter().any(|s| lossy.contains(s.as_str())) {
                    self.text(&lossy).into_bytes()
                } else {
                    bytes.to_vec()
                }
            }
        }
    }

    fn write(&mut self, path: &Path, bytes: &[u8]) -> Result<(), String> {
        let bytes = self.bytes(bytes);
        std::fs::write(path, bytes).map_err(|e| format!("write {}: {e}", path.display()))
    }
}

fn default_subc_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".local/share/cortexkit/run/subc-connection.json")
}

async fn fetch_secret(args: &Args, handle: &str) -> Result<Secret, String> {
    let subc = args.subc.clone().unwrap_or_else(default_subc_path);
    let conn = broca_subc::SubcConnection::connect(&subc)
        .await
        .map_err(|e| format!("connect subc ({}): {e}", subc.display()))?;
    // The vault authorizes by the credential handle; subc only needs an existing project root.
    let cwd = std::env::current_dir().map_err(|e| format!("current dir: {e}"))?;
    let identity =
        broca_protocol::BindIdentity::new(cwd, "clairic-live-probe", "clairic-live-probe");
    let client = broca_subc::VaultClient::new(
        Arc::new(conn),
        &args.credentials_module,
        &identity,
        Duration::from_secs(10),
    );
    client
        .get(handle, None, false)
        .await
        .result
        .map(|t| Secret {
            token: t.token,
            account_id: t.account_id,
        })
        .map_err(|e| format!("vault credential.get: {e}"))
}

/// What came back for one request.
struct Capture {
    status: Option<u16>,
    reason: Option<String>,
    headers: Vec<(String, String)>,
    body: Option<Vec<u8>>,
    /// A transport failure, with the URL (which holds the account id) removed. It is scrubbed
    /// for the token and account id when printed or written.
    error: Option<String>,
    elapsed_ms: u128,
}

async fn send(
    client: &broca_provider::ProviderHttpClient,
    url: &str,
    token: &str,
    probe: &Probe,
    timeout: Duration,
) -> Capture {
    let started = std::time::Instant::now();
    // The client that never decodes, so the body is recorded exactly as it arrived.
    let request = client
        .without_response_decoding()
        .post(url)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .timeout(timeout)
        .body(probe.body.clone().into_bytes());
    let mut capture = Capture {
        status: None,
        reason: None,
        headers: Vec::new(),
        body: None,
        error: None,
        elapsed_ms: 0,
    };
    match request.send().await {
        Ok(response) => {
            capture.status = Some(response.status().as_u16());
            capture.reason = response.status().canonical_reason().map(str::to_string);
            capture.headers = response
                .headers()
                .iter()
                .filter(|(name, _)| name.as_str() != "set-cookie")
                .map(|(name, value)| {
                    (
                        name.to_string(),
                        value.to_str().unwrap_or("<non-utf8>").to_string(),
                    )
                })
                .collect();
            match response.bytes().await {
                Ok(bytes) => capture.body = Some(bytes.to_vec()),
                Err(e) => capture.error = Some(format!("read body: {}", e.without_url())),
            }
        }
        Err(e) => capture.error = Some(format!("send: {}", e.without_url())),
    }
    capture.elapsed_ms = started.elapsed().as_millis();
    capture
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// `NN-name.json`: the status line, headers, and the body embedded verbatim when it is JSON.
fn capture_record(probe: &Probe, capture: &Capture) -> String {
    let mut members = vec![
        ("probe", n(probe.number)),
        ("name", s(probe.name)),
        ("question", s(probe.question)),
        ("request_bytes", n(probe.body.len())),
        ("request_sha256", s(sha256_hex(probe.body.as_bytes()))),
        ("elapsed_ms", n(capture.elapsed_ms)),
    ];
    if let Some(status) = capture.status {
        members.push(("http_status", n(status)));
    }
    if let Some(reason) = &capture.reason {
        members.push(("http_reason", s(reason.as_str())));
    }
    members.push((
        "headers",
        J::Obj(
            capture
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), s(value.as_str())))
                .collect(),
        ),
    ));
    if let Some(error) = &capture.error {
        members.push(("transport_error", s(error.as_str())));
    }
    if let Some(body) = &capture.body {
        members.push(("body_bytes", n(body.len())));
        members.push(("body_sha256", s(sha256_hex(body))));
        let text = std::str::from_utf8(body).ok();
        match text.filter(|text| serde_json::from_str::<&RawValue>(text).is_ok()) {
            // Valid JSON is embedded as the exact bytes received; `NN-name.body` holds them too.
            Some(text) => members.push(("body", J::Raw(text.to_string()))),
            None => members.push(("body_text", s(String::from_utf8_lossy(body).into_owned()))),
        }
    }
    let mut out = o(members).text();
    out.push('\n');
    out
}

// ---------------------------------------------------------------------------------------------
// Running.

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("clairic-live-probe: {error}");
        std::process::exit(1);
    }
}

/// Up to `limit` bytes of `text`, cut at a character boundary.
fn preview(text: &str, limit: usize) -> &str {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn print_plan(probes: &[Probe], fixture: Option<&Fixture>, cap: usize) {
    println!(
        "{} requests planned (cap {cap}); model {MODEL}",
        probes.len()
    );
    for probe in probes {
        println!();
        println!(
            "[{:02}] {} -- {} bytes (state {} bytes, fields {} bytes), sha256 {}",
            probe.number,
            probe.name,
            probe.body.len(),
            probe.state.len(),
            probe.fields.len(),
            sha256_hex(probe.body.as_bytes())
        );
        println!("     answers: {}", probe.question);
        if matches!(
            probe.name,
            "multi-pass" | "multi-pass-reversed" | "multi-pass-image"
        ) {
            println!(
                "     rough size check: fields {} bytes / 4 = about {} tokens, against a {PASS_TOKENS}-token pass",
                probe.fields.len(),
                probe.fields.len() / 4
            );
        }
        if let Some(fixture) = fixture.filter(|_| probe.name == "one-image") {
            println!(
                "     image: one {PNG_SIDE}x{PNG_SIDE} RGB PNG, {} bytes; the body is request 6's body plus `images`",
                fixture.png.len()
            );
        }
        const FULL_LIMIT: usize = 1_500;
        if probe.body.len() <= FULL_LIMIT {
            println!("     body: {}", probe.body);
        } else {
            let shown = preview(&probe.body, 600);
            println!(
                "     body: {shown}... [{} more bytes; full body in {}.request.json]",
                probe.body.len() - shown.len(),
                probe.file_stem()
            );
        }
    }
}

fn write_requests(out: &Path, probes: &[Probe], scrub: &mut Scrub) -> Result<(), String> {
    for probe in probes {
        scrub.write(
            &out.join(format!("{}.request.json", probe.file_stem())),
            probe.body.as_bytes(),
        )?;
    }
    Ok(())
}

fn utc_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default();
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|time| time.to_rfc3339())
        .unwrap_or_else(|| secs.to_string())
}

async fn run(args: Args) -> Result<(), String> {
    let (probes, fixture) = match args.plan {
        ProbePlan::Original => {
            let (probes, fixture) = plan();
            (probes, Some(fixture))
        }
        ProbePlan::OpenReadings => (open_readings_plan(), None),
        ProbePlan::Controls => (controls_plan(), None),
    };
    let cap = args.plan.cap();
    if probes.len() > cap {
        return Err(format!(
            "the plan has {} requests; the cap is {cap}",
            probes.len()
        ));
    }
    let credential =
        CredentialId::parse(CREDENTIAL_ID).ok_or("the fixed credential id does not parse")?;
    // Both modes look the handle up first, so a dry run also proves the live run will find it.
    // The handle is itself a bearer secret and is never printed.
    let handle = load_devtools_handle(&credential)?;
    println!("credential {CREDENTIAL_ID}: handle present in the dev-tools handle file");
    std::fs::create_dir_all(&args.out)
        .map_err(|e| format!("create {}: {e}", args.out.display()))?;

    if !args.live {
        drop(handle);
        let mut scrub = Scrub::new(None);
        write_requests(&args.out, &probes, &mut scrub)?;
        print_plan(&probes, fixture.as_ref(), cap);
        println!();
        println!(
            "dry run: 0 requests sent, vault not contacted; request bodies written to {}",
            args.out.display()
        );
        return Ok(());
    }

    let marker = args.out.join(LIVE_MARKER);
    if marker.exists() {
        return Err(format!(
            "{} exists: a live run already sent from this directory; pass a fresh --out",
            marker.display()
        ));
    }
    let secret = fetch_secret(&args, &handle).await?;
    drop(handle);
    let account_present = secret
        .account_id
        .as_deref()
        .is_some_and(|account| !account.trim().is_empty());
    println!("account id present: {account_present}");
    let Some(account_id) = secret.account_id.clone().filter(|_| account_present) else {
        return Err(
            "the vault returned no account id with the token, so there is no endpoint to call; \
             0 requests sent"
                .into(),
        );
    };
    let mut scrub = Scrub::new(Some(&secret));
    scrub.write(
        &marker,
        format!(
            "live run started {}; {} requests planned\n",
            utc_now(),
            probes.len()
        )
        .as_bytes(),
    )?;
    write_requests(&args.out, &probes, &mut scrub)?;

    let url = format!("https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/{MODEL}");
    let client = broca_provider::build_provider_http_client();
    let timeout = Duration::from_secs(args.request_timeout_secs);
    let mut budget = Budget::new(cap);
    let mut captures = Vec::with_capacity(probes.len());
    for probe in &probes {
        let count = budget.take()?;
        let capture = send(&client, &url, &secret.token, probe, timeout).await;
        let stem = probe.file_stem();
        if let Some(body) = &capture.body {
            scrub.write(&args.out.join(format!("{stem}.body")), body)?;
        }
        scrub.write(
            &args.out.join(format!("{stem}.json")),
            capture_record(probe, &capture).as_bytes(),
        )?;
        let outcome = match (&capture.status, &capture.error) {
            (Some(status), None) => format!("HTTP {status}"),
            (Some(status), Some(error)) => format!("HTTP {status}, {}", scrub.text(error)),
            (None, Some(error)) => scrub.text(error),
            (None, None) => "no reply".into(),
        };
        println!(
            "[{count:02}/{cap}] {}: {outcome}, {} reply bytes, {} ms -> {stem}.json",
            probe.name,
            capture.body.as_ref().map_or(0, Vec::len),
            capture.elapsed_ms
        );
        captures.push(capture);
    }
    let sent = budget.sent;
    let mut summary = match (fixture, args.plan) {
        (Some(fixture), _) => summarize(&probes, &captures, &fixture, sent, account_present),
        (None, ProbePlan::Controls) => {
            summarize_controls(&probes, &captures, sent, account_present)
        }
        (None, _) => summarize_open_readings(&probes, &captures, sent, account_present),
    };
    // Count the summary's own redactions too, then state the final total in it.
    summary = scrub.text(&summary);
    let _ = writeln!(
        summary,
        "\nRedactions of the token or account id across every file written: {}.",
        scrub.redactions
    );
    scrub.write(&args.out.join("summary.md"), summary.as_bytes())?;
    println!(
        "live run: {sent} requests sent; summary at {}",
        args.out.join("summary.md").display()
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The summary. Every figure is read from the captured bytes; a figure a reply does not carry is
// written "unmeasured", never 0.

/// One captured reply, parsed only as far as the summary needs.
struct Reply<'a> {
    probe: &'a Probe,
    capture: &'a Capture,
    envelope: Option<Members>,
    /// The `result` member, or the whole body when it is not enveloped but carries `answers`.
    result: Option<Members>,
    result_location: &'static str,
}

impl<'a> Reply<'a> {
    fn new(probe: &'a Probe, capture: &'a Capture) -> Self {
        let text = capture
            .body
            .as_deref()
            .and_then(|body| std::str::from_utf8(body).ok());
        let envelope = text.and_then(Members::parse);
        let (result, result_location) = match envelope.as_ref() {
            Some(envelope) => match envelope.object("result") {
                Some(result) => (Some(result), "`result` member of the envelope"),
                None if envelope.get("answers").is_some() => {
                    (text.and_then(Members::parse), "top level (no envelope)")
                }
                None => (None, "absent"),
            },
            None => (None, "absent (body is not a JSON object)"),
        };
        Self {
            probe,
            capture,
            envelope,
            result,
            result_location,
        }
    }

    fn status(&self) -> String {
        match (self.capture.status, &self.capture.reason) {
            (Some(status), Some(reason)) => format!("HTTP {status} {reason}"),
            (Some(status), None) => format!("HTTP {status}"),
            (None, _) => format!(
                "no HTTP status ({})",
                self.capture.error.as_deref().unwrap_or("no reply")
            ),
        }
    }

    fn answers(&self) -> Option<Members> {
        self.result
            .as_ref()
            .and_then(|result| result.object("answers"))
    }

    fn answer(&self, field: &str) -> Option<Members> {
        self.answers().and_then(|answers| answers.object(field))
    }

    fn answer_raw(&self, field: &str) -> Option<String> {
        self.answers()
            .and_then(|answers| answers.get(field).map(|raw| raw.get().to_string()))
    }

    fn usage(&self) -> Option<Members> {
        self.result
            .as_ref()
            .and_then(|result| result.object("usage"))
    }

    fn usage_int(&self, key: &str) -> Option<i64> {
        self.usage().and_then(|usage| usage.int(key))
    }

    fn envelope_raw(&self, key: &str) -> String {
        self.envelope
            .as_ref()
            .and_then(|envelope| envelope.get(key))
            .map_or_else(|| "(absent)".to_string(), |raw| format!("`{}`", raw.get()))
    }
}

fn fig(value: Option<i64>) -> String {
    value.map_or_else(|| "unmeasured".to_string(), |value| value.to_string())
}

fn sum(values: &[Option<i64>]) -> Option<i64> {
    values.iter().copied().sum()
}

/// Digits after the decimal point of a JSON number's text; `None` for exponent forms.
fn decimals_of(number: &str) -> Option<usize> {
    let number = number.trim();
    if number.contains(['e', 'E']) {
        return None;
    }
    Some(
        number
            .split_once('.')
            .map_or(0, |(_, fraction)| fraction.len()),
    )
}

/// The kind of a JSON value, from its first character.
fn kind_of(raw: &str) -> &'static str {
    match raw.trim_start().chars().next() {
        Some('[') => "array",
        Some('{') => "object",
        Some('"') => "string",
        Some('t' | 'f') => "boolean",
        Some('n') => "null",
        Some(_) => "number",
        None => "empty",
    }
}

/// Start and exclusive end of `target` in `text` under UTF-8 bytes, UTF-16 units and scalars.
fn offsets(text: &str, target: &str) -> Option<[(&'static str, usize, usize); 3]> {
    let byte = text.find(target)?;
    let before = &text[..byte];
    let utf16 = before.encode_utf16().count();
    let scalars = before.chars().count();
    Some([
        ("UTF-8 bytes", byte, byte + target.len()),
        ("UTF-16 units", utf16, utf16 + target.encode_utf16().count()),
        ("Unicode scalars", scalars, scalars + target.chars().count()),
    ])
}

fn code_block(out: &mut String, text: &str) {
    let _ = writeln!(out, "```json\n{text}\n```");
}

fn summarize(
    probes: &[Probe],
    captures: &[Capture],
    fixture: &Fixture,
    sent: usize,
    account_present: bool,
) -> String {
    let replies: Vec<Reply> = probes
        .iter()
        .zip(captures)
        .map(|(probe, capture)| Reply::new(probe, capture))
        .collect();
    let by_name = |name: &str| replies.iter().find(|reply| reply.probe.name == name);
    let mut out = String::new();
    let w = &mut out;

    let _ = writeln!(w, "# Clairic live probe: captured replies\n");
    let _ = writeln!(
        w,
        "Run finished {}. Model `{MODEL}`. Requests sent: {sent} of a {MAX_REQUESTS}-request cap. \
         No request was retried; request 12 was planned as the image probe, not a spare. \
         Account id returned by the vault with the token: {account_present}.\n",
        utc_now()
    );
    let _ = writeln!(
        w,
        "Each `NN-name.body` file holds a reply body exactly as received; `NN-name.json` adds \
         the HTTP status and headers; `NN-name.request.json` is the exact body sent. Quotes \
         below are the provider's own bytes.\n"
    );
    let _ = writeln!(
        w,
        "| # | probe | request bytes | status | reply bytes | passes | input_tokens |\n|---|---|---|---|---|---|---|"
    );
    for reply in &replies {
        let _ = writeln!(
            w,
            "| {} | {} | {} | {} | {} | {} | {} |",
            reply.probe.number,
            reply.probe.name,
            reply.probe.body.len(),
            reply.status(),
            reply
                .capture
                .body
                .as_ref()
                .map_or_else(|| "none".into(), |body| body.len().to_string()),
            fig(reply.usage_int("passes")),
            fig(reply.usage_int("input_tokens")),
        );
    }

    // The envelope.
    let _ = writeln!(
        w,
        "\n## REST envelope (draft §7: is it exactly `{{result, success, errors, messages}}`?)\n"
    );
    for reply in &replies {
        let keys = reply.envelope.as_ref().map_or_else(
            || "(not a JSON object)".to_string(),
            |e| e.keys().join(", "),
        );
        let result_keys = reply
            .result
            .as_ref()
            .map_or_else(|| "none".to_string(), |r| r.keys().join(", "));
        let _ = writeln!(
            w,
            "- {:02} {}: {}; top-level members in order: `{keys}`; `success` {}; `errors` {}; \
             `messages` {}; answer object found at: {}; its members: `{result_keys}`.",
            reply.probe.number,
            reply.probe.name,
            reply.status(),
            reply.envelope_raw("success"),
            reply.envelope_raw("errors"),
            reply.envelope_raw("messages"),
            reply.result_location,
        );
    }

    section_all_types(w, by_name("all-types"));
    section_spans(w, by_name("span-offsets"), fixture);
    section_list_decimals(w, by_name("list-decimals"));
    section_date_estimate(w, by_name("date-estimate"));
    section_passes(
        w,
        by_name("multi-pass"),
        by_name("one-pass-control"),
        by_name("multi-pass-reversed"),
    );
    section_errors(w, &replies);
    section_long_instructions(w, by_name("long-instructions"), by_name("one-pass-control"));
    section_image(
        w,
        by_name("one-image"),
        by_name("one-pass-control"),
        fixture,
    );
    section_overhead(w, &replies);

    let _ = writeln!(w, "\n## Usage blocks as returned\n");
    for reply in &replies {
        let usage = reply
            .result
            .as_ref()
            .and_then(|result| result.get("usage"))
            .map_or_else(
                || "unmeasured (no usage block)".to_string(),
                |raw| format!("`{}`", raw.get()),
            );
        let _ = writeln!(
            w,
            "- {:02} {}: {usage}",
            reply.probe.number, reply.probe.name
        );
    }
    out
}

fn unanswered(w: &mut String, reply: Option<&Reply>) -> bool {
    match reply {
        Some(reply) if reply.answers().is_some() => false,
        Some(reply) => {
            let _ = writeln!(
                w,
                "Unmeasured: {} carried no answers. Body:",
                reply.status()
            );
            let body = reply
                .capture
                .body
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_default();
            code_block(w, &body);
            true
        }
        None => {
            let _ = writeln!(w, "Unmeasured: the request was not sent.");
            true
        }
    }
}

fn raw_list(raw: &RawValue) -> Option<Vec<Box<RawValue>>> {
    serde_json::from_str(raw.get()).ok()
}

/// One line describing a `quantiles` array: its length, element kind and end elements.
fn describe_quantiles(raw: &RawValue) -> String {
    match raw_list(raw) {
        Some(items) if !items.is_empty() => {
            let first: Vec<&str> = items.iter().take(3).map(|item| item.get()).collect();
            format!(
                "{} elements of kind {}; first three `{}`; last `{}`",
                items.len(),
                kind_of(items[0].get()),
                first.join(", "),
                items[items.len() - 1].get()
            )
        }
        Some(_) => "an empty array".into(),
        None => format!("not an array: `{}`", raw.get()),
    }
}

fn state_text_comparison(sent: &str, result: Option<&Members>) -> String {
    let Some(raw) = result.and_then(|result| result.get("state_text")) else {
        return "unmeasured: the reply has no `state_text`".into();
    };
    let Ok(returned) = serde_json::from_str::<String>(raw.get()) else {
        return format!("`state_text` is not a string: `{}`", raw.get());
    };
    if returned == sent {
        return format!(
            "`state_text` equals the sent state byte for byte ({} bytes)",
            sent.len()
        );
    }
    let first_difference = sent
        .bytes()
        .zip(returned.bytes())
        .position(|(a, b)| a != b)
        .unwrap_or(sent.len().min(returned.len()));
    format!(
        "`state_text` DIFFERS from the sent state: sent {} bytes, returned {} bytes, first \
         difference at byte {first_difference}; returned text as JSON: `{}`",
        sent.len(),
        returned.len(),
        raw.get()
    )
}

fn section_all_types(w: &mut String, reply: Option<&Reply>) {
    let _ = writeln!(
        w,
        "\n## 1. Every answer shape (draft §5: `time`, `datetime`, `duration`, forecast `steps`, `quantiles` layout)\n"
    );
    if unanswered(w, reply) {
        return;
    }
    let reply = reply.expect("checked by unanswered");
    let answers = reply.answers().expect("checked by unanswered");
    let _ = writeln!(
        w,
        "Requested with `include_quantiles: true` and `return_state_text: true`. `truncated`: {}. {}.\n",
        reply
            .result
            .as_ref()
            .and_then(|r| r.get("truncated"))
            .map_or_else(|| "(absent)".to_string(), |raw| format!("`{}`", raw.get())),
        state_text_comparison(&reply.probe.state, reply.result.as_ref()),
    );
    for (field, raw) in &answers.0 {
        let members = Members::parse(raw.get());
        let kind = members
            .as_ref()
            .and_then(|m| m.get("type"))
            .map_or_else(|| "(no type)".to_string(), |t| t.get().to_string());
        let keys = members
            .as_ref()
            .map_or_else(String::new, |m| m.keys().join(", "));
        let _ = writeln!(
            w,
            "`{field}`: type {kind}; members in order `{keys}`; value kind {}.",
            members
                .as_ref()
                .and_then(|m| m.get("value"))
                .map_or("absent", |v| kind_of(v.get()))
        );
        if let Some(quantiles) = members.as_ref().and_then(|m| m.get("quantiles")) {
            let _ = writeln!(w, "  - `quantiles`: {}.", describe_quantiles(quantiles));
        }
        if let Some(steps) = members.as_ref().and_then(|m| m.get("steps")) {
            match raw_list(steps) {
                Some(items) => {
                    let _ = writeln!(
                        w,
                        "  - `steps`: {} entries (horizon requested: 3).",
                        items.len()
                    );
                    for (index, step) in items.iter().enumerate() {
                        let step_members = Members::parse(step.get());
                        let _ = writeln!(
                            w,
                            "  - step {index}: kind {}; members `{}`{}.",
                            kind_of(step.get()),
                            step_members
                                .as_ref()
                                .map_or_else(String::new, |m| m.keys().join(", ")),
                            step_members
                                .as_ref()
                                .and_then(|m| m.get("quantiles"))
                                .map_or_else(String::new, |q| format!(
                                    "; its `quantiles`: {}",
                                    describe_quantiles(q)
                                )),
                        );
                    }
                }
                None => {
                    let _ = writeln!(w, "  - `steps` is not an array: `{}`.", steps.get());
                }
            }
        }
        code_block(w, raw.get());
    }
}

fn section_spans(w: &mut String, reply: Option<&Reply>, fixture: &Fixture) {
    let _ = writeln!(
        w,
        "\n## 2. Span offsets (draft §5: unit, exclusive end, `state_text` versus the input)\n"
    );
    let target = fixture.span_target;
    let _ = writeln!(
        w,
        "Sent state (as JSON): `{}`. Target `{target}`, preceded by a precomposed é, the emoji \
         U+1F600 (outside the BMP) and four CJK characters.\n",
        serde_json::to_string(&fixture.span_state).unwrap_or_default()
    );
    if let Some(expected) = offsets(&fixture.span_state, target) {
        let _ = writeln!(
            w,
            "| unit | expected start | expected end, exclusive | expected end, inclusive |\n|---|---|---|---|"
        );
        for (unit, start, end) in expected {
            let _ = writeln!(w, "| {unit} | {start} | {end} | {} |", end - 1);
        }
        let _ = writeln!(w);
    }
    if unanswered(w, reply) {
        return;
    }
    let reply = reply.expect("checked by unanswered");
    let raw = reply.answer_raw("tracking_code").unwrap_or_default();
    let _ = writeln!(w, "Answer `tracking_code` as returned:");
    code_block(w, &raw);
    let span: Option<Vec<i64>> = reply
        .answer("tracking_code")
        .and_then(|answer| answer.get("span").map(|s| s.get().to_string()))
        .and_then(|text| serde_json::from_str(&text).ok());
    let returned_state: Option<String> = reply
        .result
        .as_ref()
        .and_then(|result| result.get("state_text"))
        .and_then(|raw| serde_json::from_str(raw.get()).ok());
    let _ = writeln!(
        w,
        "{}.\n",
        state_text_comparison(&fixture.span_state, reply.result.as_ref())
    );
    let Some(span) = span.filter(|span| span.len() == 2) else {
        let _ = writeln!(w, "Unmeasured: the answer has no two-element `span`.");
        return;
    };
    let (start, end) = (span[0], span[1]);
    let mut texts = vec![("the sent state", fixture.span_state.clone())];
    if let Some(returned) = returned_state.filter(|text| *text != fixture.span_state) {
        texts.push(("the returned state_text", returned));
    }
    for (label, text) in texts {
        let Some(expected) = offsets(&text, target) else {
            let _ = writeln!(w, "- The target does not occur in {label}.");
            continue;
        };
        let mut matched = Vec::new();
        for (unit, expected_start, expected_end) in expected {
            let (expected_start, expected_end) = (expected_start as i64, expected_end as i64);
            if start == expected_start && end == expected_end {
                matched.push(format!("{unit}, end exclusive"));
            } else if start == expected_start && end == expected_end - 1 {
                matched.push(format!("{unit}, end inclusive"));
            }
        }
        let verdict = if matched.is_empty() {
            "matches no unit".to_string()
        } else {
            format!("matches {}", matched.join(" and "))
        };
        let _ = writeln!(
            w,
            "- Returned span `[{start}, {end}]` against {label}: {verdict}."
        );
    }
}

fn section_list_decimals(w: &mut String, reply: Option<&Reply>) {
    let _ = writeln!(
        w,
        "\n## 3. `decimals` on a number list (draft §4 option table)\n"
    );
    let _ = writeln!(
        w,
        "Sent `items: number` with `decimals: 1`; the state's readings are 2.34, 0.06, 11.75, \
         4.5 and 7.18, so values with two decimals mean the option was not applied.\n"
    );
    if unanswered(w, reply) {
        return;
    }
    let reply = reply.expect("checked by unanswered");
    code_block(w, &reply.answer_raw("rainfall_mm").unwrap_or_default());
    let values = reply
        .answer("rainfall_mm")
        .and_then(|answer| answer.get("value").and_then(raw_list));
    match values {
        Some(values) => {
            let digits: Vec<String> = values
                .iter()
                .map(|value| match decimals_of(value.get()) {
                    Some(count) => format!("`{}` ({count})", value.get()),
                    None => format!("`{}` (exponent form)", value.get()),
                })
                .collect();
            let all_within = values
                .iter()
                .all(|value| decimals_of(value.get()).is_some_and(|count| count <= 1));
            let _ = writeln!(
                w,
                "Elements and their decimal digits: {}. Verdict: {}.",
                digits.join(", "),
                if all_within {
                    "every element has at most one decimal, so `decimals` applies to a number list"
                } else {
                    "some element has more than one decimal, so `decimals` was not applied"
                }
            );
        }
        None => {
            let _ = writeln!(w, "Unmeasured: `value` is not an array.");
        }
    }
}

fn section_date_estimate(w: &mut String, reply: Option<&Reply>) {
    let _ = writeln!(
        w,
        "\n## 4. Date estimate (draft §4: does an estimate with ISO-date bounds exist?)\n"
    );
    let _ = writeln!(
        w,
        "Sent `type: estimate`, `min: \"2026-03-01\"`, `max: \"2027-03-01\"`, `step: \"month\"`, \
         with `include_quantiles: true`.\n"
    );
    if unanswered(w, reply) {
        return;
    }
    let reply = reply.expect("checked by unanswered");
    code_block(w, &reply.answer_raw("completion_date").unwrap_or_default());
    if let Some(answer) = reply.answer("completion_date") {
        for member in ["value", "median", "p10", "p90"] {
            if let Some(raw) = answer.get(member) {
                let _ = writeln!(w, "- `{member}`: `{}` ({}).", raw.get(), kind_of(raw.get()));
            }
        }
        if let Some(quantiles) = answer.get("quantiles") {
            let _ = writeln!(w, "- `quantiles`: {}.", describe_quantiles(quantiles));
        }
    }
}

const USAGE_KEYS: [&str; 6] = [
    "passes",
    "state_tokens",
    "schema_tokens",
    "image_tokens",
    "input_tokens",
    "output_tokens",
];

fn section_passes(
    w: &mut String,
    multi: Option<&Reply>,
    control: Option<&Reply>,
    reversed: Option<&Reply>,
) {
    let _ = writeln!(
        w,
        "\n## 5-7. Passes: is the state billed again on a second pass, and does field order matter? (draft §4, §5)\n"
    );
    let _ = write!(w, "| usage member |");
    let rows = [
        ("05 multi-pass", multi),
        ("06 one-pass-control", control),
        ("07 multi-pass-reversed", reversed),
    ];
    for (label, _) in rows {
        let _ = write!(w, " {label} |");
    }
    let _ = writeln!(w, "\n|---|---|---|---|");
    let _ = writeln!(
        w,
        "| fields sent | {} |",
        rows.iter()
            .map(|(_, reply)| reply.map_or_else(
                || "-".into(),
                |r| {
                    Members::parse(&r.probe.fields)
                        .map_or(0, |m| m.0.len())
                        .to_string()
                }
            ))
            .collect::<Vec<_>>()
            .join(" | ")
    );
    for key in USAGE_KEYS {
        let _ = writeln!(
            w,
            "| `{key}` | {} |",
            rows.iter()
                .map(|(_, reply)| fig(reply.and_then(|r| r.usage_int(key))))
                .collect::<Vec<_>>()
                .join(" | ")
        );
    }
    let _ = writeln!(w);

    let get = |reply: Option<&Reply>, key: &str| reply.and_then(|r| r.usage_int(key));
    let control_passes = get(control, "passes");
    let template = sum(&[
        get(control, "input_tokens"),
        get(control, "schema_tokens").map(|v| -v),
        get(control, "state_tokens").map(|v| -v),
        get(control, "image_tokens").map(|v| -v).or(Some(0)),
    ]);
    let _ = writeln!(
        w,
        "Control (06) passes: {}. Its fixed overhead T = input − schema − state − image = {} (an absent `image_tokens` counts as 0 in this arithmetic only, since none of these requests carries an image).\n",
        fig(control_passes),
        fig(template)
    );
    for (label, reply) in [("05", multi), ("07", reversed)] {
        let (Some(passes), Some(schema), Some(state), Some(input), Some(template)) = (
            get(reply, "passes"),
            get(reply, "schema_tokens"),
            get(reply, "state_tokens"),
            get(reply, "input_tokens"),
            template,
        ) else {
            let _ = writeln!(
                w,
                "- {label}: unmeasured (a usage member it needs is missing from {label} or 06)."
            );
            continue;
        };
        let image = get(reply, "image_tokens").unwrap_or(0);
        let hypotheses = [
            (
                "state and overhead billed on every pass",
                schema + image + passes * (state + template),
            ),
            (
                "state once, overhead on every pass",
                schema + image + state + passes * template,
            ),
            (
                "state on every pass, overhead once",
                schema + image + passes * state + template,
            ),
            ("state and overhead once", schema + image + state + template),
        ];
        let _ = writeln!(w, "- {label}: passes {passes}, input_tokens {input}.");
        for (hypothesis, predicted) in hypotheses {
            let _ = writeln!(
                w,
                "  - {hypothesis}: predicts {predicted}{}.",
                if predicted == input {
                    " -- EXACT MATCH"
                } else {
                    ""
                }
            );
        }
    }

    // Field order: compare 05 and 07 member by member.
    if let (Some(multi), Some(reversed)) = (multi, reversed) {
        let mut differing_usage = Vec::new();
        for key in USAGE_KEYS {
            let (a, b) = (multi.usage_int(key), reversed.usage_int(key));
            if a != b {
                differing_usage.push(format!("`{key}` {} vs {}", fig(a), fig(b)));
            }
        }
        let _ = writeln!(
            w,
            "\nField order (05 versus 07): usage {}.",
            if differing_usage.is_empty() {
                "is identical in every member".to_string()
            } else {
                format!("differs: {}", differing_usage.join("; "))
            }
        );
        if let (Some(a), Some(b)) = (multi.answers(), reversed.answers()) {
            let mut differing = Vec::new();
            for (field, raw) in &a.0 {
                let value_a = Members::parse(raw.get())
                    .and_then(|m| m.get("value").map(|v| v.get().to_string()));
                let value_b = b
                    .object(field)
                    .and_then(|m| m.get("value").map(|v| v.get().to_string()));
                if value_a != value_b {
                    differing.push(format!(
                        "`{field}` {} vs {}",
                        value_a.unwrap_or_else(|| "(absent)".into()),
                        value_b.unwrap_or_else(|| "(absent)".into())
                    ));
                }
            }
            let _ = writeln!(
                w,
                "Answer values: {} of {} fields differ{}. Which pass a field ran in is not reported \
                 by the provider, so pass assignment itself is only visible through these figures.",
                differing.len(),
                a.0.len(),
                if differing.is_empty() {
                    String::new()
                } else {
                    format!(": {}", differing.join("; "))
                }
            );
        }
    }
}

fn section_errors(w: &mut String, replies: &[Reply]) {
    let _ = writeln!(
        w,
        "\n## 8, 9, 11. Error shapes (draft §4 unknown type, §4 unknown option, §3 empty state, §7-8 codes)\n"
    );
    for name in ["unknown-type", "unknown-option", "empty-state"] {
        let Some(reply) = replies.iter().find(|reply| reply.probe.name == name) else {
            continue;
        };
        let _ = writeln!(
            w,
            "- {:02} {}: {}; `success` {}; `errors` {}; `messages` {}.",
            reply.probe.number,
            name,
            reply.status(),
            reply.envelope_raw("success"),
            reply.envelope_raw("errors"),
            reply.envelope_raw("messages"),
        );
        if name == "unknown-option" {
            let verdict = match (reply.capture.status, reply.answers().is_some()) {
                (Some(200), true) => {
                    "accepted: the unknown key `decimalz` was ignored without a refusal".to_string()
                }
                (Some(status), _) if status >= 400 => {
                    format!("refused with HTTP {status}")
                }
                _ => "unmeasured".to_string(),
            };
            let _ = writeln!(w, "  - Verdict: {verdict}.");
            if let Some(raw) = reply.answer_raw("desk_price") {
                let _ = writeln!(w, "  - Answer as returned: `{raw}`");
            }
        }
        let body = reply
            .capture
            .body
            .as_deref()
            .map(String::from_utf8_lossy)
            .unwrap_or_default();
        code_block(w, &body);
    }
}

fn section_long_instructions(w: &mut String, reply: Option<&Reply>, control: Option<&Reply>) {
    let _ = writeln!(
        w,
        "\n## 10. A {LONG_INSTRUCTIONS_CHARS}-character `instructions` (draft §4: the provider's bound)\n"
    );
    let Some(reply) = reply else {
        let _ = writeln!(w, "Unmeasured: the request was not sent.");
        return;
    };
    let accepted = reply.capture.status == Some(200) && reply.answers().is_some();
    let _ = writeln!(
        w,
        "{}: {}. `errors` {}.",
        reply.status(),
        if accepted { "accepted" } else { "not answered" },
        reply.envelope_raw("errors")
    );
    if accepted {
        let schema = reply.usage_int("schema_tokens");
        let control_schema = control.and_then(|c| c.usage_int("schema_tokens"));
        let _ = writeln!(
            w,
            "schema_tokens {} (the one-field control with short instructions: {}); passes {}; \
             input_tokens {}. Answer: `{}`",
            fig(schema),
            fig(control_schema),
            fig(reply.usage_int("passes")),
            fig(reply.usage_int("input_tokens")),
            reply.answer_raw("contact_name").unwrap_or_default()
        );
    } else {
        let body = reply
            .capture
            .body
            .as_deref()
            .map(String::from_utf8_lossy)
            .unwrap_or_default();
        code_block(w, &body);
    }
}

fn section_image(
    w: &mut String,
    reply: Option<&Reply>,
    control: Option<&Reply>,
    fixture: &Fixture,
) {
    let _ = writeln!(w, "\n## 12. Per-image token cost\n");
    let _ = writeln!(
        w,
        "Request 12 is request 6's exact body plus `images` holding one {PNG_SIDE}x{PNG_SIDE} RGB \
         PNG of {} bytes ({} base64 characters). Only this one image size was measured; a \
         larger image is unmeasured.\n",
        fixture.png.len(),
        fixture.png.len().div_ceil(3) * 4
    );
    if unanswered(w, reply) {
        return;
    }
    let reply = reply.expect("checked by unanswered");
    let input = reply.usage_int("input_tokens");
    let control_input = control.and_then(|c| c.usage_int("input_tokens"));
    let _ = writeln!(
        w,
        "`image_tokens` reported: {}. input_tokens with the image {} versus {} without it: \
         difference {}.",
        fig(reply.usage_int("image_tokens")),
        fig(input),
        fig(control_input),
        fig(sum(&[input, control_input.map(|v| -v)]))
    );
}

fn section_overhead(w: &mut String, replies: &[Reply]) {
    let _ = writeln!(w, "\n## Template overhead per request\n");
    let _ = writeln!(
        w,
        "Two figures per reply. Tokens: `input_tokens − (state_tokens + schema_tokens + \
         image_tokens)`, every term as reported. Bytes: `input_tokens −` (UTF-8 bytes of the \
         state + UTF-8 bytes of the `fields` object as sent), mixing units as asked, so it shows \
         how far the byte count is from what is billed. A missing usage member makes the figure \
         unmeasured.\n"
    );
    let _ = writeln!(
        w,
        "| # | probe | passes | input_tokens | tokens overhead | state + fields bytes | bytes figure |\n|---|---|---|---|---|---|---|"
    );
    for reply in replies {
        if reply.usage().is_none() {
            let _ = writeln!(
                w,
                "| {} | {} | unmeasured | unmeasured | unmeasured | {} | unmeasured |",
                reply.probe.number,
                reply.probe.name,
                reply.probe.state.len() + reply.probe.fields.len()
            );
            continue;
        }
        let input = reply.usage_int("input_tokens");
        let tokens = sum(&[
            input,
            reply.usage_int("state_tokens").map(|v| -v),
            reply.usage_int("schema_tokens").map(|v| -v),
            reply.usage_int("image_tokens").map(|v| -v),
        ]);
        let bytes = (reply.probe.state.len() + reply.probe.fields.len()) as i64;
        let _ = writeln!(
            w,
            "| {} | {} | {} | {} | {} | {bytes} | {} |",
            reply.probe.number,
            reply.probe.name,
            fig(reply.usage_int("passes")),
            fig(input),
            fig(tokens),
            fig(input.map(|input| input - bytes)),
        );
    }
}

/// Report the status and raw evidence before drawing any conclusion from the reply.
fn open_reading_answer(w: &mut String, reply: Option<&Reply>, field: &str) -> Option<Members> {
    let Some(reply) = reply else {
        let _ = writeln!(w, "Not answered: no reply was captured.");
        return None;
    };
    let _ = writeln!(
        w,
        "{}; `success` {}; `errors` {}; `messages` {}.",
        reply.status(),
        reply.envelope_raw("success"),
        reply.envelope_raw("errors"),
        reply.envelope_raw("messages"),
    );
    let usage = USAGE_KEYS
        .iter()
        .map(|key| format!("`{key}` {}", fig(reply.usage_int(key))))
        .collect::<Vec<_>>()
        .join(", ");
    let _ = writeln!(w, "Usage: {usage}.");
    if let Some(error) = &reply.capture.error {
        let _ = writeln!(w, "Not answered: transport error: {error}.");
        return None;
    }
    match reply.capture.status {
        Some(400 | 413 | 422) => {
            let _ = writeln!(
                w,
                "Request refused. The error above is the evidence for why."
            );
        }
        Some(200..=299) if reply.envelope_raw("success") != "`false`" => {
            if let Some(raw) = reply.answer_raw(field) {
                let _ = writeln!(w, "Answer `{field}` as returned:");
                code_block(w, &raw);
                if let Some(answer) = Members::parse(&raw) {
                    let _ = writeln!(w, "Request accepted.");
                    return Some(answer);
                }
            }
            let _ = writeln!(w, "Not answered: the reply has no `{field}` answer object.");
        }
        _ => {
            let _ = writeln!(
                w,
                "Not answered: no successful result or request validation refusal."
            );
        }
    }
    if let Some(body) = &reply.capture.body {
        code_block(w, &String::from_utf8_lossy(body));
    }
    None
}

fn report_forecast_bounds(w: &mut String, answer: &Members) {
    let Some(steps) = answer.get("steps").and_then(raw_list) else {
        let _ = writeln!(w, "Not answered: the forecast answer has no `steps` array.");
        return;
    };
    let min = DATE_MIN
        .parse::<chrono::NaiveDate>()
        .expect("ISO date bound");
    let max = DATE_MAX
        .parse::<chrono::NaiveDate>()
        .expect("ISO date bound");
    let dates: Vec<_> = steps
        .iter()
        .filter_map(|step| {
            let step = Members::parse(step.get())?;
            let date: String = serde_json::from_str(step.get("value")?.get()).ok()?;
            date.parse::<chrono::NaiveDate>().ok()
        })
        .collect();
    let outside = dates
        .iter()
        .filter(|date| !(min..=max).contains(*date))
        .count();
    let _ = writeln!(
        w,
        "Returned {} steps (requested {FORECAST_HORIZON}); {} `value` members are dates, of which \
         {outside} fall outside {DATE_MIN} through {DATE_MAX} (inclusive).",
        steps.len(),
        dates.len(),
    );
    if outside > 0 {
        let _ = writeln!(w, "Not every forecast value falls inside the bounds.");
    } else if steps.len() != FORECAST_HORIZON || dates.len() != steps.len() {
        let _ = writeln!(w, "Not answered: the reply did not supply all {FORECAST_HORIZON} forecast values as dates.");
    } else {
        let _ = writeln!(
            w,
            "Every forecast value falls inside the bounds for this request."
        );
    }
}

// The original plan measured this for the same 64x64 PNG in its one-pass image probe.
const ONE_PASS_64_IMAGE_TOKENS: i64 = 64;

fn report_multi_pass_image(w: &mut String, reply: Option<&Reply>) {
    if open_reading_answer(w, reply, "clause_01").is_none() {
        let _ = writeln!(
            w,
            "Not answered: no successful multi-pass image reply to compare."
        );
        return;
    }
    let usage = |key| reply.and_then(|reply| reply.usage_int(key));
    let (Some(passes), Some(image_tokens)) = (usage("passes"), usage("image_tokens")) else {
        let _ = writeln!(w, "Not answered: `passes` or `image_tokens` is missing.");
        return;
    };
    if passes <= 1 {
        let _ = writeln!(
            w,
            "Not answered: the request did not split across multiple passes."
        );
        return;
    }
    let per_pass = passes.checked_mul(ONE_PASS_64_IMAGE_TOKENS);
    let _ = writeln!(
        w,
        "One-pass reference: {ONE_PASS_64_IMAGE_TOKENS} `image_tokens` for this PNG. Across \
         {passes} passes, counting it once predicts {ONE_PASS_64_IMAGE_TOKENS}; counting it on \
         each pass predicts {}.",
        fig(per_pass)
    );
    let verdict = if image_tokens == ONE_PASS_64_IMAGE_TOKENS {
        "matches counting the image once"
    } else if Some(image_tokens) == per_pass {
        "matches counting the image on every pass"
    } else {
        "not answered: matches neither prediction"
    };
    let _ = writeln!(w, "Reported `image_tokens` {image_tokens}: {verdict}.");
}

fn report_image_usage(w: &mut String, reply: Option<&Reply>) -> Option<i64> {
    if open_reading_answer(w, reply, "clause_01").is_none() {
        let _ = writeln!(
            w,
            "Not answered: no successful result to measure image usage."
        );
        return None;
    }
    let reply = reply.expect("an answer needs a reply");
    if ["schema_tokens", "input_tokens", "passes"]
        .iter()
        .any(|key| reply.usage_int(key).is_none())
    {
        let _ = writeln!(
            w,
            "Not answered: the reply lacks usage figures for the full input cost or pass count."
        );
    }
    match reply.usage_int("image_tokens") {
        Some(tokens) if tokens > 0 => Some(tokens),
        _ => {
            let _ = writeln!(
                w,
                "Not answered: no positive `image_tokens` count to show image processing."
            );
            None
        }
    }
}

/// The original plan's requests the controls plan resends, by name.
const CONTROL_NAMES: [&str; 2] = ["multi-pass", "one-pass-control"];

/// The original plan's requests 5 and 6, byte for byte. Clairic's usage figures changed between
/// the first run and the open-readings run, and these two bodies, sent without images, show what
/// changed: the first run reported 40 passes (26,488 schema tokens, 29,888 input) and 1 pass
/// (656 schema tokens, 741 input) for them.
fn controls_plan() -> Vec<Probe> {
    let (probes, _) = plan();
    probes
        .into_iter()
        .filter(|probe| CONTROL_NAMES.contains(&probe.name))
        .collect()
}

/// For each control: its usage, the billing formula the contract pins checked against its input
/// tokens, the same with the state counted once, and the retry tokens its answers report, so the
/// summary shows which accounting the provider now uses.
fn summarize_controls(
    probes: &[Probe],
    captures: &[Capture],
    sent: usize,
    account_present: bool,
) -> String {
    let mut out = String::new();
    let w = &mut out;
    let _ = writeln!(
        w,
        "# Clairic live probe: controls\n\nRun finished {}. Model `{MODEL}`. Requests sent: {sent} \
         of a {}-request cap. No request was retried. Account id returned by the vault with the \
         token: {account_present}.",
        utc_now(),
        CONTROL_NAMES.len()
    );
    for (probe, capture) in probes.iter().zip(captures) {
        let reply = Reply::new(probe, capture);
        let _ = writeln!(w, "\n## {:02} {}\n", probe.number, probe.name);
        let _ = writeln!(w, "{}.", reply.status());
        let usage = USAGE_KEYS
            .iter()
            .map(|key| format!("`{key}` {}", fig(reply.usage_int(key))))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(w, "Usage: {usage}.");
        let int = |key| reply.usage_int(key);
        if let (Some(input), Some(schema), Some(state), Some(passes)) = (
            int("input_tokens"),
            int("schema_tokens"),
            int("state_tokens"),
            int("passes"),
        ) {
            let image = int("image_tokens").unwrap_or(0);
            let per_pass = schema + passes * state + image;
            let once = schema + state + image;
            let _ =
                writeln!(
                w,
                "`schema + passes x state + image` = {per_pass} ({}); `schema + state + image` = \
                 {once} ({}).",
                if per_pass == input { "equals input" } else { "differs from input" },
                if once == input { "equals input" } else { "differs from input" },
            );
        }
        let retries: Vec<i64> = reply
            .answers()
            .map(|answers| {
                answers
                    .0
                    .iter()
                    .filter_map(|(_, raw)| {
                        let answer = Members::parse(raw.get())?;
                        serde_json::from_str(answer.get("retry_input_tokens")?.get()).ok()
                    })
                    .collect()
            })
            .unwrap_or_default();
        let _ = writeln!(
            w,
            "Answers reporting `retry_input_tokens`: {}, summing to {}.",
            retries.len(),
            retries.iter().sum::<i64>()
        );
    }
    out
}

fn summarize_open_readings(
    probes: &[Probe],
    captures: &[Capture],
    sent: usize,
    account_present: bool,
) -> String {
    let replies: Vec<_> = probes
        .iter()
        .zip(captures)
        .map(|(probe, capture)| Reply::new(probe, capture))
        .collect();
    let by_name = |name| replies.iter().find(|reply| reply.probe.name == name);
    let mut out = String::new();
    let w = &mut out;
    let _ = writeln!(
        w,
        "# Clairic live probe: open readings\n\nRun finished {}. Model `{MODEL}`. Requests sent: \
         {sent} of an {OPEN_READINGS_MAX_REQUESTS}-request cap. No request was retried. Account id \
         returned by the vault with the token: {account_present}.\n\nSee `NN-name.request.json` \
         for the sent bodies and `NN-name.body` and `NN-name.json` for the captured replies.",
        utc_now()
    );

    let _ = writeln!(
        w,
        "\n## 1. Date year step and forecast date bounds\n\n### 01 date-year-step\n"
    );
    let _ = open_reading_answer(w, by_name("date-year-step"), "completion_date");
    let _ = writeln!(w, "\n### 02 forecast-date-bounds\n");
    if let Some(answer) = open_reading_answer(w, by_name("forecast-date-bounds"), "completion_date")
    {
        report_forecast_bounds(w, &answer);
    }

    let _ = writeln!(
        w,
        "\n## 2. Image billing across passes (03 multi-pass-image)\n"
    );
    report_multi_pass_image(w, by_name("multi-pass-image"));

    let _ = writeln!(
        w,
        "\n## 3. Larger images\n\nThe original plan's 64x64 PNG reported {ONE_PASS_64_IMAGE_TOKENS} \
         `image_tokens` and added 41 `schema_tokens` versus its no-image control. The following \
         usage blocks report the totals with larger PNGs on the same control body."
    );
    for (number, name) in [(4, "image-512"), (5, "image-1024")] {
        let _ = writeln!(w, "\n### {number:02} {name}\n");
        if let Some(tokens) = report_image_usage(w, by_name(name)) {
            let _ = writeln!(w, "This PNG accounts for {tokens} reported `image_tokens`.");
        }
    }

    let _ = writeln!(
        w,
        "\n## 4. Image spelling and count\n\nThe original plan accepted the object spelling with \
         one 64x64 image.\n\n### 06 image-data-url\n"
    );
    if let Some(tokens) = report_image_usage(w, by_name("image-data-url")) {
        let _ = writeln!(
            w,
            "The data URL spelling was accepted with {tokens} reported `image_tokens`."
        );
    }
    let _ = writeln!(w, "\n### 07 two-images\n");
    if let Some(tokens) = report_image_usage(w, by_name("two-images")) {
        let _ =
            writeln!(
            w,
            "An item with two image objects was accepted and reported {tokens} `image_tokens`. \
             The one-image reference is {ONE_PASS_64_IMAGE_TOKENS}. The total {} counting \
             both images at the single-image rate.",
            if tokens == 2 * ONE_PASS_64_IMAGE_TOKENS { "matches" } else { "does not match" },
        );
    }
    let _ = writeln!(
        w,
        "Not answered: the maximum image count was not probed above two."
    );

    let _ = writeln!(w, "\n## 5. Instruction length (08 long-instructions-60k)\n");
    let answer = open_reading_answer(w, by_name("long-instructions-60k"), "contact_name");
    let _ = writeln!(
        w,
        "The original plan accepted {LONG_INSTRUCTIONS_CHARS} characters."
    );
    if answer.is_some() {
        let _ = writeln!(w, "The provider also accepted {OPEN_READINGS_INSTRUCTIONS_CHARS} characters. \
            Not answered: this establishes a lower bound on the accepted length, not an exact maximum.");
    } else {
        let _ = writeln!(w, "Not answered: no accepted result at {OPEN_READINGS_INSTRUCTIONS_CHARS} \
            characters. A refusal only indicates a limit on instruction length if the error identifies it.");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_readings_has_eight_requests_and_refuses_a_ninth() {
        let args = Args::try_parse_from(["probe", "--plan", "open-readings", "--out", "out"])
            .expect("valid arguments");
        assert!(matches!(args.plan, ProbePlan::OpenReadings));
        let probes = open_readings_plan();
        let names = [
            "date-year-step",
            "forecast-date-bounds",
            "multi-pass-image",
            "image-512",
            "image-1024",
            "image-data-url",
            "two-images",
            "long-instructions-60k",
        ];
        assert_eq!(probes.len(), names.len());
        let mut budget = Budget::new(args.plan.cap());
        for (index, (probe, name)) in probes.iter().zip(names).enumerate() {
            assert_eq!((probe.number, probe.name), (index + 1, name));
            assert_eq!(budget.take(), Ok(index + 1));
        }
        assert_eq!(
            budget.take(),
            Err("refusing request 9: a run sends at most 8 requests".into())
        );
        assert_eq!(budget.sent, 8);
    }

    #[test]
    fn open_readings_bodies_change_only_the_features_being_probed() {
        fn json(probe: &Probe) -> serde_json::Value {
            serde_json::from_str(&probe.body).expect("valid request JSON")
        }
        let (original, _) = plan();
        let probes = open_readings_plan();
        assert_eq!(
            probes[0].body,
            original[3]
                .body
                .replace(r#""step":"month""#, r#""step":"year""#)
        );

        let mut date_fields = json(&original[3]);
        date_fields["fields"]["completion_date"] = serde_json::json!({
            "type": "forecast", "min": "2026-03-01", "max": "2027-03-01",
            "horizon": 3, "interval": "month", "instructions": "Date the kitchen renovation will be finished"
        });
        assert_eq!(json(&probes[1]), date_fields);

        let original_png = json(&original[11])["images"][0].clone();
        for (probe, baseline, side, count) in [
            (&probes[2], &original[4], 64_u32, 1),
            (&probes[3], &original[5], 512, 1),
            (&probes[4], &original[5], 1024, 1),
            (&probes[6], &original[5], 64, 2),
        ] {
            assert!(probe
                .body
                .starts_with(baseline.body.strip_suffix('}').expect("object")));
            let mut image_body = json(probe);
            let images = image_body
                .as_object_mut()
                .unwrap()
                .remove("images")
                .unwrap();
            assert_eq!(image_body, json(baseline));
            let images = images.as_array().unwrap();
            assert_eq!(images.len(), count);
            for image in images {
                assert_eq!(image["content_type"], "image/png");
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(image["base64"].as_str().expect("base64 PNG"))
                    .expect("valid base64");
                assert!(bytes.starts_with(b"\x89PNG\r\n\x1a\n"));
                assert_eq!(&bytes[16..20], &side.to_be_bytes());
                assert_eq!(&bytes[20..24], &side.to_be_bytes());
                assert!(bytes.len() < 12 * 1024, "keep the PNG small");
            }
        }
        assert_eq!(json(&probes[2])["images"][0], original_png);
        assert_eq!(json(&probes[6])["images"][0], original_png);
        assert_ne!(json(&probes[6])["images"][1], original_png);

        let mut data_url = json(&original[5]);
        data_url["images"] = serde_json::json!([format!(
            "data:image/png;base64,{}",
            original_png["base64"].as_str().unwrap()
        )]);
        assert_eq!(json(&probes[5]), data_url);

        let mut long = json(&probes[7]);
        let instructions = &mut long["fields"]["contact_name"]["instructions"];
        let text = instructions.as_str().unwrap();
        assert_eq!(text.chars().count(), 60_000);
        *instructions = serde_json::Value::String(text[..20_000].to_string());
        assert_eq!(long, json(&original[9]));
    }

    #[test]
    fn the_plan_is_twelve_uniquely_named_requests_within_the_cap() {
        let (probes, _) = plan();
        assert_eq!(probes.len(), MAX_REQUESTS);
        for (index, probe) in probes.iter().enumerate() {
            assert_eq!(probe.number, index + 1);
            let body: serde_json::Value = serde_json::from_str(&probe.body).expect("valid JSON");
            assert_eq!(body["model"], MODEL);
            assert_eq!(body["state"], probe.state.as_str());
            assert!(probe.body.contains(&probe.fields), "{}", probe.name);
        }
        let mut names: Vec<_> = probes.iter().map(|probe| probe.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), MAX_REQUESTS);
    }

    #[test]
    fn the_budget_refuses_the_thirteenth_request() {
        let mut budget = Budget::default();
        for expected in 1..=MAX_REQUESTS {
            assert_eq!(budget.take(), Ok(expected));
        }
        let refusal = budget
            .take()
            .expect_err("a thirteenth request must be refused");
        assert!(refusal.contains("refusing request 13"), "{refusal}");
        assert_eq!(budget.sent, MAX_REQUESTS);
    }

    #[test]
    fn the_image_request_is_the_control_body_plus_images() {
        let (probes, fixture) = plan();
        let control = &probes[5].body;
        let image = &probes[11].body;
        assert!(image.starts_with(&control[..control.len() - 1]));
        assert!(
            image[control.len() - 1..].starts_with(",\"images\":[{\"content_type\":\"image/png\"")
        );
        let body: serde_json::Value = serde_json::from_str(image).expect("valid JSON");
        assert_eq!(body["images"].as_array().map(Vec::len), Some(1));
        assert!(fixture.png.starts_with(b"\x89PNG\r\n\x1a\n"));
        assert_eq!(&fixture.png[16..24], &[0, 0, 0, 64, 0, 0, 0, 64]);
    }

    #[test]
    fn the_pass_probes_overflow_one_pass_and_the_reversed_one_reverses_field_order() {
        let (probes, _) = plan();
        let (multi, control, reversed) = (&probes[4], &probes[5], &probes[6]);
        assert!(multi.fields.len() / 4 > PASS_TOKENS + PASS_TOKENS / 4);
        assert!(control.fields.len() / 4 < PASS_TOKENS / 8);
        assert_eq!(multi.state, control.state);
        let order = |probe: &Probe| {
            Members::parse(&probe.fields)
                .expect("fields object")
                .keys()
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        let mut forward = order(multi);
        assert_eq!(forward.len(), CLAUSES);
        forward.reverse();
        assert_eq!(forward, order(reversed));
        assert_eq!(multi.fields.len(), reversed.fields.len());
    }

    #[test]
    fn long_instructions_are_exactly_the_requested_length() {
        assert_eq!(
            long_instructions(LONG_INSTRUCTIONS_CHARS).chars().count(),
            20_000
        );
        assert_eq!(long_instructions(LONG_INSTRUCTIONS_CHARS).len(), 20_000);
    }

    #[test]
    fn the_span_state_gives_three_different_starts() {
        let state = span_state();
        let [bytes, utf16, scalars] = offsets(&state, SPAN_TARGET).expect("target present");
        // é is 2 bytes, the emoji 4, each CJK character 3; in UTF-16 the emoji is 2 units.
        assert_eq!(bytes.1, scalars.1 + 1 + 3 + 4 * 2);
        assert_eq!(utf16.1, scalars.1 + 1);
        assert_eq!(bytes.2 - bytes.1, SPAN_TARGET.len());
    }

    #[test]
    fn members_keep_order_and_the_providers_number_text() {
        let members = Members::parse(r#"{"b":1.50,"a":3.8e-10,"c":{"x":[1,2]}}"#).expect("object");
        assert_eq!(members.keys(), ["b", "a", "c"]);
        assert_eq!(members.get("b").map(RawValue::get), Some("1.50"));
        assert_eq!(members.get("a").map(RawValue::get), Some("3.8e-10"));
        assert_eq!(decimals_of("1.50"), Some(2));
        assert_eq!(decimals_of("3.8e-10"), None);
    }

    #[test]
    fn the_scrubber_redacts_token_and_account_id() {
        let secret = Secret {
            token: "tok_0123456789".into(),
            account_id: Some("acct_abcdef0123".into()),
        };
        let mut scrub = Scrub::new(Some(&secret));
        let text = scrub.text("x tok_0123456789 y /accounts/acct_abcdef0123/ai");
        assert_eq!(text, "x <REDACTED> y /accounts/<REDACTED>/ai");
        assert_eq!(scrub.redactions, 2);
    }

    fn capture(status: u16, body: &str) -> Capture {
        Capture {
            status: Some(status),
            reason: None,
            headers: Vec::new(),
            body: Some(body.as_bytes().to_vec()),
            error: None,
            elapsed_ms: 1,
        }
    }

    #[test]
    fn open_readings_summary_distinguishes_observations_from_missing_evidence() {
        let probes = open_readings_plan();
        let image_reply = |tokens, passes| {
            capture(
                200,
                &format!(
                    r#"{{"success":true,"result":{{"answers":{{"clause_01":{{"value":null}}}},"usage":{{"passes":{passes},"image_tokens":{tokens},"schema_tokens":640,"input_tokens":800}}}}}}"#
                ),
            )
        };
        let mut captures = vec![
            capture(
                200,
                r#"{"result":{"answers":{"completion_date":{"value":"2027-03-01"}}}}"#,
            ),
            capture(
                200,
                r#"{"result":{"answers":{"completion_date":{"steps":[{"value":"2026-03-01"},{"value":"2026-09-01"},{"value":"2027-03-01"}]}}}}"#,
            ),
            image_reply(192, 3),
            image_reply(256, 1),
            capture(503, "temporarily unavailable"),
            capture(
                400,
                r#"{"success":false,"errors":[{"code":5006,"message":"expected object"}]}"#,
            ),
            image_reply(128, 1),
            capture(
                200,
                r#"{"result":{"answers":{"contact_name":{"value":"Imogen Hart"}}}}"#,
            ),
        ];
        let summary = summarize_open_readings(&probes, &captures, 8, true);
        for evidence in [
            "Every forecast value falls inside the bounds for this request.",
            "Reported `image_tokens` 192: matches counting the image on every pass.",
            "This PNG accounts for 256 reported `image_tokens`.",
            "HTTP 503",
            "Not answered: no successful result or request validation refusal.",
            r#"`errors` `[{"code":5006,"message":"expected object"}]`"#,
            "An item with two image objects was accepted and reported 128 `image_tokens`.",
            "The provider also accepted 60000 characters.",
        ] {
            assert!(
                summary.contains(evidence),
                "missing {evidence:?}\n{summary}"
            );
        }

        captures[1] = capture(
            200,
            r#"{"result":{"answers":{"completion_date":{"steps":[{"value":"2027-04-01"}]}}}}"#,
        );
        captures[2] = image_reply(64, 3);
        let summary = summarize_open_readings(&probes, &captures, 8, true);
        assert!(summary.contains("Not every forecast value falls inside the bounds."));
        assert!(summary.contains("Reported `image_tokens` 64: matches counting the image once."));

        captures[1] = capture(
            200,
            r#"{"result":{"answers":{"completion_date":{"steps":[{"value":null}]}}}}"#,
        );
        captures[2] = image_reply(64, 1);
        let summary = summarize_open_readings(&probes, &captures, 8, true);
        assert!(summary
            .contains("Not answered: the reply did not supply all 3 forecast values as dates."));
        assert!(summary.contains("Not answered: the request did not split across multiple passes."));
    }

    /// A summary over synthetic replies: the span verdict, the re-billing arithmetic and the
    /// "unmeasured" wording all come from the captured bytes.
    #[test]
    fn the_summary_reads_its_verdicts_from_the_replies() {
        let (probes, fixture) = plan();
        let state = span_state();
        let [_, _, scalars] = offsets(&state, SPAN_TARGET).expect("target present");
        let ok = |result: String| {
            format!(r#"{{"result":{result},"success":true,"errors":[],"messages":[]}}"#)
        };
        let captures: Vec<Capture> = probes
            .iter()
            .map(|probe| match probe.name {
                "span-offsets" => capture(
                    200,
                    &ok(format!(
                        r#"{{"answers":{{"tracking_code":{{"type":"text","span":[{},{}],"value":"{SPAN_TARGET}"}}}},"truncated":false,"state_text":{},"usage":{{"input_tokens":50,"output_tokens":0}}}}"#,
                        scalars.1,
                        scalars.2,
                        serde_json::to_string(&state).unwrap()
                    )),
                ),
                "one-pass-control" => capture(
                    200,
                    &ok(r#"{"answers":{"clause_01":{"type":"text","value":null}},"truncated":false,"usage":{"state_tokens":60,"image_tokens":0,"schema_tokens":300,"input_tokens":400,"output_tokens":0,"passes":1}}"#.into()),
                ),
                "multi-pass" => capture(
                    200,
                    &ok(r#"{"answers":{"clause_01":{"type":"text","value":null}},"truncated":false,"usage":{"state_tokens":60,"image_tokens":0,"schema_tokens":12000,"input_tokens":12200,"output_tokens":0,"passes":2}}"#.into()),
                ),
                "unknown-type" => capture(
                    400,
                    r#"{"result":null,"success":false,"errors":[{"code":5006,"message":"bad"}],"messages":[]}"#,
                ),
                _ => capture(500, "upstream failure"),
            })
            .collect();
        let summary = summarize(&probes, &captures, &fixture, 12, true);
        assert!(
            summary.contains(&format!(
                "Returned span `[{}, {}]` against the sent state: matches Unicode scalars, end exclusive.",
                scalars.1, scalars.2
            )),
            "{summary}"
        );
        assert!(summary.contains("`state_text` equals the sent state byte for byte"));
        // Control overhead T = 400 - 300 - 60 = 40; two passes, each billing state and T:
        // 12000 + 2 * (60 + 40) = 12200.
        assert!(summary
            .contains("state and overhead billed on every pass: predicts 12200 -- EXACT MATCH"));
        assert!(summary.contains("state once, overhead on every pass: predicts 12140."));
        assert!(summary.contains(r#"`errors` `[{"code":5006,"message":"bad"}]`"#));
        assert!(summary.contains("| 1 | all-types | unmeasured | unmeasured | unmeasured |"));
        assert!(!summary.contains("| 1 | all-types | 0"));
    }
}
