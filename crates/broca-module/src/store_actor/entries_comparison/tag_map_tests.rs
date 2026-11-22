//! `session.tag_map` over a session kept in the session store, through the module's registry: the
//! numbers it names are the ones `session.entries` serves and the next request shows, before and
//! after a compaction cut seals history and after a revert, and its pages split no message.

use std::collections::BTreeSet;

use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::record::{Marker, Placement, Record};
use broca_session_store::session::Session;
use broca_wire::tag_map::{TagMapParams, TagMapResponse, TagMapRow};

use super::*;

/// The compaction provider the plan names.
const PROVIDER: &str = "ck-mc";
/// A module the plan names as a sender, whose note rides a tool result.
const NOTER: &str = "aft";

/// A plan naming [`PROVIDER`], rendering part numbers, and naming [`NOTER`] as a sender.
fn noting_manifest() -> broca_records::record::PlanManifest {
    let mut manifest = compacting_manifest(true);
    manifest.plan = broca_records::record::OpaqueJson::from_json_text(
        json!({
            "compaction_item": {"provider": PROVIDER, "preset": "default"},
            "render_tags": true,
            "senders": [{"module": NOTER}],
        })
        .to_string(),
    )
    .unwrap();
    manifest
}

/// The module the noted session's own sends come from, which owns it.
pub(super) const OWNER: &str = "prefrontal-core";

/// A send from [`OWNER`].
fn owned(text: &str) -> SubmissionRequest {
    let mut request = untooled(text);
    request.principal = Some(broca_protocol::BindPrincipal::Reserved {
        module_id: OWNER.into(),
    });
    request
}

/// A send from [`NOTER`].
fn note(text: &str) -> SubmissionRequest {
    let mut request = untooled(text);
    request.principal = Some(broca_protocol::BindPrincipal::Reserved {
        module_id: NOTER.into(),
    });
    request
}

/// A store side whose session is on the compaction contract and renders part numbers, holding
/// user text, a reply, a tool turn whose result carries a note [`NOTER`] sent while the session
/// was idle, and the reply after it. `more` steps are scripted after those.
pub(super) async fn noted_session(more: Vec<Vec<NormalizedEvent>>) -> Side {
    let mut steps = vec![
        text_step("a1", "Done."),
        tool_step("a2"),
        text_step("a3", "It says file contents."),
    ];
    steps.extend(more);
    let side = side(steps, false, true).await;
    let first = side
        .send_request(
            "r1",
            owned("first"),
            Some(planned("plan-1", Some(noting_manifest()))),
        )
        .await;
    assert!(matches!(first, SendOutcome::Active { .. }), "{first:?}");
    side.settled("r1").await;
    side.assert_completed("r1").await;
    // Delivered only on a tool result, the note waits for the next run's tool batch.
    let noted = side
        .send_ordered(
            "q1",
            note("a note for the tool"),
            json!({"modes": ["tool_result"]}),
        )
        .await;
    assert!(matches!(noted, SendOutcome::Pending { .. }), "{noted:?}");
    let second = side
        .send_request("r2", owned("read a"), Some(planned("plan-1", None)))
        .await;
    assert!(
        matches!(
            second,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ),
        "{second:?}"
    );
    side.settled("r2").await;
    side.assert_completed("r2").await;
    let entries = side
        .compaction(
            Some(PROVIDER),
            "session.entries",
            json!({"select": {"by": "after"}, "detail": "full"}),
        )
        .await
        .unwrap();
    assert!(
        entries.to_string().contains("a note for the tool"),
        "the note rode the tool's result: {entries}"
    );
    side
}

/// One page of the session's tag map, as the registry serves `session.tag_map`.
async fn map_page(side: &Side, after_ordinal: Option<u64>, limit: Option<u64>) -> TagMapResponse {
    crate::serve::ServeHandler::tag_map(
        &side.registry,
        &side.key,
        &TagMapParams {
            after_ordinal,
            limit,
        },
    )
    .await
    .expect("the tag map is served")
    .expect("the session is kept in the new storage")
}

/// Every page of the session's tag map, `limit` messages a page: whether it numbers, and the
/// pages' rows.
async fn whole_map(side: &Side, limit: Option<u64>) -> (bool, Vec<Vec<TagMapRow>>) {
    let mut pages = Vec::new();
    let mut after = None;
    let mut numbered = false;
    loop {
        let page = map_page(side, after, limit).await;
        numbered |= page.numbered;
        pages.push(page.rows);
        match page.next_after_ordinal {
            Some(next) => {
                assert!(after.is_none_or(|after| next > after), "pages move forward");
                after = Some(next);
            }
            None => return (numbered, pages),
        }
    }
}

/// Every row of the session's tag map, read in pages of the default size.
pub(super) async fn map_rows(side: &Side) -> Vec<TagMapRow> {
    let (numbered, pages) = whole_map(side, None).await;
    assert!(numbered, "the session numbers its parts");
    pages.concat()
}

/// `(mid, part, tag)` of each row.
fn triples(rows: &[TagMapRow]) -> BTreeSet<(String, u64, u64)> {
    rows.iter()
        .map(|row| (row.mid.clone(), row.part, row.tag))
        .collect()
}

/// `(mid, part, tag)` of every part `session.entries` serves with a number.
async fn entry_numbers(side: &Side) -> BTreeSet<(String, u64, u64)> {
    let page = side
        .compaction(
            Some(PROVIDER),
            "session.entries",
            json!({"select": {"by": "after"}, "detail": "full"}),
        )
        .await
        .unwrap();
    let mut numbers = BTreeSet::new();
    for entry in page["entries"].as_array().unwrap() {
        for part in entry["parts"].as_array().unwrap() {
            if let Some(tag) = part["tag"].as_u64() {
                numbers.insert((
                    entry["mid"].as_str().unwrap().to_owned(),
                    part["id"].as_str().unwrap().parse().unwrap(),
                    tag,
                ));
            }
        }
    }
    numbers
}

/// Every part number a request shows: each `N` between two section signs in its text.
pub(super) fn shown_numbers(prompt: &Value) -> BTreeSet<u64> {
    const SIGN: char = '\u{a7}';
    let text = prompt.to_string();
    let mut numbers = BTreeSet::new();
    let mut rest = text.as_str();
    while let Some(at) = rest.find(SIGN) {
        rest = &rest[at + SIGN.len_utf8()..];
        let digits = rest.chars().take_while(char::is_ascii_digit).count();
        if digits > 0 && rest[digits..].starts_with(SIGN) {
            numbers.insert(rest[..digits].parse().unwrap());
            rest = &rest[digits + SIGN.len_utf8()..];
        }
    }
    numbers
}

/// The request the provider was sent at index `call`, as JSON.
pub(super) fn prompt_at(side: &Side, call: usize) -> Value {
    serde_json::to_value(&side.provider.calls()[call].prompt).unwrap()
}

/// Sends `text` as a later run of the session's plan and waits for it to complete. The index of
/// the provider call its first step made.
async fn later_run(side: &Side, run_id: &str, text: &str) -> usize {
    let call = side.provider.calls().len();
    let outcome = side
        .send_request(run_id, untooled(text), Some(planned("plan-1", None)))
        .await;
    assert!(
        matches!(
            outcome,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ),
        "{outcome:?}"
    );
    side.settled(run_id).await;
    side.assert_completed(run_id).await;
    call
}

/// The numbers of `rows` whose message is at `range`'s ordinals.
fn tags_in(rows: &[TagMapRow], range: std::ops::Range<u64>) -> BTreeSet<u64> {
    rows.iter()
        .filter(|row| range.contains(&row.ordinal))
        .map(|row| row.tag)
        .collect()
}

/// No two rows share a number.
fn assert_unique(rows: &[TagMapRow]) {
    let tags: BTreeSet<u64> = rows.iter().map(|row| row.tag).collect();
    assert_eq!(tags.len(), rows.len(), "a number is shown once: {rows:?}");
}

/// The map names exactly the numbers `session.entries` serves, for the same messages and parts:
/// user text, model text, a tool call whose result carries a note, and the reply after it. The
/// next request shows exactly those numbers. Read in pages of any size, the rows are the same,
/// and a message's parts are never split across pages.
#[tokio::test]
async fn the_map_names_the_numbers_session_entries_serves_and_the_next_request_shows() {
    let side = noted_session(vec![text_step("a4", "Noted.")]).await;
    let rows = map_rows(&side).await;
    assert_eq!(triples(&rows), entry_numbers(&side).await);
    assert_unique(&rows);
    // The user's text, the model's text, and the tool turn's text and call each take a number.
    let first = &rows[0];
    assert_eq!(
        (first.ordinal, first.part, first.role),
        (1, 0, broca_protocol::content::Role::User),
        "{rows:?}"
    );
    let tool_turn: Vec<&TagMapRow> = rows
        .iter()
        .filter(|row| row.role == broca_protocol::content::Role::Assistant && row.part == 1)
        .collect();
    assert_eq!(tool_turn.len(), 1, "the tool call takes a number: {rows:?}");
    let tool_ordinal = tool_turn[0].ordinal;
    assert_eq!(
        rows.iter()
            .filter(|row| row.ordinal == tool_ordinal)
            .count(),
        2,
        "the tool turn's text and its call are numbered: {rows:?}"
    );

    let call = later_run(&side, "r3", "next").await;
    let after = map_rows(&side).await;
    assert_eq!(triples(&after), entry_numbers(&side).await);
    assert_eq!(after[..rows.len()], rows[..], "earlier numbers stay");
    // The request shows every message before r3's reply, r3's input included.
    let reply = after.iter().map(|row| row.ordinal).max().unwrap();
    assert_eq!(
        shown_numbers(&prompt_at(&side, call)),
        tags_in(&after, 1..reply),
        "the request shows the map's numbers"
    );

    for limit in 1..=3 {
        let (numbered, pages) = whole_map(&side, Some(limit)).await;
        assert!(numbered);
        assert_eq!(pages.concat(), after, "pages of {limit} hold the same rows");
        let mut seen = BTreeSet::new();
        for page in &pages {
            let ordinals: BTreeSet<u64> = page.iter().map(|row| row.ordinal).collect();
            assert!(
                ordinals.is_disjoint(&seen),
                "a message's parts stay on one page of {limit}: {pages:?}"
            );
            seen.extend(ordinals);
        }
    }
}

/// Waits until the session's actor has closed.
pub(super) async fn closed(side: &Side) {
    broca_test_support::wait_until("the session closes", || async {
        match side.registry.slot_state(&side.key) {
            None => Ok(()),
            Some(state) => Err(state.to_owned()),
        }
    })
    .await;
}

/// The session's writer, loaded as no actor holds it.
pub(super) async fn writer(side: &Side) -> Session<LocalBackend> {
    Session::load(
        LocalBackend::new(side.dir.join(broca_wal::SESSION_STORAGE_DIR)),
        SessionAddr(broca_core::session_addr(&side.key)),
        &SessionTriple {
            project_root: side.key.project_root.display().to_string(),
            harness: side.key.harness.clone(),
            session: side.key.session.clone(),
        },
        &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
    )
    .await
    .expect("the session opens for writing")
}

/// The ordinal, id and entry facts' base of the window message whose text is `text`.
pub(super) async fn entry_of(session: &Session<LocalBackend>, text: &str) -> (u64, String, u64) {
    let window = session.conversation.window_start();
    session
        .entries(window, 200)
        .await
        .unwrap()
        .into_iter()
        .find(|entry| {
            entry.message.content.iter().any(|block| {
                matches!(block, broca_protocol::content::ContentBlock::Text { text: at } if at == text)
            })
        })
        .and_then(|entry| {
            let facts = entry.state.facts?;
            Some((entry.ordinal, facts.mid.clone(), facts.tag_base))
        })
        .expect("the message has facts")
}

/// The seq of run `run`'s own input, where a cut can fall.
async fn input_of(session: &Session<LocalBackend>, run: &str) -> u64 {
    session
        .records_from(1)
        .await
        .unwrap()
        .into_iter()
        .find_map(|(seq, record)| match record {
            Record::Input(input)
                if input.run_id == run && input.placement == Placement::RunInput =>
            {
                Some(seq)
            }
            _ => None,
        })
        .expect("the run's input")
}

/// A compaction cut seals the messages before it: the map still names their numbers, and the
/// request after the cut shows the map's numbers from the cut on. A revert then drops the
/// newest run: its numbers leave the map, and no later message takes any of them.
#[tokio::test]
async fn numbers_before_a_cut_stay_and_a_revert_frees_none_for_reuse() {
    let side = noted_session(vec![text_step("a4", "Third."), text_step("a5", "Fourth.")]).await;
    let before = map_rows(&side).await;

    // The cut keeps r2's input on; everything before it is sealed.
    closed(&side).await;
    let mut session = writer(&side).await;
    let cut = input_of(&session, "r2").await;
    let (kept, mid, checkpoint) = entry_of(&session, "read a").await;
    session
        .append(
            &[Record::Marker(Marker {
                view_version: 0,
                first_kept_mid: mid,
                first_kept_ordinal: kept,
                checkpoint,
                harness: Some(side.key.harness.clone()),
                session: Some(side.key.session.clone()),
            })],
            true,
        )
        .await
        .unwrap();
    session.seal(Some(cut), 1).await.unwrap();
    assert_eq!(
        session.conversation.window_start(),
        kept,
        "the messages before the cut are sealed"
    );
    session.close().await.unwrap();
    assert_eq!(map_rows(&side).await, before, "sealed numbers stay");
    assert_eq!(triples(&before), entry_numbers(&side).await);

    let call = later_run(&side, "r3", "third").await;
    let after_cut = map_rows(&side).await;
    assert_eq!(triples(&after_cut), entry_numbers(&side).await);
    assert_eq!(
        after_cut[..before.len()],
        before[..],
        "earlier numbers stay"
    );
    assert_unique(&after_cut);
    let reply = after_cut.iter().map(|row| row.ordinal).max().unwrap();
    assert_eq!(
        shown_numbers(&prompt_at(&side, call)),
        tags_in(&after_cut, kept..reply),
        "the request after the cut shows the map's numbers from the cut on"
    );

    // A revert to r3's input: r9 takes its place, left unfinished as a crash leaves a run.
    closed(&side).await;
    let mut session = writer(&side).await;
    let (target_ordinal, target, _) = entry_of(&session, "third").await;
    let high =
        broca_session_loop::entries::lifetime_tag_high(&session.conversation, &session.operations)
            .unwrap();
    let mut start = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.clone())
        .expect("r3's start");
    start.run_id = "r9".into();
    start.send_id = Some("s9".into());
    let records = session.records_from(1).await.unwrap();
    let input = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::Input(input) if input.run_id == "r3" => {
                let mut input = input.clone();
                input.run_id = "r9".into();
                Some(input)
            }
            _ => None,
        })
        .expect("r3's input");
    let facts = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::EntryFacts(facts) if facts.ordinal == target_ordinal => {
                let mut facts = (**facts).clone();
                facts.run_id = Some("r9".into());
                // The resent input's parts are numbered after every number shown before.
                facts.tag_base = high;
                Some(facts)
            }
            _ => None,
        })
        .expect("r3's input's facts");
    session
        .revert(
            &target,
            vec![
                Record::RunStart(Box::new(start)),
                Record::Input(input),
                Record::EntryFacts(std::sync::Arc::new(facts)),
            ],
            Some(high),
            2,
        )
        .await
        .unwrap();
    session.close().await.unwrap();
    let reverted = tags_in(&after_cut, target_ordinal..u64::MAX);
    let after_revert = map_rows(&side).await;
    assert_eq!(triples(&after_revert), entry_numbers(&side).await);
    assert_eq!(
        after_revert[..before.len()],
        before[..],
        "numbers before the revert target stay"
    );
    let resent = tags_in(&after_revert, target_ordinal..u64::MAX);
    assert!(!resent.is_empty(), "the resent input is numbered");
    assert!(
        resent.is_disjoint(&reverted) && resent.iter().all(|tag| *tag > high),
        "the resent input takes new numbers: {resent:?} after {reverted:?}"
    );

    // A run after the revert, on an actor that first ends r9 as a crash's orphan.
    later_run(&side, "r4", "fourth").await;
    let last = map_rows(&side).await;
    assert_eq!(triples(&last), entry_numbers(&side).await);
    assert_unique(&last);
    let newest = tags_in(&last, target_ordinal..u64::MAX);
    assert!(
        newest.is_disjoint(&reverted) && newest.iter().all(|tag| *tag > high),
        "no number a revert dropped is shown again: {newest:?} after {reverted:?}"
    );
}

/// A session whose runs number nothing, and a session that was never written, answer that they
/// number nothing, with no rows.
#[tokio::test]
async fn a_session_that_numbers_nothing_says_so() {
    let side = side(vec![text_step("a1", "hello")], false, false).await;
    let unwritten = map_page(&side, None, None).await;
    assert_eq!(unwritten, TagMapResponse::unnumbered());
    assert!(matches!(
        side.send("r1", "first").await,
        SendOutcome::Active { .. }
    ));
    side.settled("r1").await;
    assert!(side.holds(), "the session is kept in the new storage");
    assert_eq!(
        map_page(&side, None, None).await,
        TagMapResponse::unnumbered()
    );
}
