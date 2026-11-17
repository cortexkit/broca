//! `session.read {tag, call_key}` (llm-runner `tag_read`) over a session kept in the new
//! storage, through the module's registry: a number the model cites in a call resolves to the
//! part the request that made the call showed under it, and to nothing else.

use std::collections::BTreeSet;

use broca_session_store::record::{Placement, Record, TagRange};
use cortexkit_role_llm_runner::errors::{UNKNOWN_CALL, UNKNOWN_TAG};
use cortexkit_role_llm_runner::tag_read::{TagReadAnswer, TagReadRequest};

use super::tag_map_tests::{
    closed, entry_of, map_rows, noted_session, prompt_at, shown_numbers, writer, OWNER,
};
use super::*;
use crate::serve::ServeError;

/// The session as stored, read through the module's store wherever it keeps it.
async fn stored(
    side: &Side,
) -> Arc<crate::store_read::StoredView<broca_session_store::backend::LocalBackend>> {
    let store = side
        .registry
        .store_holding(&side.key)
        .unwrap()
        .expect("the session is kept in the new storage");
    store
        .current_view(&side.key)
        .await
        .unwrap()
        .expect("the session was written")
}

/// The registry's answer to a tag read of `tag` in call `call_key`'s request, from the session's
/// owner on an unstamped route.
async fn tag_read(side: &Side, tag: u64, call_key: &str) -> Result<TagReadAnswer, ServeError> {
    let owner = broca_protocol::BindPrincipal::Reserved {
        module_id: OWNER.into(),
    };
    read_as(
        side,
        tag,
        call_key,
        &owner,
        &broca_protocol::SendScope::Unstamped,
    )
    .await
}

/// [`tag_read`] from `principal` on a route stamped `scope`.
async fn read_as(
    side: &Side,
    tag: u64,
    call_key: &str,
    principal: &broca_protocol::BindPrincipal,
    scope: &broca_protocol::SendScope,
) -> Result<TagReadAnswer, ServeError> {
    crate::serve::ServeHandler::tag_read(
        &side.registry,
        &side.key,
        &TagReadRequest::new(tag, call_key),
        Some(principal),
        scope,
    )
    .await
    .map(|answer| answer.expect("the session is kept in the new storage"))
}

/// The code a refused tag read answers with.
fn refusal_code(result: Result<TagReadAnswer, ServeError>) -> &'static str {
    match result {
        Err(ServeError::TagReadRefused { code, .. }) => code,
        other => panic!("expected a tag read refusal, got {other:?}"),
    }
}

/// The key of the session's one dispatched call, and the part numbers its batch's start recorded.
async fn the_call(side: &Side) -> (String, BTreeSet<u64>) {
    // An import's seeded run dispatched nothing, so the first intent is the run's own call.
    let records = stored(side).await.records_from(1).await.unwrap();
    let (at, intent) = records
        .iter()
        .find_map(|(seq, record)| match record {
            Record::ToolIntent(intent) => Some((*seq, intent.clone())),
            _ => None,
        })
        .expect("the tool turn's call was dispatched");
    let key = intent.call_key.expect("the call has a key");
    let shown = records
        .iter()
        .filter(|(seq, _)| *seq < at)
        .find_map(|(_, record)| match record {
            Record::BatchStart(batch) if batch.batch_id == intent.batch_id => {
                Some(batch.shown_tags.clone())
            }
            _ => None,
        })
        .expect("the call's batch start");
    (key, expand(&shown))
}

/// Every number `ranges` hold.
fn expand(ranges: &[TagRange]) -> BTreeSet<u64> {
    ranges
        .iter()
        .flat_map(|range| range.first..=range.last)
        .collect()
}

/// The batch of a call records exactly the numbers its request showed; each resolves
/// to the message and part the session's tag map names for it, served as `session.read` serves
/// the message, display facts included.
#[tokio::test]
async fn each_number_the_calls_request_showed_resolves_to_its_part() {
    let side = noted_session(vec![text_step("a4", "Noted.")]).await;
    // r1's step is the provider's first call; r2's tool turn its second.
    let shown_in_request = shown_numbers(&prompt_at(&side, 1));
    let (key, recorded) = the_call(&side).await;
    assert!(!recorded.is_empty(), "the request showed numbers");
    assert_eq!(
        recorded, shown_in_request,
        "the batch recorded what its request showed"
    );
    let rows = map_rows(&side).await;
    for tag in &recorded {
        let row = rows
            .iter()
            .find(|row| row.tag == *tag)
            .expect("the session numbers every number a request showed");
        let answer = tag_read(&side, *tag, &key)
            .await
            .expect("a shown number resolves");
        assert_eq!(
            (
                answer.entry.ordinal,
                answer.entry.mid.as_str(),
                u64::from(answer.part)
            ),
            (row.ordinal, row.mid.as_str(), row.part),
            "number {tag}"
        );
        assert!(!answer.host, "broca writes no host parts");
        let mut read =
            crate::serve::SessionReadRequest::new(side.key.clone(), Some(row.ordinal), Some(1));
        read.include_display_facts = true;
        let page = crate::serve::ServeHandler::read(&side.registry, read)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&answer.entry).unwrap(),
            serde_json::to_value(&page.messages[0]).unwrap(),
            "number {tag}'s message, as session.read serves it with display facts"
        );
    }
}

/// A number the session gives a part the call's request didn't show is refused, as is one no
/// part has: a citation can only name what the model read in that request.
#[tokio::test]
async fn a_number_the_calls_request_did_not_show_is_unknown_tag() {
    let side = noted_session(vec![text_step("a4", "Noted.")]).await;
    let (key, shown) = the_call(&side).await;
    let rows = map_rows(&side).await;
    let later = rows
        .iter()
        .map(|row| row.tag)
        .find(|tag| !shown.contains(tag))
        .expect("the reply after the call takes a number the call's request never showed");
    assert_eq!(
        refusal_code(tag_read(&side, later, &key).await),
        UNKNOWN_TAG
    );
    let beyond = rows.iter().map(|row| row.tag).max().unwrap() + 100;
    assert_eq!(
        refusal_code(tag_read(&side, beyond, &key).await),
        UNKNOWN_TAG
    );
}

/// A key no call of the session has names no request.
#[tokio::test]
async fn a_key_no_call_has_is_unknown_call() {
    let side = noted_session(vec![text_step("a4", "Noted.")]).await;
    let (_, shown) = the_call(&side).await;
    let tag = *shown.iter().next().unwrap();
    assert_eq!(
        refusal_code(tag_read(&side, tag, "no-such-call").await),
        UNKNOWN_CALL
    );
}

/// A revert that removes the call's message removes the call from the session's history: its
/// numbers no longer resolve, though its records stay in the log.
#[tokio::test]
async fn a_call_a_revert_removed_is_unknown_call() {
    let side = noted_session(vec![text_step("a4", "Noted.")]).await;
    let (key, shown) = the_call(&side).await;
    let tag = *shown.iter().next().unwrap();
    assert!(tag_read(&side, tag, &key).await.is_ok());

    // Revert to r2's input, which drops r2's tool turn; r9 resends it, left unfinished.
    closed(&side).await;
    let mut session = writer(&side).await;
    let records = session.records_from(1).await.unwrap();
    let (target_ordinal, target, _) = entry_of(&session, "read a").await;
    let high =
        broca_session_loop::entries::lifetime_tag_high(&session.conversation, &session.operations)
            .unwrap();
    let mut start = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.clone())
        .expect("the latest run's start");
    start.run_id = "r9".into();
    start.send_id = Some("s9".into());
    let input = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::Input(input)
                if input.run_id == "r2" && input.placement == Placement::RunInput =>
            {
                let mut input = input.clone();
                input.run_id = "r9".into();
                Some(input)
            }
            _ => None,
        })
        .unwrap();
    let facts = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::EntryFacts(facts) if facts.ordinal == target_ordinal => {
                let mut facts = (**facts).clone();
                facts.run_id = Some("r9".into());
                facts.tag_base = high;
                Some(facts)
            }
            _ => None,
        })
        .unwrap();
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
    assert_eq!(refusal_code(tag_read(&side, tag, &key).await), UNKNOWN_CALL);
}

/// The principal and scope Prefrontal moves a head under.
fn mover() -> (broca_protocol::BindPrincipal, broca_protocol::SessionScope) {
    let owner = broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    };
    let scope = broca_protocol::SessionScope {
        owner: owner.clone(),
        scope_ref: "agent:a1".into(),
        scope_epoch: 1,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
        agent_id_frozen: false,
    };
    (owner, scope)
}

/// A session that imported Thalamus's published OpenCode 2 export, then ran one turn on the
/// compaction contract, rendering part numbers, whose model called a tool. The number of
/// messages the import placed.
async fn imported_session() -> (Side, u64) {
    use broca_wire::head_import::{FileImportParams, ImportHarness, ImportOrigin, ImportSource};
    use sha2::Digest as _;
    let folder = super::head_import_input().join("opencode2");
    let export = std::fs::read(folder.join("export.json")).unwrap();
    let session_id = serde_json::from_slice::<Value>(&export).unwrap()["info"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let floor: Value =
        serde_json::from_slice(&std::fs::read(folder.join("reply.json")).unwrap()).unwrap();
    let side = side(
        vec![tool_step("b1"), text_step("b2", "Read it.")],
        false,
        true,
    )
    .await;
    let source = side.dir.join("import-source");
    std::fs::create_dir_all(&source).unwrap();
    let path = source.join("head.json");
    std::fs::write(&path, &export).unwrap();
    std::fs::copy(
        folder.join("export.sends.json"),
        source.join("head.sends.json"),
    )
    .unwrap();
    let (owner, scope) = mover();
    let command =
        crate::store_import::StoreCommand::Import(Box::new(crate::store_import::ImportCommand {
            params: FileImportParams {
                import_id: "imp-1".into(),
                source: ImportSource {
                    harness: ImportHarness::OpenCode2,
                    path: path.display().to_string(),
                    session: session_id.clone(),
                    sha256: format!("{:x}", sha2::Sha256::digest(&export)),
                },
                origin: ImportOrigin {
                    kind: "import".into(),
                    source_ref: format!("thalamus:{session_id}"),
                },
                tag_floor: floor["tag_floor"].as_u64(),
                retention: None,
                moves_from: None,
            },
            principal: owner.clone(),
            scope: Some(scope.clone()),
            roots: Arc::new(crate::store_import::ImportRoots::new(
                vec![source.clone()],
                Some(source.clone()),
            )),
            now_ms: 9_000,
        }));
    match side
        .registry
        .route_store_command(&side.key, command)
        .await
        .unwrap()
    {
        crate::store_import::StoreOutcome::Imported(_) => {}
        other => panic!("the import was refused: {other:?}"),
    }
    let imported = side.registry.store_holding(&side.key).unwrap().is_some();
    assert!(imported, "the imported session is kept in the new storage");
    let mut request = untooled("read a");
    request.principal = Some(owner);
    request.scope = broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
        scope,
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    });
    let outcome = side
        .send_request(
            "r1",
            request,
            Some(planned("plan-1", Some(compacting_manifest(true)))),
        )
        .await;
    assert!(
        matches!(
            outcome,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ),
        "{outcome:?}"
    );
    side.settled("r1").await;
    side.assert_completed("r1").await;
    let placed = stored(&side)
        .await
        .import_receipt()
        .await
        .unwrap()
        .expect("the session kept its import's receipt")
        .imported_messages;
    (side, placed)
}

/// Numbers the request showed on imported messages are the source runner's, never resolved;
/// a number the request showed on a message this runner placed after the import resolves.
#[tokio::test]
async fn a_number_on_an_imported_message_is_unknown_tag() {
    let (side, placed) = imported_session().await;
    let (key, shown) = the_call(&side).await;
    let rows = map_rows(&side).await;
    let ordinal_of = |tag: u64| {
        rows.iter()
            .find(|row| row.tag == tag)
            .map(|row| row.ordinal)
    };
    let imported = shown
        .iter()
        .copied()
        .find(|tag| ordinal_of(*tag).is_some_and(|ordinal| ordinal <= placed))
        .expect("the request showed a number on an imported message");
    assert_eq!(
        refusal_code(tag_read(&side, imported, &key).await),
        UNKNOWN_TAG
    );
    let native = shown
        .iter()
        .copied()
        .find(|tag| ordinal_of(*tag).is_some_and(|ordinal| ordinal > placed))
        .expect("the request showed a number on the run's own input");
    let answer = tag_read(&side, native, &key).await.unwrap();
    assert_eq!(Some(answer.entry.ordinal), ordinal_of(native));
}

/// Writes reader samples for `tag_read` when `BROCA_TAG_READ_SAMPLES` names a folder: the reply
/// bodies the serve layer sends for an owner's resolved tag (`{"result": answer}`) and for each
/// refusal, each body built by the serializers the serve layer uses.
#[tokio::test]
async fn tag_read_reader_samples() {
    let Some(folder) = std::env::var_os("BROCA_TAG_READ_SAMPLES") else {
        return;
    };
    let folder = std::path::PathBuf::from(folder);
    std::fs::create_dir_all(&folder).unwrap();
    let side = noted_session(vec![text_step("a4", "Noted.")]).await;
    let (key, shown) = the_call(&side).await;
    let rows = map_rows(&side).await;
    let write = |name: &str, body: Vec<u8>| std::fs::write(folder.join(name), body).unwrap();
    // The person's own words, and the model's tool call, as a citation names each.
    for (name, role) in [
        ("answer-user.json", "user"),
        ("answer-assistant.json", "assistant"),
    ] {
        let tag = rows
            .iter()
            .find(|row| shown.contains(&row.tag) && serde_json::to_value(row.role).unwrap() == role)
            .map(|row| row.tag)
            .unwrap();
        let answer = tag_read(&side, tag, &key).await.unwrap();
        write(
            name,
            serde_json::to_vec(&json!({ "result": answer })).unwrap(),
        );
    }
    let refusal = |result: Result<TagReadAnswer, ServeError>| match result {
        Err(error @ ServeError::TagReadRefused { code, .. }) => {
            serde_json::to_vec(&subc_protocol::ErrorBody::new(code, error.to_string())).unwrap()
        }
        other => panic!("expected a refusal, got {other:?}"),
    };
    let later = rows
        .iter()
        .map(|row| row.tag)
        .find(|tag| !shown.contains(tag))
        .unwrap();
    write(
        "refused-unknown-tag.json",
        refusal(tag_read(&side, later, &key).await),
    );
    write(
        "refused-unknown-call.json",
        refusal(tag_read(&side, *shown.iter().next().unwrap(), "no-such-call").await),
    );
    let stranger = broca_protocol::BindPrincipal::Reserved {
        module_id: "occipital".into(),
    };
    let unstamped = broca_protocol::SendScope::Unstamped;
    write(
        "refused-scope-owner-mismatch.json",
        refusal(read_as(&side, later, &key, &stranger, &unstamped).await),
    );
}

/// Only the session's owner, on a plain route, reads a tag: another principal, and the owner on
/// a flow's or an agent run's route, are refused before any lookup, even for a number the call's
/// request showed.
#[tokio::test]
async fn anyone_but_the_owner_on_a_plain_route_is_refused() {
    use cortexkit_role_llm_runner::errors::SCOPE_OWNER_MISMATCH;
    let side = noted_session(vec![text_step("a4", "Noted.")]).await;
    let (key, shown) = the_call(&side).await;
    let tag = *shown.iter().next().unwrap();
    assert!(tag_read(&side, tag, &key).await.is_ok());
    let stranger = broca_protocol::BindPrincipal::Reserved {
        module_id: "occipital".into(),
    };
    let unstamped = broca_protocol::SendScope::Unstamped;
    let refused = read_as(&side, tag, &key, &stranger, &unstamped).await;
    assert_eq!(refusal_code(refused), SCOPE_OWNER_MISMATCH);
    let owner = broca_protocol::BindPrincipal::Reserved {
        module_id: OWNER.into(),
    };
    for (flow_id, run_id) in [(Some("flow-1"), None), (None, Some("run-1"))] {
        let stamp = broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
            scope: broca_protocol::SessionScope {
                owner: owner.clone(),
                scope_ref: "agent:a1".into(),
                scope_epoch: 1,
                flow_id: flow_id.map(str::to_owned),
                sender: None,
                agent_id: None,
                run_id: run_id.map(str::to_owned),
                agent_id_frozen: false,
            },
            owner_authorized: true,
            kind: broca_protocol::ScopeKind::Head,
            parent: None,
            parent_state: None,
        });
        let refused = read_as(&side, tag, &key, &owner, &stamp).await;
        assert_eq!(
            refusal_code(refused),
            SCOPE_OWNER_MISMATCH,
            "{flow_id:?} {run_id:?}"
        );
    }
}
