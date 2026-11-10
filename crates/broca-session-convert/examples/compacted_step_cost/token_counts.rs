//! Token-count costs on the same encrypted session histories as the step-cost benchmark.
use super::*;
use broca_core::compaction_v2::{
    self as v2,
    protocol::{
        part::{Part, PartBody},
        read::{Entry, Role},
    },
};

pub(super) async fn measure(args: &Args, templates: &[Template], parts: &SharedParts) {
    let mut report = format!(
        "# Token-count read costs\n\nMachine: {}\n\n\
        Encrypted local-file histories from the existing step-cost templates. Session loading and \
        construction are excluded. Pages contain at most 100 full-detail entries; each cold read \
        starts with an empty session token cache. Notice timing includes observing one new entry \
        and encoding/delivering the notice through the benchmark plane after priming. The 150k \
        walk starts cold and retains its cache between pages. Tokenizer time is measured inside \
        cortexkit_tokens::count (including lazy vocabulary loading), not projection or JSON.\n\n\
        | History messages | Operation | Entries | Total ms | Tokenizer ms | Token share |\n\
        |---:|---|---:|---:|---:|---:|\n",
        machine()
    );
    for &size in &args.sizes {
        let built = build_base(args, size, false, templates, parts).await;
        let session = Session::load(
            LocalBackend::new(built.root.clone()),
            SessionAddr(ADDR.into()),
            &triple(),
            &deployment(),
        )
        .await
        .unwrap();
        let cache = Mutex::new(v2::tokens::Counts::default());
        let start = Instant::now();
        let page = read(&session, parts, &cache, None).await;
        let total = start.elapsed();
        row(
            &mut report,
            built.messages,
            "cold full-detail page",
            page["entries"].as_array().unwrap().len(),
            total,
            cache.lock().unwrap().tokenizer_time(),
        );

        if size == 150_000 {
            let walk = Mutex::new(v2::tokens::Counts::default());
            let mut after = None;
            let mut entries = 0;
            let start = Instant::now();
            loop {
                let page = read(&session, parts, &walk, after).await;
                entries += page["entries"].as_array().unwrap().len();
                after = page["next_after"].as_u64();
                if after.is_none() {
                    break;
                }
            }
            row(
                &mut report,
                built.messages,
                "cold paged whole-history walk",
                entries,
                start.elapsed(),
                walk.lock().unwrap().tokenizer_time(),
            );
        }

        let runtime = v2::runtime::Runtime::default();
        *runtime.tokens.lock().unwrap() = cache.into_inner().unwrap();
        let last = serde_json::from_value(json!({"select": {"by": "after", "after_ordinal": built.messages.saturating_sub(2)}, "detail": "full"})).unwrap();
        let mut entries = broca_module::store_entries::entries_page(&session, &last)
            .await
            .unwrap()
            .entries;
        let admission = broca_session_loop::run::admission_of_start(
            session
                .operations
                .run
                .as_ref()
                .unwrap()
                .start
                .as_ref()
                .unwrap(),
            parts,
        )
        .unwrap();
        let conditions = v2::conditions::conditions(
            v2::protocol::setup::ModelRef::new(
                &admission.model.provider_module_id,
                &admission.model.model_id,
            ),
            admission.context_limit.unwrap(),
            1000,
            false,
        );
        let setup = v2::conditions::setup_request("default", &conditions, 100, false);
        let compactor = Compactor::new(Timeline::new());
        let bind = bind();
        let plane = v2::runtime::BoundPlane {
            plane: Some(&*compactor),
            provider: COMPACTOR,
            bind: &bind,
            scope: None,
        };
        runtime.observe(&entries);
        runtime.flush(&plane, &setup, &conditions, None).await;
        entries.push(Entry::new(
            built.messages + 1,
            "notice-cost-new-entry",
            Role::User,
            100,
            vec![Part::new(
                "0",
                PartBody::Text {
                    text: "the next user input".into(),
                },
            )],
        ));
        let before = runtime.tokens.lock().unwrap().tokenizer_time();
        let start = Instant::now();
        runtime.observe(&entries);
        runtime.flush(&plane, &setup, &conditions, None).await;
        row(
            &mut report,
            built.messages,
            "incremental notice",
            1,
            start.elapsed(),
            runtime.tokens.lock().unwrap().tokenizer_time() - before,
        );
        session.close().await.unwrap();
        std::fs::write(args.out.join("token-counts.md"), &report).unwrap();
    }
    print!("{report}");
}

async fn read(
    session: &Session<LocalBackend>,
    parts: &SharedParts,
    counts: &Mutex<v2::tokens::Counts>,
    after: Option<u64>,
) -> Value {
    let request = serde_json::value::to_raw_value(
        &json!({"select": {"by": "after", "after_ordinal": after, "limit": 100}, "detail": "full"}),
    )
    .unwrap();
    let page = broca_module::store_entries::serve_compaction(
        parts,
        Some(session),
        &bind(),
        Some(COMPACTOR),
        "session.entries",
        &request,
        &broca_module::store_entries::EntryAttribution::Known(false),
        Some(counts),
    )
    .await
    .unwrap();
    serde_json::from_str(page.get()).unwrap()
}

fn row(
    report: &mut String,
    messages: u64,
    operation: &str,
    entries: usize,
    total: Duration,
    tokenizer: Duration,
) {
    let total_ms = total.as_secs_f64() * 1000.0;
    let tokenizer_ms = tokenizer.as_secs_f64() * 1000.0;
    report.push_str(&format!(
        "| {messages} | {operation} | {entries} | {total_ms:.3} | {tokenizer_ms:.3} | {:.1}% |\n",
        tokenizer_ms / total_ms * 100.0
    ));
}
