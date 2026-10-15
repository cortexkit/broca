use super::*;

struct EmptyFeed;
#[async_trait::async_trait]
impl ElicitationPlane for EmptyFeed {
    async fn answers(
        &self,
        _: &BatchCtx,
        _: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        Ok(ApprovalPage {
            records: vec![],
            cursor: ApprovalCursor {
                provider_incarnation: "scope-test".into(),
                seq: 0,
            },
        })
    }
}

async fn capability_for(
    key: &str,
    prepared: &WalRecord,
    id: &str,
) -> crate::consent::AuthorizedAnswer {
    let raw = json!({"state":"answered","requester_ref":key,"elicitation_id":id,"answered_choice_id":"approve","binding":crate::consent::binding(prepared)});
    let candidate = crate::consent::answer(&raw, key, prepared)
        .unwrap()
        .unwrap();
    let ctx = BatchCtx {
        batch_id: BatchId("scope-test".into()),
        bind: BindIdentity::new("/project", "test", "scope"),
        run_id: RunId("r".into()),
        scope: None,
    };
    crate::consent::authorize_candidate(
        &EmptyFeed,
        &ctx,
        key,
        prepared,
        None,
        &candidate.record,
        &Mutex::new(crate::consent::LookupProgress::default()),
    )
    .await
    .unwrap()
}

fn another_binding(prepared: &WalRecord) -> WalRecord {
    let mut other = prepared.clone();
    let WalRecord::ToolCallDeferred { module, .. } = &mut other else {
        unreachable!()
    };
    *module = Some("another-module".into());
    other
}

#[tokio::test]
async fn ordinary_approval_writer_rejects_capability_for_another_call_or_binding() {
    for wrong_binding in [false, true] {
        let rig = Rig::new("pass", "none", false, true);
        let paused = rig
            .batch(&crate::AllowAll, vec![call("c")], None, true)
            .await;
        let mut held = paused.open[0].clone();
        let id = held
            .id
            .get_or_insert_with(|| format!("q:{}", held.key))
            .clone();
        let other = if wrong_binding {
            another_binding(&held.prepared)
        } else {
            (*held.prepared).clone()
        };
        let key = if wrong_binding {
            held.key.as_str()
        } else {
            "another-call"
        };
        let proof = capability_for(key, &other, &id).await;
        let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
        let result =
            approval_gate::approved_decision(&held, &proof, &rig.writer, &mut history).await;
        assert!(
            result.is_err(),
            "ordinary approval must check its capability key and binding"
        );
        assert!(!rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::ApprovalRecorded {
                decision: broca_wal::DeferredDecision::Approved,
                ..
            }
        )));
        rig.finish().await;
    }
}

#[tokio::test]
async fn late_approval_writer_rejects_capability_for_another_call_or_binding() {
    for wrong_binding in [false, true] {
        let rig = expired_run("pass").await;
        let frames = rig.frames();
        let (key, id) = frames
            .iter()
            .find_map(|frame| match &frame.record {
                WalRecord::ApprovalRecorded {
                    call_key,
                    elicitation_id: Some(id),
                    decision: broca_wal::DeferredDecision::Expired,
                    ..
                } => Some((call_key.clone(), id.clone())),
                _ => None,
            })
            .unwrap();
        let prepared = &frames
            .iter()
            .find(|frame| key.ends_with(&format!(":{}", frame.seq)))
            .unwrap()
            .record;
        let other = if wrong_binding {
            another_binding(prepared)
        } else {
            prepared.clone()
        };
        let proof = capability_for(
            if wrong_binding { &key } else { "another-call" },
            &other,
            &id,
        )
        .await;
        let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
        let result = approval_gate::record_late_answer(
            &mut history,
            &rig.writer,
            &key,
            proof,
            broca_wal::LateExecution::Execute,
        )
        .await;
        assert!(
            result.is_err(),
            "late approval must check its capability key and binding"
        );
        assert!(!rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::LateApprovalRecorded {
                decision: broca_wal::LateDecision::Approved,
                ..
            }
        )));
        rig.finish().await;
    }
}
