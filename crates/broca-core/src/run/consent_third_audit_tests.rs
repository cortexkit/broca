use super::*;

fn expired_card(rig: &Rig) -> (String, WalRecord, String) {
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
    let prepared = frames
        .iter()
        .find(|frame| key.ends_with(&format!(":{}", frame.seq)))
        .unwrap()
        .record
        .clone();
    (key, prepared, id)
}

struct SlowLateFeed {
    key: String,
    prepared: WalRecord,
    id: String,
    positions: Mutex<Vec<u64>>,
}

#[async_trait::async_trait]
impl ElicitationPlane for SlowLateFeed {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        let position = cursor.map_or(0, |cursor| cursor.seq);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        self.positions.lock().unwrap().push(position);
        let records = match position {
            0..16 => vec![json!({"requester_ref":"another-call","state":"pending"})],
            16 => vec![
                json!({"state":"answered","requester_ref":self.key,"elicitation_id":self.id,"answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)}),
            ],
            _ => vec![],
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "slow-late".into(),
                seq: position + 1,
            },
        })
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_admission_continues_healthy_feed_beyond_one_budget() {
    let mut rig = expired_run("pass").await;
    let (key, prepared, id) = expired_card(&rig);
    let feed = Arc::new(SlowLateFeed {
        key: key.clone(),
        prepared,
        id,
        positions: Mutex::new(Vec::new()),
    });
    rig.control.elicitation_plane = Some(feed.clone());
    let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
    for _ in 0..3 {
        approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        )
        .await
        .unwrap();
        if rig.tools.provider_requests.load(Ordering::SeqCst) > 0 {
            break;
        }
    }
    assert_eq!(
        rig.tools.provider_requests.load(Ordering::SeqCst),
        1,
        "a healthy late feed must finish across one-second admission budgets"
    );
    assert_eq!(
        feed.positions
            .lock()
            .unwrap()
            .iter()
            .filter(|position| **position == 0)
            .count(),
        1,
        "a budget retry must resume from its last validated late-feed cursor"
    );
    assert_eq!(rig.frames().iter().filter(|frame| matches!(&frame.record, WalRecord::LateApprovalRecorded { call_key, decision: broca_wal::LateDecision::Approved, .. } if call_key == &key)).count(), 1);
    rig.finish().await;
}
