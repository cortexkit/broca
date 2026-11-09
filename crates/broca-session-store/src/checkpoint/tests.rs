use super::Checkpoint;
use crate::codec::Message as _;
use crate::fold::conversation::Conversation;
use crate::fold::operations::Operations;

#[test]
fn an_empty_checkpoint_round_trips() {
    let mut checkpoint = Checkpoint {
        covers_through: 0,
        next_seq: 1,
        conversation: Conversation::new(false),
        operational: Operations::default(),
        transcript_base: None,
        ledger_base: None,
        activity_ms: None,
    };
    let bytes = checkpoint.encode();
    let decoded = Checkpoint::decode(&bytes).unwrap();
    assert_eq!(decoded.encode(), bytes);
    assert_eq!(decoded.next_seq, 1);
    assert!(decoded.conversation.messages.is_empty());
    assert_eq!(decoded.activity_ms, None);
    assert!(
        !crate::codec::Fields::new(&bytes, "Checkpoint").any(|field| field.unwrap().number == 7)
    );
    checkpoint.activity_ms = Some(0);
    assert_eq!(
        Checkpoint::decode(&checkpoint.encode())
            .unwrap()
            .activity_ms,
        Some(0)
    );
    let activity = Checkpoint::SCHEMA
        .iter()
        .find(|field| field.name == "activity_ms")
        .unwrap();
    assert_eq!(activity.owner, "session");
    assert_eq!(activity.reset, "Moved");
    assert_eq!(activity.bound, crate::fold::Bound::Fixed);
}

/// A checkpoint whose conversation or operational state is missing doesn't decode, rather than
/// starting a session from nothing.
#[test]
fn a_checkpoint_without_its_states_refuses() {
    assert!(Checkpoint::decode(&[]).is_err());
}
