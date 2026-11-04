use super::Checkpoint;
use crate::codec::Message as _;
use crate::fold::conversation::Conversation;
use crate::fold::operations::Operations;

#[test]
fn an_empty_checkpoint_round_trips() {
    let checkpoint = Checkpoint {
        covers_through: 0,
        next_seq: 1,
        conversation: Conversation::new(false),
        operational: Operations::default(),
        transcript_base: None,
        ledger_base: None,
    };
    let bytes = checkpoint.encode();
    let decoded = Checkpoint::decode(&bytes).unwrap();
    assert_eq!(decoded.encode(), bytes);
    assert_eq!(decoded.next_seq, 1);
    assert!(decoded.conversation.messages.is_empty());
}

/// A checkpoint whose conversation or operational state is missing doesn't decode, rather than
/// starting a session from nothing.
#[test]
fn a_checkpoint_without_its_states_refuses() {
    assert!(Checkpoint::decode(&[]).is_err());
}
