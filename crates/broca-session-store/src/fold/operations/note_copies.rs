use crate::record::{PlanNote, PlanNoteKind, PlanNotes};

/// A copy repeats the same note without another change to its subject in between. The
/// position of the first delivery identifies the change even when a view restores an old anchor.
pub(super) fn changes(
    notes: &[PlanNotes],
) -> impl DoubleEndedIterator<Item = (usize, &PlanNotes, &PlanNote)> {
    let mut latest: Vec<(usize, &PlanNote)> = Vec::new();
    notes
        .iter()
        .flat_map(|delivery| delivery.notes.iter().map(move |note| (delivery, note)))
        .enumerate()
        .map(|(index, (delivery, note))| {
            let change = if let Some((id, prior)) = latest
                .iter_mut()
                .find(|(_, prior)| same_subject(prior, note))
            {
                if *prior != note {
                    *id = index;
                    *prior = note;
                }
                *id
            } else {
                latest.push((index, note));
                index
            };
            (change, delivery, note)
        })
        .collect::<Vec<_>>()
        .into_iter()
}

fn same_subject(left: &PlanNote, right: &PlanNote) -> bool {
    if left.kind == PlanNoteKind::System && right.kind == PlanNoteKind::System {
        return true;
    }
    left == right
        || left
            .named_tool()
            .is_some_and(|name| right.named_tool() == Some(name))
}
