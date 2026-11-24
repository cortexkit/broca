//! Numbering the window runs' messages of a request off the compaction contract, each run by
//! its own frozen part-number rules.

use broca_core::tags::TagNumbering;

use super::*;

/// Numbers each window run's messages in `prompt` by that run's frozen rules, run after run.
/// A run continues after the highest number shown before its start; that number is carried
/// from one run to the next, each run's messages visited once, rather than recomputed over the
/// whole earlier prompt for every run, which grows with the square of the window's runs.
pub(super) fn number_runs(
    prompt: &mut [Message],
    run_rules: &[RunTagRules],
) -> Result<(), broca_protocol::tags::NumberingOverflow> {
    // The highest number shown by `prompt[..seen]`.
    let (mut high, mut seen) = (0, 0);
    for (at, run) in run_rules.iter().enumerate() {
        let end = run_rules
            .get(at + 1)
            .map_or(prompt.len(), |next| next.start)
            .min(prompt.len());
        let start = run.start.min(end);
        if start < seen {
            // Runs out of order: count again from the prompt's first message.
            (high, seen) = (0, 0);
        }
        high = high.max(broca_protocol::tags::high_after(&prompt[seen..start]));
        seen = start;
        TagNumbering::for_run_after(high, start, run.rules, run.computed)
            .number_new(&mut prompt[..end])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::content::{ContentBlock, Role};

    fn text(role: Role, text: &str) -> Message {
        Message::new(role, vec![ContentBlock::Text { text: text.into() }])
    }

    /// The numbering each run's own rescan of every earlier message gives.
    fn rescanned(prompt: &mut [Message], run_rules: &[RunTagRules]) {
        for (at, run) in run_rules.iter().enumerate() {
            let end = run_rules
                .get(at + 1)
                .map_or(prompt.len(), |next| next.start)
                .min(prompt.len());
            let start = run.start.min(end);
            TagNumbering::for_run(&prompt[..start], run.rules, run.computed)
                .number_new(&mut prompt[..end])
                .unwrap();
        }
    }

    fn tags(prompt: &[Message]) -> Vec<Option<broca_protocol::tags::PartTags>> {
        prompt
            .iter()
            .map(|message| message.part_tags().map(|tags| (*tags).clone()))
            .collect()
    }

    /// Carrying the highest number from run to run numbers every message as rescanning the
    /// prompt before each run does: across rendering and silent runs, a run continuing after a
    /// checkpoint above what the prompt shows, stored-ledger numbers before the first run, and
    /// runs whose starts are out of order.
    #[test]
    fn carrying_the_high_number_numbers_as_a_rescan_does() {
        let rendering = Some(ComputedRules {
            render: true,
            clean: true,
        });
        let silent = Some(ComputedRules {
            render: false,
            clean: false,
        });
        let checkpoint = |checkpoint| Some(ComputedTagRules { checkpoint });
        let layouts = [
            vec![
                (1, rendering, checkpoint(0)),
                (3, silent, checkpoint(0)),
                (5, rendering, checkpoint(40)),
                (8, rendering, checkpoint(0)),
            ],
            vec![
                (0, rendering, None),
                (4, None, None),
                (6, rendering, checkpoint(3)),
            ],
            vec![
                (2, rendering, checkpoint(0)),
                (6, rendering, checkpoint(0)),
                (4, rendering, checkpoint(9)),
                (7, rendering, checkpoint(0)),
            ],
        ];
        for layout in layouts {
            let mut prompt: Vec<Message> = (0..10)
                .map(|at| {
                    text(
                        if at % 2 == 0 {
                            Role::User
                        } else {
                            Role::Assistant
                        },
                        "part",
                    )
                })
                .collect();
            // A message numbered under the stored-ledger rules before the first run.
            prompt[0].set_tags(Some(Arc::new(broca_protocol::tags::PartTags {
                parts: vec![Some(7)],
                computed: None,
            })));
            let rules: Vec<RunTagRules> = layout
                .iter()
                .map(|&(start, rules, computed)| RunTagRules {
                    start,
                    rules,
                    computed,
                })
                .collect();
            let mut expected = prompt.clone();
            rescanned(&mut expected, &rules);
            number_runs(&mut prompt, &rules).unwrap();
            assert_eq!(tags(&prompt), tags(&expected), "layout {layout:?}");
        }
    }
}
