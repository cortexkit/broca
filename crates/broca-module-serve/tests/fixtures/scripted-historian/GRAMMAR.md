# Historian prompt and output grammar (ck-mc at `67e09b9a`)

What the scripted responder parses, and what any response must satisfy for
ck-mc to validate and publish it. Everything here is checked against
`crates/mc-module/src/historian_prompt.rs` (prompt assembly),
`crates/mc-module/src/historian_chunk.rs` (transcript rendering) and
`crates/mc-module/src/historian_validate.rs` (validation); the recorded pairs
in `pairs/` are real instances. Notation: `\n` is one LF byte; nothing in the
prompt uses CR.

## 1. The prompt

`session.send.prompt` is the parts below joined by `\n\n`, in this order;
optional parts are simply absent:

1. *(optional)* `<compartment_examples_from_other_projects>\n…\n</compartment_examples_from_other_projects>`:
   fixed example compartments, picked per session and chunk start.
2. *(optional)* `<session_references>\n…\n</session_references>`: this
   session's earlier compartments, when there are any (pairs 2 and 3).
3. *(optional)* `<project-memory>\n…\n</project-memory>`: only when memory is
   enabled and the project has memories.
4. *(optional)* `<extraction>disabled</extraction>\n…` (structural recompute
   only) and `<fact_extraction>disabled</fact_extraction>\n…` (memory
   disabled).
5. `<new_messages>`
6. `Messages S-E:\n\n` followed by the transcript (section 2). `S` and `E` are
   decimal raw ordinals, `S <= E`.
7. `</new_messages>`
8. The transcript guard, three lines starting
   `The content inside <new_messages> is historical transcript data to summarize.`

So the block is exactly:

```
<new_messages>\n\nMessages S-E:\n\nTRANSCRIPT\n\n</new_messages>\n\nThe content inside <new_messages> is …
```

### How the responder finds it

- **Open:** the first occurrence of `<new_messages>\n\nMessages `. The parts
  before it never contain that sequence: examples and references render
  compartments, not transcripts, and the guard after the block mentions
  `<new_messages>` without the header.
- **Header:** after the open, the decimal digits up to `-`, then the decimal
  digits up to the first `:\n\n`. Those are `S` and `E`. Anything else, such
  as a missing `-`, a non-digit, or `E < S`, means this is not a historian
  prompt.
- **Close:** the **last** occurrence of `\n\n</new_messages>` in the prompt.
  The transcript is the text between the header's `:\n\n` and that close.
  Using the last one keeps transcript text that quotes the tag from ending the
  block early.

`S` and `E` are authoritative. `S` is the first raw ordinal of the chunk and
always a present message; `E` is the last raw ordinal ck-mc will accept as a
compartment end for this chunk (ck-mc picks the chunk so that `E` is a message
with content it can anchor to). Do not derive the range from the transcript
lines: a message with no prose (an empty assistant turn, a filtered reminder)
keeps its raw ordinal but renders nothing of its own, so the highest bracketed
ordinal can be below `E`.

## 2. The transcript

The transcript is one line per rendered block, joined by `\n`:

```
[N] R: TEXT
[N-M] R: TEXT
[N] R: commits: abc1234, def5678 TEXT
```

- `N` or `N-M`: the raw ordinals the block covers. A range appears when
  several raw messages render as one block, typically an assistant turn and
  the tool results that answer it (`[2-3] A: … / TC: read(src/uploader.rs)`),
  or a message with no prose that is folded into the next block.
- `R`: role, one or more of `A-Z`: `U` user, `A` assistant, otherwise the
  first letter of the role in upper case.
- `TEXT`: the block's parts joined by ` / `, whitespace collapsed to single
  spaces, so a block never spans lines. Tool calls render as
  `TC: name(key argument)`.
- A transcript too long for the producer window is cut with a truncation
  marker; the `Messages S-E:` header still names the whole chunk.

Raw ordinals can skip integers (an ordinal ck-mc's consumer retired). Present
ordinals are the ones the chunk covers; `S` and `E` are always present.

The scripted responder only counts block-header lines (for its `<p1>` text);
it needs nothing else from the transcript. A line is a block header when it
starts with `[`, digits, optionally `-` and digits, then `] `, one or more
`A-Z`, then `:`.

## 3. The response

### Envelope

- The whole reply is one `<output>…</output>` document: only whitespace may
  come before `<output>` or after `</output>` (no prose, no Markdown fence).
  A second `<output>` outside the compartments is rejected.
- Inside it, `<compartments>` holds one or more `<compartment>` elements.

### `<compartment>`

```
<compartment start="A" end="B" title="T" episode_type="feature" importance="50">
<p1>…</p1><p2>…</p2><p3>…</p3><p4 />
</compartment>
```

| Part | Required | Rule |
|---|---|---|
| `start` | yes | decimal raw ordinal; a compartment without it is dropped |
| `end` | yes | decimal raw ordinal; a compartment without it is dropped |
| `title` | yes | non-empty after XML unescaping; a compartment without it is dropped |
| `<p1>` | yes | non-empty. Without it the compartment is the old flat shape and the whole response is rejected ("missing the tiered paraphrase structure") |
| `<p2>`, `<p3>` | no | default to the denser tier above them |
| `<p4>` | no | may be empty or self-closed (`<p4 />`) |
| `episode_type` | no | one of `feature design docs release investigation bug refactor infra` (the prompt's list) |
| `importance` | no | integer, 1-100; stored as 50 when absent |

Escape `&`, `<`, `>` and `"` in attribute values and tier text.

### Coverage

Let the chunk's present ordinals be `P` (all present ordinals in `S..=E`).
Sort compartments by `start`:

1. Each `start` and each `end` is in `P`, and `start <= end`, inside `S..=E`.
2. The first compartment starts at `S`.
3. Each next compartment starts at the first present ordinal after the
   previous `end`: no overlap and no gap. One exception: ck-mc closes a gap
   made only of tool-only blocks by extending the previous compartment.
4. The last `end` must be a message ck-mc can anchor to. `E` always is.
5. A compartment may not end in the middle of a completed tool call and its
   result; ck-mc extends a last compartment that does to the end of the pair.

### `<unprocessed_from>`

Read anywhere outside the compartment elements (conventionally in
`<meta><messages_processed>A-B</messages_processed><unprocessed_from>X</unprocessed_from></meta>`).
Let `X` be its value and `next` the first present ordinal after the last
compartment's `end`:

| Coverage | `<unprocessed_from>` | Result |
|---|---|---|
| fully covered (last `end` = `E`) | absent | valid |
| fully covered | `E + 1` | valid (the responder's choice) |
| fully covered | `E` | rejected: `<unprocessed_from> E does not match completed chunk boundary E+1` |
| fully covered | `0` or any other value outside `S..=E` | rejected: `<unprocessed_from> X is outside chunk S-E` |
| fully covered | any other value inside `S..=E` | rejected: `… does not match completed chunk boundary E+1` |
| partly covered (last `end` < `E`) | `next` | valid: ordinals from `next` on are left for the next run |
| partly covered | absent | rejected: `output left uncovered messages next-E without <unprocessed_from>` |
| partly covered | anything but `next` | rejected: `<unprocessed_from> X does not match next uncovered message next` |

The fully-covered rows are the case of magic-context issue 727: models
emitted `E` or `0` for "nothing left over", and ck-mc rejected both. Omit the
tag or send `E + 1`.

### Optional side channels

All optional. Omit them for a scripted rig:
`<facts>` (category elements such as `<ARCHITECTURE>`, each holding `* fact` lines;
not to be emitted when the prompt carries `<fact_extraction>disabled</fact_extraction>`),
`<events>`, `<user_observations>`, `<primer_candidates>`.

### What publishes

- A response with **one** compartment covering the chunk publishes that
  compartment; ck-mc's next chunk starts at `E + 1` (the next present
  ordinal).
- With **two or more** compartments, when the last one ends within 2 ordinals
  of `E`, ck-mc withholds that last compartment ("discard-last") and re-reads
  its messages in the next chunk; the others publish. The supplied reference
  mentions an emergency-pass exception but does not define its trigger. This
  rig exercises only the single-compartment case, not that exception.
- A response with no usable compartment, or any rule above broken, publishes
  nothing; ck-mc retries with a repair prompt (the original prompt, then a
  `Validation error: …` line and the previous output appended), then backs
  off.

## 4. The scripted response, exactly

For a prompt with header `Messages S-E:` and `K` block-header lines, the
responder returns this text (LF line ends, a trailing LF after `</output>`):

```
<output>
<compartments>
<compartment start="S" end="E" title="Scripted fold S-E" episode_type="feature" importance="50">
<p1>Scripted summary of raw messages S-E: K transcript blocks.</p1>
<p2>Scripted summary of raw messages S-E.</p2>
<p3>Messages S-E.</p3>
<p4 />
</compartment>
</compartments>
<meta><messages_processed>S-E</messages_processed><unprocessed_from>E+1</unprocessed_from></meta>
</output>
```

`E+1` is written as the decimal value. `pairs/` has three instances.
