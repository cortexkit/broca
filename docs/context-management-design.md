# Per-session context management

The `context_management` field and its admission rules are described in
`docs/module-surface.md` §4. `"compaction"` is refused at admission until that mode
is available.

**Current behaviour of `{"transform": ...}`:** broca does not serve it; per-step role contracts
(step-transform and compaction providers) take its place. It still decodes, because older
`RunStarted` records carry it. Admission, resume and boot refuse it by name with
`context_management_unavailable`, rather than running the session as `"none"`. Sections below
that describe the transform mode are the design record, not current behaviour.

Context management is selected per session and frozen at admission. Legacy records
may name a process-level transform, but current admission refuses transform mode.

## Shape

A `context_management` field on `session.send`:

    {"transform": "<module id>"}   // prompt reduction through a transform module (Magic Context)
    "none"                          // no management: a run that reaches the window ends with
                                    // a context_overflow error, as today
    "compaction"                    // a broca-native classic compaction: summarise-and-continue,
                                     // a summarise-and-continue mode. Refused at admission
                                    // with a clear message until the mode exists.

Rules:

- **Frozen at admission onto `RunStarted`,** like the tool surface, cache policy and stop
  conditions. Resume renders from the frozen value, never from process configuration.
- **The first episode binds the session.** A later send that omits the field inherits the frozen
  value, the way an omitted tool surface is inherited.
- **A later send with a different value is refused permanently,** the way a changed system prompt
  is (`SystemPromptChanged`). Switching would rewrite the session's cached prefix once, and the
   transform would see a history it didn't shape.
- **Default for a new session with no field**, first match wins: a per-project default from
  `broca.jsonc` (keyed by the session's `project_root`), then a global default from the same
  file, then `BROCA_TRANSFORM_MODULE` (a transform module when set), then `"none"`. With no
  config and the env var unset, every existing caller stays byte-identical. A default applies
  only when a session is first admitted: changing the config later never changes a session that
  already froze its value.
- **A session created before this change** has no frozen value, and keeps what it had: the
  process default at its admission. Record which, so resume doesn't depend on today's env.
  Nothing durable recorded that default (the transform is never WAL-recorded, and
  `context_limit` is frozen whether or not a transform ran), so the settled rule is: such a
  session's next new episode binds to the running process's `BROCA_TRANSFORM_MODULE` (`"none"`
  when unset) and freezes it; `broca.jsonc` never reaches it, because a
  default applies only at a first admission and this session had one; a differing explicit
  value is refused like any change. A resumed mid-run episode of such a session cannot gain a
  frozen value and follows the env exactly as before.

## Legacy transform compatibility

- One broca process calling the transform for some sessions and not others is fine: its state is
  keyed per session (conversation key), and a session it never sees costs nothing.
- The module already accepts broca's `owned-broca` serializer profile per request, so nothing on
  its side assumes a process-wide switch.


## Constraint on the compaction mode

Magic-context keys tags, compartment boundaries and ordinal ranges on each message's
`meta.ordinal` and `meta.harness_id`, which broca sets from the session's durable message
ordinal and id. Those must stay append-only and never reused. So when broca's own
`"compaction"` mode replaces history with a summary, the summary gets a FRESH ordinal and id,
and no retained message's ordinal changes. Reusing or renumbering ordinals would silently
misattribute magic-context's state for any session that later runs the transform, and would
break `session.read` cursors.

## Unavailable modes

- `"compaction"` is accepted by the parser from day one and refused at admission until the mode
  is built, so a caller learns it's unavailable instead of silently getting `"none"`. Any
  unknown value is refused the same way: falling back to `"none"` would let a run hit the window
  its caller thought was managed.
- Transform module ids are no longer validated against a configured set. Every transform
  value is refused by name (`context_management_unavailable`), never run as `"none"`.
