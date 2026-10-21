/**
 * Scripted historian for test rigs that have no historian model.
 *
 * ck-mc (Magic Context's compaction module) summarizes old conversation into
 * "compartments" by sending a historian prompt to a model. Under the async
 * compaction lane that prompt reaches the session's own runner as
 * `session.send` with `system` (the historian system prompt) and `prompt`
 * (the user message this module reads). A rig whose provider is scripted can
 * call `scriptedHistorianResponse(prompt)` and answer with the returned text:
 * one compartment covering the whole chunk, which ck-mc's validation accepts
 * and publishes.
 *
 * Plain TypeScript, no imports. The parsing is deliberately byte-level (no
 * regular expressions with multiline flags, no Unicode-sensitive splitting)
 * so a port in another language produces identical bytes; `pairs/` holds the
 * recorded prompts and the exact responses this module gives.
 */

/** The prompt's `<new_messages>` opening and the `Messages S-E:` header after it. */
const OPEN = "<new_messages>\n\nMessages ";
/** The block's closing tag, preceded by the blank line ck-mc writes. */
const CLOSE = "\n\n</new_messages>";

export interface HistorianChunk {
    /** First raw ordinal of the chunk (`S` in `Messages S-E:`). */
    start: number;
    /** Last raw ordinal of the chunk (`E` in `Messages S-E:`). */
    end: number;
    /** Lines of the transcript that start a rendered block (`[N] U:`, `[N-M] A:`). */
    blocks: number;
}

/** Digits only, as a non-negative integer; `null` for anything else. */
function digits(text: string): number | null {
    if (text.length === 0) return null;
    for (let i = 0; i < text.length; i++) {
        const c = text.charCodeAt(i);
        if (c < 48 || c > 57) return null;
    }
    return Number(text);
}

/** `[N] R:` or `[N-M] R:` at the start of `line`, `R` one or more of A-Z. */
export function isBlockHeader(line: string): boolean {
    if (!line.startsWith("[")) return false;
    const rest = line.slice(1);
    const close = rest.indexOf("] ");
    if (close < 0) return false;
    const range = rest.slice(0, close);
    const dash = range.indexOf("-");
    const rangeOk =
        dash < 0
            ? digits(range) !== null
            : digits(range.slice(0, dash)) !== null && digits(range.slice(dash + 1)) !== null;
    const role = rest.slice(close + 2);
    let letters = 0;
    while (letters < role.length) {
        const c = role.charCodeAt(letters);
        if (c < 65 || c > 90) break;
        letters++;
    }
    return rangeOk && letters > 0 && role.charAt(letters) === ":";
}

/**
 * Read the chunk out of a historian prompt, or `null` when `prompt` is not
 * one (no `<new_messages>` block with a `Messages S-E:` header).
 *
 * The first `<new_messages>\n\nMessages ` opens the block: the reference and
 * memory blocks before it never contain that sequence, and the transcript
 * guard after the block names `<new_messages>` without the header. The LAST
 * `\n\n</new_messages>` closes it, so transcript text quoting the tag cannot
 * end it early.
 */
export function parseHistorianPrompt(prompt: string): HistorianChunk | null {
    const open = prompt.indexOf(OPEN);
    if (open < 0) return null;
    const rest = prompt.slice(open + OPEN.length);
    const colon = rest.indexOf(":\n\n");
    if (colon < 0) return null;
    const header = rest.slice(0, colon);
    const dash = header.indexOf("-");
    if (dash < 0) return null;
    const start = digits(header.slice(0, dash));
    const end = digits(header.slice(dash + 1));
    if (start === null || end === null || end < start) return null;
    const close = rest.lastIndexOf(CLOSE);
    if (close < 0) return null;
    const body = close >= colon + 3 ? rest.slice(colon + 3, close) : "";
    const blocks = body.split("\n").filter(isBlockHeader).length;
    return { start, end, blocks };
}

/**
 * The historian's answer for `prompt`: one compartment from `S` to `E` with
 * all four paraphrase tiers, and `<unprocessed_from>E+1</unprocessed_from>`,
 * the value ck-mc accepts for a fully covered chunk. `null` when `prompt` is
 * not a historian prompt.
 */
export function scriptedHistorianResponse(prompt: string): string | null {
    const chunk = parseHistorianPrompt(prompt);
    if (!chunk) return null;
    const { start, end, blocks } = chunk;
    return [
        "<output>",
        "<compartments>",
        `<compartment start="${start}" end="${end}" title="Scripted fold ${start}-${end}" episode_type="feature" importance="50">`,
        `<p1>Scripted summary of raw messages ${start}-${end}: ${blocks} transcript blocks.</p1>`,
        `<p2>Scripted summary of raw messages ${start}-${end}.</p2>`,
        `<p3>Messages ${start}-${end}.</p3>`,
        "<p4 />",
        "</compartment>",
        "</compartments>",
        `<meta><messages_processed>${start}-${end}</messages_processed><unprocessed_from>${end + 1}</unprocessed_from></meta>`,
        "</output>",
        "",
    ].join("\n");
}
