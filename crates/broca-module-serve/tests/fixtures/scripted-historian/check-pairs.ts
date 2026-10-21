/**
 * Checks `scripted-historian.ts` against the recorded pairs: for every
 * `pairs/pair-N.prompt.txt`, the module's answer must equal
 * `pairs/pair-N.response.xml` byte for byte. ck-mc's own test
 * (`scripted_historian_pairs` in the mc-module crate) validates those same
 * responses and checks the prompts are what ck-mc builds.
 *
 * Run: bun check-pairs.ts
 */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { scriptedHistorianResponse } from "./scripted-historian";

const dir = join(import.meta.dir, "pairs");
let failures = 0;
let checked = 0;
for (const name of readdirSync(dir).filter((n) => n.endsWith(".prompt.txt")).sort()) {
    const prompt = readFileSync(join(dir, name), "utf8");
    const expected = readFileSync(join(dir, name.replace(".prompt.txt", ".response.xml")), "utf8");
    const actual = scriptedHistorianResponse(prompt);
    checked++;
    if (actual !== expected) {
        failures++;
        console.error(`MISMATCH ${name}\n--- expected\n${expected}\n--- actual\n${actual}`);
    } else {
        console.log(`ok ${name}`);
    }
}
if (scriptedHistorianResponse("not a historian prompt") !== null) {
    failures++;
    console.error("MISMATCH a non-historian prompt was answered");
}
console.log(`${checked} pairs checked, ${failures} failures`);
process.exit(failures === 0 && checked > 0 ? 0 : 1);
