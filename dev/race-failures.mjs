import {readFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';

// Classifies the failures of one CI race job (Go test JSON events from its
// processes) and decides whether a single isolated re-run may be attempted.
// A re-run is allowed only for a few failed top-level tests whose processes
// otherwise completed normally: never for data races, panics, timeouts, or a
// failed process whose failing test cannot be identified (see docs/CI-规范.md).
export const raceRerunMaxTests = 5;

export function classifyRaceFailures(reports) {
  const failed = new Set();
  let dataRace = false;
  let panic = false;
  let timedOut = false;
  let unexplained = false;
  for (const {text, exitedOK} of reports) {
    const ownFailures = new Set();
    let packageFailed = false;
    for (const line of text.split(/\r?\n/)) {
      if (!line.trim()) continue;
      let event;
      try { event = JSON.parse(line); } catch { continue; }
      const output = typeof event.Output === 'string' ? event.Output : '';
      if (output.includes('WARNING: DATA RACE') || output.includes('race detected during execution of test')) dataRace = true;
      if (/^panic: test timed out/m.test(output)) timedOut = true;
      else if (/^panic: /m.test(output)) panic = true;
      if (event.Action === 'fail') {
        if (typeof event.Test === 'string') ownFailures.add(event.Test.split('/')[0]);
        else packageFailed = true;
      }
    }
    for (const name of ownFailures) failed.add(name);
    if ((!exitedOK || packageFailed) && ownFailures.size === 0) unexplained = true;
  }
  const tests = [...failed].sort();
  // Failures that never come from machine speed: they always fail the job,
  // including in race-advisory runs (docs/CI-规范.md §3).
  const blocking = [];
  if (dataRace) blocking.push('data race reported');
  if (panic) blocking.push('test panicked');
  if (timedOut) blocking.push('test binary timed out');
  if (unexplained) blocking.push('a process failed without a failing test');
  const reasons = [...blocking];
  if (tests.length > raceRerunMaxTests) reasons.push(`more than ${raceRerunMaxTests} tests failed`);
  return {tests, rerunAllowed: tests.length > 0 && reasons.length === 0, reasons, blocking};
}

const escape = value => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// CLI: node dev/race-failures.mjs <report.jsonl>:<exit-status> ...
// Prints JSON {tests, rerunAllowed, reasons, pattern}.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const reports = process.argv.slice(2).map(arg => {
      const at = arg.lastIndexOf(':');
      return {text: readFileSync(arg.slice(0, at), 'utf8'), exitedOK: arg.slice(at + 1) === '0'};
    });
    const result = classifyRaceFailures(reports);
    console.log(JSON.stringify({...result, pattern: result.tests.length ? `^(${result.tests.map(escape).join('|')})$` : ''}));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
