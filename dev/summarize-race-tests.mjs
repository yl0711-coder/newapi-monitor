import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

// Report event metadata only: do not reproduce fixture data or raw test output.
export function summarizeRaceTests(jsonl, shard) {
  if (!/^[a-z0-9-]+$/.test(shard)) throw new Error('Invalid shard name');
  const active = new Set();
  const completed = [];
  const failed = new Set();
  let packageResult = 'unfinished';
  let packageSeconds = null;
  let events = 0;
  for (const line of jsonl.split(/\r?\n/)) {
    if (!line.trim()) continue;
    let event;
    try { event = JSON.parse(line); } catch { continue; } // Preserve a report after interrupted output.
    if (!event || typeof event !== 'object' || typeof event.Action !== 'string') continue;
    events++;
    const name = event.Test;
    if (typeof name === 'string') {
      if (event.Action === 'run') active.add(name);
      if (['pass', 'fail', 'skip'].includes(event.Action)) {
        active.delete(name);
        if (event.Action === 'fail') failed.add(name);
        if (!name.includes('/')) completed.push({ name, action: event.Action, seconds: Number.isFinite(event.Elapsed) ? event.Elapsed : 0 });
      }
    } else if (['pass', 'fail'].includes(event.Action)) {
      packageResult = event.Action;
      packageSeconds = Number.isFinite(event.Elapsed) ? event.Elapsed : null;
    }
  }
  if (!events) throw new Error('No Go test events found');
  const safe = value => String(value).replace(/[\r\n|`<>]/g, ' ');
  const counts = ['pass', 'fail', 'skip'].map(action => `${action}: ${completed.filter(t => t.action === action).length}`).join(' / ');
  const lines = [`## Monitor race: ${shard}`, '', `Package: ${packageResult}${packageSeconds === null ? '' : ` (${packageSeconds.toFixed(2)}s)`}`, '', `Top-level tests — ${counts}`, '', '### Slowest completed top-level tests', '', '| Test | Result | Seconds |', '|---|---|---:|'];
  for (const test of [...completed].sort((a, b) => b.seconds - a.seconds).slice(0, 15)) {
    lines.push(`| ${safe(test.name)} | ${test.action} | ${test.seconds.toFixed(2)} |`);
  }
  for (const [title, names] of [['Failed tests', failed], ['Unfinished tests (including interrupted subtests)', active]]) {
    if (names.size) lines.push('', `### ${title}`, '', ...[...names].slice(0, 30).map(name => `- ${safe(name)}`));
  }
  lines.push('', 'Durations include test setup/cleanup. Parent and child durations must not be added together.', 'Full event evidence is retained in the shard artifact for 7 days.', '');
  return lines.join('\n');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    console.log(summarizeRaceTests(readFileSync(process.argv[2], 'utf8'), process.argv[3]));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
