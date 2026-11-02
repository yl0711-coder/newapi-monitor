import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';

export const monthlyRaceShardCount = 12;
export const monthlyRaceShardTargetSeconds = 16 * 60;
const recordedTimings = JSON.parse(readFileSync(new URL('./monthly-race-timings.json', import.meta.url), 'utf8'));

function validateTimings(timings) {
  assert.equal(timings?.format_version, 1, 'unsupported monthly timing format');
  assert.ok(Number.isInteger(timings.fallback_seconds) && timings.fallback_seconds > 0 && timings.fallback_seconds <= monthlyRaceShardTargetSeconds,
    'invalid conservative default for new monthly tests');
  assert.ok(timings.test_seconds && typeof timings.test_seconds === 'object' && !Array.isArray(timings.test_seconds), 'missing monthly test timings');
  for (const [name, seconds] of Object.entries(timings.test_seconds)) {
    assert.ok(/^(?:Test|Example|Fuzz)\S*$/.test(name), 'invalid monthly timing test name');
    assert.ok(Number.isInteger(seconds) && seconds > 0 && seconds <= 20 * 60, 'invalid monthly test duration');
  }
}

// The ACTUAL race-built inventory, never the historical timing table, selects
// tests. Deterministic longest-first packing balances measured WORK, not counts.
// Unknown/new tests receive a conservative estimate and still execute once.
export function monthlyRaceShards(listing, timings = recordedTimings) {
  const names = listing.split(/\r?\n/).filter(line => /^(?:Test|Example|Fuzz)\S*$/.test(line));
  assert.ok(names.length >= monthlyRaceShardCount, 'monthly test inventory is missing or incomplete');
  assert.equal(new Set(names).size, names.length, 'duplicate monthly test inventory');
  validateTimings(timings);
  const weight = name => Object.hasOwn(timings.test_seconds, name) ? timings.test_seconds[name] : timings.fallback_seconds;
  const lexical = (a, b) => a < b ? -1 : a > b ? 1 : 0;
  const groups = Array.from({length: monthlyRaceShardCount}, () => []);
  const estimatedSeconds = Array.from({length: monthlyRaceShardCount}, () => 0);
  for (const name of [...names].sort((a, b) => weight(b) - weight(a) || lexical(a, b))) {
    let index = 0;
    for (let i = 1; i < groups.length; i++) {
      if (estimatedSeconds[i] < estimatedSeconds[index] ||
        (estimatedSeconds[i] === estimatedSeconds[index] && groups[i].length < groups[index].length)) index = i;
    }
    groups[index].push(name);
    estimatedSeconds[index] += weight(name);
  }
  for (const group of groups) assert.ok(group.length, 'empty monthly race shard');
  assert.ok(Math.max(...estimatedSeconds) <= monthlyRaceShardTargetSeconds,
    'predicted monthly shard exceeds 16-minute target; refresh timings or explicitly review capacity before running');
  const escape = value => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const patterns = groups.map(group => `^(${group.map(escape).join('|')})$`);
  for (const name of names) assert.equal(patterns.filter(pattern => new RegExp(pattern).test(name)).length, 1);
  return {count: names.length, groups, patterns, estimatedSeconds,
    knownTimingCount: names.filter(name => Object.hasOwn(timings.test_seconds, name)).length};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const raw = process.argv[2];
  const index = Number(raw);
  assert.ok(raw !== undefined && String(index) === raw && Number.isInteger(index) && index >= 0 && index < monthlyRaceShardCount,
    `monthly race shard must be an integer from 0 to ${monthlyRaceShardCount - 1}`);
  const result = monthlyRaceShards(readFileSync(0, 'utf8'));
  console.error(`Monthly race coverage: ${result.count} tests, shard sizes ${result.groups.map(group => group.length).join(',')}`);
  console.error(`Estimated shard seconds: ${result.estimatedSeconds.join(',')}; measured ${result.knownTimingCount}/${result.count}, new tests use conservative defaults`);
  console.log(result.patterns[index]);
}
