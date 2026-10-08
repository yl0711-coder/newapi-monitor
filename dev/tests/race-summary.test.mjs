import assert from 'node:assert/strict';
import test from 'node:test';
import { summarizeRaceTests } from '../summarize-race-tests.mjs';

const encode = events => events.map(event => JSON.stringify(event)).join('\n');

test('summary reports timings without counting nested duration twice or revealing output', () => {
  const result = summarizeRaceTests(encode([
    { Action: 'run', Test: 'TestParent' },
    { Action: 'output', Test: 'TestParent', Output: 'PRIVATE_FIXTURE_VALUE' },
    { Action: 'pass', Test: 'TestParent/child', Elapsed: 4 },
    { Action: 'pass', Test: 'TestParent', Elapsed: 5 },
    { Action: 'skip', Test: 'TestOptIn', Elapsed: 0 },
    { Action: 'pass', Elapsed: 6 },
  ]), 'f-other');
  assert.match(result, /Package: pass \(6.00s\)/);
  assert.match(result, /pass: 1 \/ fail: 0 \/ skip: 1/);
  assert.match(result, /TestParent \| pass \| 5.00/);
  assert.doesNotMatch(result, /PRIVATE_FIXTURE_VALUE|TestParent\/child|Unfinished tests/);
});

test('timeouts retain unfinished tests and package failure, even with truncated trailing JSON', () => {
  const result = summarizeRaceTests(encode([
    { Action: 'run', Test: 'TestSlow' },
    { Action: 'run', Test: 'TestSlow/lock' },
    { Action: 'fail', Test: 'TestEarlier', Elapsed: 1 },
    { Action: 'fail', Elapsed: 1200 },
  ]) + '\n{"Action":', 'f-gift-scope');
  assert.match(result, /Package: fail \(1200.00s\)/);
  assert.match(result, /Failed tests\n\n- TestEarlier/);
  assert.match(result, /Unfinished tests[^]*- TestSlow\n- TestSlow\/lock/);
});

test('invalid evidence fails and unfinished packages never claim success', () => {
  assert.throws(() => summarizeRaceTests('broken', 'f-other'), /No Go test events/);
  assert.throws(() => summarizeRaceTests('{}', '<unsafe>'), /Invalid shard/);
  assert.match(summarizeRaceTests(encode([{ Action: 'start' }]), 'f-other'), /Package: unfinished/);
});
