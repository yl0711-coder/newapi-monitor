import test from 'node:test';
import assert from 'node:assert/strict';
import {classifyRaceFailures, raceRerunMaxTests} from '../race-failures.mjs';

const ev = (Action, Test, Output) => JSON.stringify({Action, Package: 'example/monitor', ...(Test ? {Test} : {}), ...(Output ? {Output} : {})});
const report = (lines, exitedOK = false) => ({text: lines.join('\n'), exitedOK});

test('a few ordinary failed tests may be re-run once', () => {
  const r = classifyRaceFailures([
    report([ev('run', 'TestA'), ev('fail', 'TestA/sub'), ev('fail', 'TestA'), ev('fail')]),
    report([ev('pass', 'TestB'), ev('pass')], true),
    report([ev('fail', 'TestC'), ev('fail')]),
  ]);
  assert.deepEqual(r, {tests: ['TestA', 'TestC'], rerunAllowed: true, reasons: [], blocking: []});
});

test('data races, panics, timeouts and unidentified failures are never re-run', () => {
  const cases = [
    [report([ev('output', 'TestA', 'WARNING: DATA RACE\n'), ev('fail', 'TestA'), ev('fail')]), /data race/],
    [report([ev('output', 'TestA', 'testing.go:1617: race detected during execution of test\n'), ev('fail', 'TestA'), ev('fail')]), /data race/],
    [report([ev('output', 'TestA', 'panic: runtime error: index out of range\n'), ev('fail', 'TestA'), ev('fail')]), /panicked/],
    [report([ev('output', '', 'panic: test timed out after 25m0s\n'), ev('fail', 'TestA'), ev('fail')]), /timed out/],
    [report([ev('output', '', 'FAIL\n'), ev('fail')]), /without a failing test/],
    [report([ev('pass', 'TestA')], false), /without a failing test/],
  ];
  for (const [input, reason] of cases) {
    const r = classifyRaceFailures([input]);
    assert.equal(r.rerunAllowed, false);
    assert.match(r.reasons.join('; '), reason);
  }
});

test('more than the limit of failed tests is a systemic failure, not flakiness', () => {
  const lines = Array.from({length: raceRerunMaxTests + 1}, (_, i) => ev('fail', `TestMany${i}`));
  const r = classifyRaceFailures([report([...lines, ev('fail')])]);
  assert.equal(r.rerunAllowed, false);
  assert.match(r.reasons.join(), /more than/);
  const ok = classifyRaceFailures([report([...lines.slice(0, raceRerunMaxTests), ev('fail')])]);
  assert.equal(ok.rerunAllowed, true);
});

test('nothing failed means nothing to re-run', () => {
  assert.equal(classifyRaceFailures([report([ev('pass', 'TestA'), ev('pass')], true)]).rerunAllowed, false);
});

test('race-advisory runs are blocked only by data races, panics, hangs and unidentified failures', () => {
  const assertionOnly = classifyRaceFailures([report([...Array.from({length: 8}, (_, i) => ev('fail', `TestSlow${i}`)), ev('fail')])]);
  assert.deepEqual(assertionOnly.blocking, []);
  assert.equal(assertionOnly.rerunAllowed, false); // too many to re-run, but not blocking in advisory mode
  const raced = classifyRaceFailures([report([ev('output', 'TestA', 'WARNING: DATA RACE\n'), ev('fail', 'TestA'), ev('fail')])]);
  assert.deepEqual(raced.blocking, ['data race reported']);
});
