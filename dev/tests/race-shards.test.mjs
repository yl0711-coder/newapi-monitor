import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { verifyRaceShards } from '../check-race-shards.mjs';

const workflow = readFileSync(new URL('../../.github/workflows/ci.yml', import.meta.url), 'utf8');
const names = ['TestAlpha', 'TestGroup', 'TestOperations', 'TestZulu', 'Test_Regression', 'Test2026', 'Test', 'Test中文', 'Example', 'ExampleMonitor', 'FuzzDecode'];

test('actual CI matrix covers ordinary and non-letter tests, examples and fuzz seeds', () => {
  assert.deepEqual(verifyRaceShards(workflow, names.concat('BenchmarkRead', 'ok example/monitor 0.01s').join('\n')), { tests: 11, counts: [1, 1, 1, 8] });
});

test('shard gate rejects missing, duplicate, overlapping and empty coverage', () => {
  assert.throws(() => verifyRaceShards(workflow, ''), /Empty/);
  assert.throws(() => verifyRaceShards(workflow, names.concat('TestAlpha').join('\n')), /duplicate/);
  assert.throws(() => verifyRaceShards(workflow, 'TestAlpha'), /Empty monitor race shard/);
  assert.throws(() => verifyRaceShards(workflow.replace("'^(Test[T-Z]|Test[^A-Z]|Test$|Example|Fuzz)'", "'^Test[T-Z]'"), names.join('\n')), /belongs to 0/);
  assert.throws(() => verifyRaceShards(workflow.replace("'^Test[G-N]'", "'^Test[A-N]'"), names.join('\n')), /belongs to 2/);
  assert.throws(() => verifyRaceShards('no matrix', names.join('\n')), /No monitor/);
});
