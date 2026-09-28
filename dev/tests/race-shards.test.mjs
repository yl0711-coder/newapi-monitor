import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { readRaceShards, verifyRaceShards } from '../check-race-shards.mjs';

const workflow = readFileSync(new URL('../../.github/workflows/ci.yml', import.meta.url), 'utf8');
const names = ['TestAlpha', 'TestChannel', 'TestCloudWatch', 'TestDelta', 'TestEcho', 'TestFinance', 'TestFinanceGiftNewFeature', 'TestFinanceGiftHandoffNewFeature', 'TestFinanceGiftScopeNewFeature', 'TestGroup', 'TestOperations', 'TestStability', 'TestZulu', 'Test_Regression', 'Test2026', 'Test', 'Test中文', 'Example', 'ExampleMonitor', 'FuzzDecode'];

test('actual CI matrix covers ordinary and non-letter tests, examples and fuzz seeds', () => {
  assert.deepEqual(verifyRaceShards(workflow, names.concat('BenchmarkRead', 'ok example/monitor 0.01s').join('\n')), { tests: 20, counts: [1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 8] });
});

test('finance exclusions route tests to another shard rather than omit coverage', () => {
  assert.throws(() => verifyRaceShards(workflow.replace("            skip: '^TestFinanceGift'\n", ''), names.join('\n')), /belongs to 2/);
  assert.throws(() => verifyRaceShards(workflow.replace("pattern: '^TestFinanceGiftScope'", "pattern: '^TestMissingScope'"), names.join('\n')), /belongs to 0/);
  const shards = readRaceShards(workflow);
  for (const suffix of ['', 'Boundary', 'Future2027', '中文']) {
    for (const prefix of ['TestFinanceGift', 'TestFinanceGiftScope', 'TestFinanceGiftHandoff']) {
      const name = prefix + suffix;
      assert.equal(shards.filter(s => s.pattern.test(name) && !s.skip?.test(name)).length, 1, name);
    }
  }
});

test('invalid shard identifiers and subtest routing fail closed', () => {
  assert.throws(() => readRaceShards(workflow.replace('shard: f-other', 'shard: a-b')), /Duplicate/);
  assert.throws(() => readRaceShards(workflow.replace("pattern: '^TestF'", "pattern: '^TestF/case'")), /Subtest/);
  assert.throws(() => readRaceShards(workflow.replace("pattern: '^TestF'", "missing: '^TestF'")), /Missing pattern/);
});

test('channel and stability splits keep old A-C and O-S coverage without overlaps', () => {
  assert.throws(() => verifyRaceShards(workflow.replace("            skip: '^TestChannel'\n", ''), names.join('\n')), /belongs to 2/);
  assert.throws(() => verifyRaceShards(workflow.replace("pattern: '^TestS'", "pattern: '^TestMissingStability'"), names.join('\n')), /belongs to 0/);
  const shards = readRaceShards(workflow);
  for (const name of ['TestA', 'TestB', 'TestC', 'TestChannel', 'TestChannelFuture', 'TestCloudWatch', 'TestCustomer', 'TestO', 'TestP', 'TestQ', 'TestR', 'TestS', 'TestStabilityFuture']) {
    assert.equal(shards.filter(s => s.pattern.test(name) && !s.skip?.test(name)).length, 1, name);
  }
});

test('CI keeps race detection, bounded execution and a failing pipeline on test errors', () => {
  assert.match(workflow, /go test -race -count=1 -json -timeout 20m -run "\$TEST_PATTERN" -skip "\$TEST_SKIP"/);
  assert.match(workflow, /set -o pipefail/);
  assert.match(workflow, /Summarize slow tests even on failure\n\s+if: always\(\)/);
});

test('shard gate rejects missing, duplicate, overlapping and empty coverage', () => {
  assert.throws(() => verifyRaceShards(workflow, ''), /Empty/);
  assert.throws(() => verifyRaceShards(workflow, names.concat('TestAlpha').join('\n')), /duplicate/);
  assert.throws(() => verifyRaceShards(workflow, 'TestAlpha'), /Empty monitor race shard/);
  assert.throws(() => verifyRaceShards(workflow.replace("'^(Test[T-Z]|Test[^A-Z]|Test$|Example|Fuzz)'", "'^Test[T-Z]'"), names.join('\n')), /belongs to 0/);
  assert.throws(() => verifyRaceShards(workflow.replace("'^Test[G-N]'", "'^Test[A-N]'"), names.join('\n')), /belongs to 2/);
  assert.throws(() => verifyRaceShards('no matrix', names.join('\n')), /No monitor/);
});
