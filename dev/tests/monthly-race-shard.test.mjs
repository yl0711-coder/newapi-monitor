import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {monthlyRaceShards} from '../monthly-race-shard.mjs';

test('monthly race inventory is covered exactly once, including examples, fuzz seeds and new tests', () => {
  const names = ['TestOne', 'TestTwo', 'TestThree', 'TestNewFeature', 'ExampleBill', 'FuzzAmounts'];
  const listing = `${names.join('\n')}\nok example.invalid/monthlydraft 0.1s\n`;
  const result = monthlyRaceShards(listing);
  assert.equal(result.count, names.length);
  assert.equal(new Set(result.groups.flat()).size, names.length);
  for (const name of names) assert.equal(result.patterns.filter(p => new RegExp(p).test(name)).length, 1);
  for (const pattern of result.patterns) assert.equal(new RegExp(pattern).test('TestOneExtra'), false);
  assert.deepEqual(monthlyRaceShards([...names].reverse().join('\n')), result);
});

test('monthly race shard rejects empty, failed and duplicate inventories', () => {
  for (const listing of ['', 'FAIL example.invalid/monthlydraft [setup failed]', 'TestOne\nTestTwo\nTestThree',
    'TestOne\nTestTwo\nTestThree\nTestFour\nTestOne']) assert.throws(() => monthlyRaceShards(listing));
});

test('release gates wait for all monthly race shards without weakening the time limit', () => {
  const ci = readFileSync(new URL('../../.github/workflows/ci.yml', import.meta.url), 'utf8');
  const job = ci.split('\n  race-monthly:\n')[1]?.split(/^  [\w-]+:/m)[0] ?? '';
  assert.match(job, /fail-fast: false/);
  assert.match(job, /shard: \[0, 1, 2, 3\]/);
  assert.match(job, /set -euo pipefail/);
  assert.match(job, /go test -list \. \.\/internal\/monthlydraft/);
  assert.match(job, /node dev\/monthly-race-shard\.mjs "\$MONTHLY_SHARD"/);
  assert.match(job, /go test -race -count=1 -timeout 20m -run "\$pattern" \.\/internal\/monthlydraft/);
  assert.doesNotMatch(job, /continue-on-error|\|\|\s*true|(?:^|\s)-short(?:\s|$)/);
  assert.ok(ci.includes("grep -Ev '/(monitor$|internal/monthlydraft$)'"));
  for (const name of ['image-security', 'build-push', 'build-nginx-ops-tools']) {
    const body = ci.split(`\n  ${name}:\n`)[1]?.split(/^  [\w-]+:/m)[0] ?? '';
    assert.match(body, /needs: \[[^\n]*race-monthly[^\n]*\]/);
  }
});
