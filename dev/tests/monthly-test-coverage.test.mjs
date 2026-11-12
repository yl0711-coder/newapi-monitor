import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {checkMonthlyTestCoverage, checkMonthlyNativeResults, monthlyNativeOnlyTests, monthlyBoundedRaceTests} from '../check-monthly-test-coverage.mjs';

const names = ['TestMoney', 'TestOwner', 'ExampleBill', 'FuzzAmounts', ...monthlyBoundedRaceTests];
const listing = values => `${values.join('\n')}\nok example.invalid/monthlydraft 0.1s\n`;

test('all monthly tests are accounted for by race plus the mandatory real-deadline scale gate', () => {
  const result = checkMonthlyTestCoverage(listing([...names, ...monthlyNativeOnlyTests]), listing(names));
  assert.equal(result.allCount, names.length + monthlyNativeOnlyTests.length);
  assert.equal(result.raceCount, names.length);
  assert.deepEqual(result.nativeOnly, monthlyNativeOnlyTests);
  for (const name of monthlyNativeOnlyTests) assert.ok(new RegExp(result.nativePattern).test(name));
  assert.equal(new RegExp(result.nativePattern).test(`${monthlyNativeOnlyTests[0]}Extra`), false);
});

test('coverage gate rejects missing scale, missing race, unknown exemptions and duplicate inventories', () => {
  const all = [...names, ...monthlyNativeOnlyTests];
  const invalid = [
    [names, names],
    [all, names.slice(1)],
    [all, [...names, ...monthlyNativeOnlyTests]],
    [[...all, 'TestUnregisteredNativeOnly'], names],
    [all, [...names, 'TestUnregisteredRaceOnly']],
    [[...all, names[0]], names],
    [all, [...names, names[0]]],
    ...monthlyBoundedRaceTests.map(testName => [all, names.filter(name => name !== testName)]),
    [[], []],
  ];
  for (const [a, r] of invalid) assert.throws(() => checkMonthlyTestCoverage(listing(a), listing(r)));
});

test('mandatory native scale must actually execute and pass; skipped, empty and truncated runs fail', () => {
  const encode = rows => rows.map(row => JSON.stringify(row)).join('\n');
  const completed = monthlyNativeOnlyTests.map(name => ({Action:'pass',Test:name}));
  const passed = [...completed, {Action:'pass'}];
  assert.deepEqual(checkMonthlyNativeResults(encode(passed)), {passed:monthlyNativeOnlyTests.length});
  for (const rows of [[], [{Action:'pass'}], completed,
    ...monthlyNativeOnlyTests.flatMap(name => [
      passed.filter(row => row.Test !== name),
      passed.map(row => row.Test === name ? {...row,Action:'skip'} : row),
      passed.map(row => row.Test === name ? {...row,Action:'fail'} : row),
    ]),
    [passed[0], ...passed]]) assert.throws(() => checkMonthlyNativeResults(encode(rows)));
});

test('CI runs native scale as a required gate and enumerates race-built tests for race shards', () => {
  const ci = readFileSync(new URL('../../.github/workflows/ci.yml', import.meta.url), 'utf8');
  const normal = ci.split('\n  test:\n')[1]?.split(/^  [\w-]+:/m)[0] ?? '';
  assert.match(normal, /go test -list \. \.\/internal\/monthlydraft > "\$RUNNER_TEMP\/monthly-native-inventory\.txt"/);
  assert.match(normal, /go test -race -list \. \.\/internal\/monthlydraft > "\$RUNNER_TEMP\/monthly-race-inventory\.txt"/);
  assert.match(normal, /node dev\/check-monthly-test-coverage\.mjs/);
  assert.match(normal, /go test -count=1 -json -timeout 3m -run "\$native_pattern" \.\/internal\/monthlydraft \| tee/);
  assert.match(normal, /node dev\/check-monthly-test-coverage\.mjs --results/);
  assert.doesNotMatch(normal, /continue-on-error/);
  const race = ci.split('\n  race-monthly:\n')[1]?.split(/^  [\w-]+:/m)[0] ?? '';
  assert.match(race, /go test -race -list \. \.\/internal\/monthlydraft \| node dev\/monthly-race-shard\.mjs/);
  const file = readFileSync(new URL('../../internal/monthlydraft/partitioned_captured_large_test.go', import.meta.url), 'utf8');
  assert.match(file, /^\/\/go:build !race/);
  assert.match(file, /testPartitionedCapturedGenerationScale\(t, 200\)/);
  const common = readFileSync(new URL('../../internal/monthlydraft/partitioned_captured_scale_test.go', import.meta.url), 'utf8');
  assert.match(common, /testPartitionedCapturedGenerationScale\(t, 12\)/);
  assert.match(common, /for _, u := range e.Users/);
  assert.match(common, /large per-account\/token money changed/);
  assert.doesNotMatch(file + common, /t\.Skip|testing\.Short|os\.Getenv/);
  const retentionNative = readFileSync(new URL('../../internal/monthlydraft/retention_payload_native_test.go', import.meta.url), 'utf8');
  const retentionCommon = readFileSync(new URL('../../internal/monthlydraft/retention_test.go', import.meta.url), 'utf8');
  assert.match(retentionNative, /^\/\/go:build !race/);
  assert.match(retentionNative, /testRetentionReceiverPayload\(t, 512<<10\)/);
  assert.match(retentionCommon, /testRetentionReceiverPayload\(t, 32<<10\)/);
  assert.match(retentionCommon, /for i := range generationRecordLimit/);
  assert.match(retentionCommon, /InspectGenerationRetention\(ctx, ro, start\)/);
  assert.doesNotMatch(retentionNative + retentionCommon, /t\.Skip|testing\.Short|os\.Getenv/);
});
