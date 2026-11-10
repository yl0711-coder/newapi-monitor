import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {racePlan, racePlanForJob, racePackages, raceProcessTargetSeconds, loadRaceTimings, loadRaceSerial, raceJobCount} from '../race-plan.mjs';

const small = {jobs: 2, processes: 3};
const empty = {format_version: 1, fallback_seconds: 30, test_seconds: {}};
const listing = names => `${names.join('\n')}\nok example.invalid/pkg 0.1s\n`;

test('every listed test, example and fuzz seed runs in exactly one process, never a prefix match', () => {
  const names = ['TestAlpha', 'TestAlphaExtra', 'Test', 'Test_Regression', 'Test2026', 'Test中文', 'Example', 'ExampleMonitor',
    'FuzzDecode', 'TestFinanceGiftScope', 'TestFinanceGiftScopeNew', ...Array.from({length: 8}, (_, i) => `TestMore${i}`)];
  const plan = racePlan(listing(names.concat('BenchmarkRead')), empty, small);
  assert.equal(plan.count, names.length);
  assert.equal(plan.patterns.length, 6);
  assert.equal(new Set(plan.groups.flat()).size, names.length);
  for (const name of names) assert.equal(plan.patterns.filter(p => new RegExp(p).test(name)).length, 1, name);
  for (const pattern of plan.patterns) {
    assert.equal(new RegExp(pattern).test('BenchmarkRead'), false);
    assert.equal(new RegExp(pattern).test('TestAlpha/sub'), false);
  }
  // Deterministic whatever the listing order.
  assert.deepEqual(racePlan([...names].reverse().join('\n'), empty, small), plan);
});

test('measured work is balanced; new tests use the fallback and deleted history is ignored', () => {
  const heavy = Array.from({length: 12}, (_, i) => `TestHeavy${i}`);
  const light = Array.from({length: 60}, (_, i) => `TestLight${i}`);
  const names = [...heavy, ...light, 'TestNewUnmeasured'];
  const timings = {format_version: 1, fallback_seconds: 30, test_seconds: {
    ...Object.fromEntries(heavy.map(n => [n, 120])), ...Object.fromEntries(light.map(n => [n, 1])), TestDeleted: 600}};
  const before = JSON.stringify(timings);
  const plan = racePlan(names.join('\n'), timings, small);
  assert.equal(plan.knownTimingCount, 72);
  assert.equal(plan.groups.flat().includes('TestDeleted'), false);
  assert.equal(plan.groups.flat().filter(n => n === 'TestNewUnmeasured').length, 1);
  assert.equal(plan.estimatedSeconds.reduce((a, b) => a + b, 0), 12 * 120 + 60 + 30);
  assert.ok(Math.max(...plan.estimatedSeconds) - Math.min(...plan.estimatedSeconds) <= 120);
  assert.equal(JSON.stringify(timings), before);
});

test('empty, failed, duplicate or too small inventories and bad timings fail before running', () => {
  const names = Array.from({length: 6}, (_, i) => `TestInventory${i}`);
  for (const text of ['', 'FAIL example.invalid/pkg [setup failed]', names.slice(1).join('\n'), [...names, names[0]].join('\n')]) {
    assert.throws(() => racePlan(text, empty, small));
  }
  for (const timings of [{...empty, format_version: 2}, {...empty, fallback_seconds: 0}, {...empty, test_seconds: []},
    {...empty, test_seconds: {bad: 5}}, {...empty, test_seconds: {TestInventory0: -1}},
    {...empty, test_seconds: {TestInventory0: raceProcessTargetSeconds + 1}}]) {
    assert.throws(() => racePlan(names.join('\n'), timings, small));
  }
});

test('current measured workloads fit the per-process target with headroom for future tests', () => {
  for (const name of Object.keys(racePackages)) {
    const timings = loadRaceTimings(name);
    const runs = timings.source?.run_ids;
    assert.ok(Array.isArray(runs) && runs.length > 0 && runs.every(Number.isInteger), `${name} timings must name their CI runs`);
    const names = [...Object.keys(timings.test_seconds), ...Array.from({length: 5}, (_, i) => `TestFutureUnmeasured${i}`)];
    const plan = racePlan(names.join('\n'), timings, racePackages[name], loadRaceSerial(name));
    assert.equal(plan.patterns.length, racePackages[name].jobs * racePackages[name].processes);
    assert.ok(Math.max(...plan.estimatedSeconds) <= 12 * 60, `${name}: measured work must leave room below the 14-minute target`);
  }
});

test('job CLI prints exactly that job\'s processes and rejects ambiguous or out-of-range jobs', () => {
  const config = racePackages.monthly;
  const names = Array.from({length: config.jobs * config.processes * 2}, (_, i) => `TestCLI${i}`);
  const script = fileURLToPath(new URL('../race-plan.mjs', import.meta.url));
  const all = racePlan(names.join('\n'), loadRaceTimings('monthly'), config).patterns;
  for (let job = 0; job < config.jobs; job++) {
    const result = spawnSync(process.execPath, [script, 'monthly', String(job)], {input: names.join('\n'), encoding: 'utf8'});
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(result.stdout.trim().split('\n'), all.slice(job * config.processes, (job + 1) * config.processes));
    assert.deepEqual(racePlanForJob('monthly', names.join('\n'), job).jobPatterns, all.slice(job * config.processes, (job + 1) * config.processes));
  }
  for (const job of ['-1', '01', '1.0', 'NaN', String(config.jobs)]) {
    assert.notEqual(spawnSync(process.execPath, [script, 'monthly', job], {input: names.join('\n'), encoding: 'utf8'}).status, 0, job);
  }
  assert.notEqual(spawnSync(process.execPath, [script, 'unknown', '0'], {input: names.join('\n'), encoding: 'utf8'}).status, 0);
  const verify = spawnSync(process.execPath, [script, 'monthly', '--verify'], {input: names.join('\n'), encoding: 'utf8'});
  assert.equal(verify.status, 0, verify.stderr);
  assert.equal(JSON.parse(verify.stdout).tests, names.length);
});

test('CI runs every planned job with race detection and gates publication on all of them', () => {
  const ci = readFileSync(new URL('../../.github/workflows/ci.yml', import.meta.url), 'utf8');
  const job = name => ci.split(`\n  ${name}:\n`)[1]?.split(/^  [\w-]+:/m)[0] ?? '';
  for (const [name, config] of Object.entries(racePackages)) {
    const body = job(`race-${name}`);
    assert.match(body, /fail-fast: false/);
    const matrix = body.match(/job: \[([\d, ]+)\]/)?.[1].split(',').map(v => Number(v.trim()));
    assert.deepEqual(matrix, Array.from({length: raceJobCount(name)}, (_, i) => i), `race-${name} matrix must match the plan`);
    assert.match(body, new RegExp(`dev/run-race-job\\.sh ${name} "\\$\\{\\{ matrix\\.job \\}\\}"`));
    assert.match(body, /node dev\/summarize-race-tests\.mjs/);
    assert.doesNotMatch(body, /continue-on-error|\|\|\s*true|(?:^|\s)-short(?:\s|$)/);
  }
  assert.match(job('test'), /node dev\/race-plan\.mjs monitor --verify/);
  assert.match(job('test'), /node dev\/race-plan\.mjs monthly --verify/);
  assert.ok(ci.includes("grep -Ev '/(monitor$|internal/monthlydraft$)'"));
  for (const name of ['build-push', 'build-nginx-ops-tools']) {
    assert.match(job(name), /needs: \[test, race-monitor, race-monthly, secrets, image-security\]/);
  }
  const runner = readFileSync(new URL('../run-race-job.sh', import.meta.url), 'utf8');
  assert.match(runner, /set -euo pipefail/);
  assert.match(runner, /go test -race -c -o/);
  assert.match(runner, /-test\.count=1 -test\.timeout=25m -test\.run/);
  assert.match(runner, /MONITOR_TEST_NGINX_COLLECTOR_BIN/);
  assert.match(runner, /wait "\$\{pids\[\$index\]\}"/);
  assert.match(runner, /exit "\$status"/);
});

test('timing refresh takes passed measurements, keeps estimates for failed tests and drops removed tests', async () => {
  const {collectDurations, refreshTimings} = await import('../update-race-timings.mjs');
  const pkg = 'github.com/example/monitor';
  const events = [
    {Action: 'pass', Package: pkg, Test: 'TestFast', Elapsed: 0.2},
    {Action: 'pass', Package: pkg, Test: 'TestSlow', Elapsed: 61.1},
    {Action: 'pass', Package: pkg, Test: 'TestSlow/sub', Elapsed: 60},
    {Action: 'fail', Package: pkg, Test: 'TestBroken', Elapsed: 1},
    {Action: 'pass', Package: 'github.com/example/other', Test: 'TestElsewhere', Elapsed: 9},
  ].map(e => `2026-11-10T00:00:00Z ${JSON.stringify(e)}`).join('\n');
  const second = JSON.stringify({Action: 'pass', Package: pkg, Test: 'TestFast', Elapsed: 3.4});
  const measured = collectDurations([events, second], 'monitor');
  assert.deepEqual(measured, {TestFast: 4, TestSlow: 62});
  const previous = {format_version: 1, fallback_seconds: 30, test_seconds: {TestBroken: 40, TestRemoved: 5, TestSlow: 100}};
  const next = refreshTimings(previous, measured, {runIds: [1, 2], listing: 'TestFast\nTestSlow\nTestBroken\nTestNew\n'});
  assert.deepEqual(next.test_seconds, {TestBroken: 40, TestFast: 4, TestSlow: 62});
  assert.deepEqual(next.source.run_ids, [1, 2]);
  assert.throws(() => refreshTimings(previous, {}, {runIds: [1]}));
  assert.throws(() => refreshTimings(previous, measured, {runIds: []}));
});

test('serial tests run alone in the extra job, exactly once, and a stale serial list fails', () => {
  const names = [...Array.from({length: 12}, (_, i) => `TestParallel${i}`), 'TestWallClockBudget'];
  const plan = racePlan(names.join('\n'), empty, small, ['TestWallClockBudget']);
  assert.equal(plan.patterns.length, 6);
  assert.equal(plan.serialPattern, '^(TestWallClockBudget)$');
  for (const pattern of plan.patterns) assert.equal(new RegExp(pattern).test('TestWallClockBudget'), false);
  for (const name of names) assert.equal([...plan.patterns, plan.serialPattern].filter(p => new RegExp(p).test(name)).length, 1, name);
  assert.throws(() => racePlan(names.join('\n'), empty, small, ['TestRenamedAway']), /not in the inventory/);
  assert.throws(() => racePlan(names.join('\n'), empty, small, ['TestWallClockBudget', 'TestWallClockBudget']));
  // The monitor package's serial job is the last matrix index.
  const serial = loadRaceSerial('monitor');
  assert.ok(serial.length > 0);
  const listing = [...serial, ...Array.from({length: 40}, (_, i) => `TestMonitorFixture${i}`)].join('\n');
  const job = racePlanForJob('monitor', listing, racePackages.monitor.jobs, empty, serial);
  assert.deepEqual(job.jobPatterns, [`^(${[...serial].sort().join('|')})$`]);
  assert.throws(() => racePlanForJob('monitor', listing, racePackages.monitor.jobs + 1, empty, serial));
  assert.equal(raceJobCount('monthly'), racePackages.monthly.jobs);
});
