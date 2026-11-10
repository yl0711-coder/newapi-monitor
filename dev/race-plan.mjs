import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';

// CI race plan (see docs/CI-规范.md).
//
// Parallel group: most tests are split over `jobs` runners × `processes` test
// processes of one race-built binary running at the same time (the tests are
// serial; one process uses one of a runner's four cores).
//
// Serial group: tests that assert behaviour bounded by real wall-clock budgets
// fail when other processes compete for the CPU. They are named in the
// package's serial file (by prefix or by name) and split over `serial.jobs`
// further runners, ONE process each (job indices `jobs` .. `jobs+serial.jobs-1`).
//
// Both groups are packed longest-first by measured CI duration. The ACTUAL
// race-built inventory selects tests; a test without a recorded duration gets a
// conservative estimate and still runs exactly once.
export const racePackages = {
  monitor: {pkg: './monitor', jobs: 7, processes: 3, timings: './monitor-race-timings.json', serial: './monitor-race-serial.json'},
  monthly: {pkg: './internal/monthlydraft', jobs: 6, processes: 3, timings: './monthly-race-timings.json', serial: './monthly-race-serial.json'},
};

// Durations are single-process CI measurements; leave headroom below the
// 25-minute per-process test timeout (parallel processes share a runner).
export const raceProcessTargetSeconds = 14 * 60;

const testName = /^(?:Test|Example|Fuzz)\S*$/;
const lexical = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const escape = value => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const toPattern = group => `^(${[...group].sort(lexical).map(escape).join('|')})$`;

const noSerial = {jobs: 0, prefixes: [], tests: []};

export function validateSerial(serial) {
  assert.equal(serial?.format_version, 2, 'unsupported serial test list format');
  assert.ok(Number.isInteger(serial.jobs) && serial.jobs >= 0, 'invalid serial job count');
  for (const field of ['prefixes', 'tests']) {
    assert.ok(Array.isArray(serial[field]) && serial[field].every(name => testName.test(name)), `invalid serial ${field}`);
    assert.equal(new Set(serial[field]).size, serial[field].length, `duplicate serial ${field}`);
  }
  assert.ok(serial.jobs > 0 || (!serial.prefixes.length && !serial.tests.length), 'serial tests need at least one serial job');
  return serial;
}

export function loadRaceSerial(name) {
  const config = racePackages[name];
  assert.ok(config, `unknown race package ${name}`);
  if (!config.serial) return noSerial;
  return validateSerial(JSON.parse(readFileSync(new URL(config.serial, import.meta.url), 'utf8')));
}

export function loadRaceTimings(name) {
  const config = racePackages[name];
  assert.ok(config, `unknown race package ${name}`);
  return JSON.parse(readFileSync(new URL(config.timings, import.meta.url), 'utf8'));
}

function validateTimings(timings) {
  assert.equal(timings?.format_version, 1, 'unsupported race timing format');
  assert.ok(Number.isInteger(timings.fallback_seconds) && timings.fallback_seconds > 0 && timings.fallback_seconds <= raceProcessTargetSeconds,
    'invalid conservative default for new tests');
  assert.ok(timings.test_seconds && typeof timings.test_seconds === 'object' && !Array.isArray(timings.test_seconds), 'missing test timings');
  for (const [name, seconds] of Object.entries(timings.test_seconds)) {
    assert.ok(testName.test(name), 'invalid timing test name');
    assert.ok(Number.isInteger(seconds) && seconds > 0 && seconds <= 20 * 60, 'invalid test duration');
  }
}

// Longest-first packing into `bins` groups of balanced measured work.
function pack(names, bins, weight) {
  const groups = Array.from({length: bins}, () => []);
  const seconds = Array.from({length: bins}, () => 0);
  for (const name of [...names].sort((a, b) => weight(b) - weight(a) || lexical(a, b))) {
    let index = 0;
    for (let i = 1; i < bins; i++) {
      if (seconds[i] < seconds[index] || (seconds[i] === seconds[index] && groups[i].length < groups[index].length)) index = i;
    }
    groups[index].push(name);
    seconds[index] += weight(name);
  }
  return {groups, seconds};
}

// Returns one regexp per process: `jobs × processes` parallel processes, then
// `serial.jobs` single-process serial jobs. Every listed test is in exactly one.
export function racePlan(listing, timings, {jobs, processes}, serial = noSerial) {
  assert.ok(Number.isInteger(jobs) && jobs > 0 && Number.isInteger(processes) && processes > 0, 'invalid race plan size');
  validateSerial({format_version: 2, ...serial});
  validateTimings(timings);
  const inventory = listing.split(/\r?\n/).filter(line => testName.test(line));
  assert.equal(new Set(inventory).size, inventory.length, 'duplicate test inventory');
  const listed = new Set(inventory);
  // A stale serial list must fail loudly instead of silently running nothing.
  for (const name of serial.tests) assert.ok(listed.has(name), `serial test ${name} is not in the inventory; update the serial list`);
  for (const prefix of serial.prefixes) assert.ok(inventory.some(name => name.startsWith(prefix)), `serial prefix ${prefix} matches no test; update the serial list`);
  const isSerial = name => serial.tests.includes(name) || serial.prefixes.some(prefix => name.startsWith(prefix));
  const serialNames = inventory.filter(isSerial);
  const parallelNames = inventory.filter(name => !isSerial(name));
  const bins = jobs * processes;
  assert.ok(parallelNames.length >= bins, 'test inventory is missing or incomplete');
  assert.ok(!serial.jobs || serialNames.length >= serial.jobs, 'fewer serial tests than serial jobs');
  const weight = name => Object.hasOwn(timings.test_seconds, name) ? timings.test_seconds[name] : timings.fallback_seconds;
  const parallel = pack(parallelNames, bins, weight);
  const serialPack = serial.jobs ? pack(serialNames, serial.jobs, weight) : {groups: [], seconds: []};
  for (const group of [...parallel.groups, ...serialPack.groups]) assert.ok(group.length, 'empty race process');
  assert.ok(Math.max(...parallel.seconds, ...serialPack.seconds) <= raceProcessTargetSeconds,
    'predicted race process exceeds the 14-minute target; refresh timings or add jobs');
  const patterns = parallel.groups.map(toPattern);
  const serialPatterns = serialPack.groups.map(toPattern);
  const compiled = [...patterns, ...serialPatterns].map(pattern => new RegExp(pattern));
  for (const name of inventory) assert.equal(compiled.filter(pattern => pattern.test(name)).length, 1, `${name} must belong to exactly one race process`);
  return {count: inventory.length, groups: parallel.groups, patterns, estimatedSeconds: parallel.seconds,
    serialGroups: serialPack.groups, serialPatterns, serialSeconds: serialPack.seconds, serialCount: serialNames.length,
    knownTimingCount: inventory.filter(name => Object.hasOwn(timings.test_seconds, name)).length};
}

// Matrix size for the package: parallel jobs plus serial jobs.
export function raceJobCount(name, serial = loadRaceSerial(name)) {
  return racePackages[name].jobs + serial.jobs;
}

export function racePlanForJob(name, listing, job, timings = loadRaceTimings(name), serial = loadRaceSerial(name)) {
  const config = racePackages[name];
  assert.ok(config, `unknown race package ${name}`);
  const total = raceJobCount(name, serial);
  assert.ok(Number.isInteger(job) && job >= 0 && job < total, `race job must be an integer from 0 to ${total - 1}`);
  const plan = racePlan(listing, timings, config, serial);
  if (job >= config.jobs) {
    const index = job - config.jobs;
    return {...plan, serialJob: true, jobPatterns: [plan.serialPatterns[index]], jobSeconds: [plan.serialSeconds[index]]};
  }
  const from = job * config.processes;
  return {...plan, serialJob: false, jobPatterns: plan.patterns.slice(from, from + config.processes),
    jobSeconds: plan.estimatedSeconds.slice(from, from + config.processes)};
}

// CLI: `node dev/race-plan.mjs <monitor|monthly> <job>` prints that job's
// process patterns, one per line; `--verify` instead only checks coverage.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [name, raw] = process.argv.slice(2);
    assert.ok(racePackages[name], `unknown race package ${name}`);
    const listing = readFileSync(0, 'utf8');
    if (raw === '--verify') {
      const plan = racePlan(listing, loadRaceTimings(name), racePackages[name], loadRaceSerial(name));
      console.log(JSON.stringify({package: name, tests: plan.count, jobs: raceJobCount(name),
        parallelProcesses: plan.patterns.length, serialJobs: plan.serialPatterns.length, serialTests: plan.serialCount,
        measured: plan.knownTimingCount, maxParallelSeconds: Math.max(...plan.estimatedSeconds),
        maxSerialSeconds: plan.serialSeconds.length ? Math.max(...plan.serialSeconds) : 0}));
    } else {
      const job = Number(raw);
      assert.ok(raw !== undefined && String(job) === raw, 'race job index required');
      const plan = racePlanForJob(name, listing, job);
      console.error(`${name} race: ${plan.count} tests (${plan.knownTimingCount} measured); job ${job} ${plan.serialJob ? 'serial (one process)' : 'parallel'} estimates ${plan.jobSeconds.join(',')}s`);
      process.stdout.write(plan.jobPatterns.join('\n') + '\n');
    }
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
