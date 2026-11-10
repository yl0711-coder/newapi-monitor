import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';

// CI race plan. Each package's race tests are split over `jobs` runners, and
// each runner runs `processes` test processes of one race-built binary at the
// same time (the tests themselves are serial; a 4-vCPU runner otherwise idles
// three cores). Tests are packed longest-first by measured CI duration, so the
// processes finish together. The ACTUAL race-built inventory selects tests:
// a test without a recorded duration gets a conservative estimate and still
// runs exactly once.
//
// Tests listed in a package's `serial` file are sensitive to CPU contention
// (they assert behaviour bounded by real wall-clock budgets). They run in one
// extra job, index `jobs`, as a single process on a runner of their own.
export const racePackages = {
  monitor: {pkg: './monitor', jobs: 10, processes: 3, timings: './monitor-race-timings.json', serial: './monitor-race-serial.json'},
  monthly: {pkg: './internal/monthlydraft', jobs: 7, processes: 3, timings: './monthly-race-timings.json'},
};

// Durations are single-process CI measurements; three processes share a
// runner, so leave headroom below the 25-minute per-process test timeout.
export const raceProcessTargetSeconds = 14 * 60;

export function loadRaceSerial(name) {
  const config = racePackages[name];
  assert.ok(config, `unknown race package ${name}`);
  if (!config.serial) return [];
  const list = JSON.parse(readFileSync(new URL(config.serial, import.meta.url), 'utf8'));
  assert.equal(list?.format_version, 1, 'unsupported serial test list format');
  assert.ok(Array.isArray(list.tests) && list.tests.every(name => /^(?:Test|Example|Fuzz)\S*$/.test(name)), 'invalid serial test list');
  assert.equal(new Set(list.tests).size, list.tests.length, 'duplicate serial test');
  return list.tests;
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
    assert.ok(/^(?:Test|Example|Fuzz)\S*$/.test(name), 'invalid timing test name');
    assert.ok(Number.isInteger(seconds) && seconds > 0 && seconds <= 20 * 60, 'invalid test duration');
  }
}

const escape = value => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// Returns one regexp per parallel process (jobs × processes), plus one for the
// serial job when the package has serial tests; every listed test in exactly one.
export function racePlan(listing, timings, {jobs, processes}, serial = []) {
  assert.ok(Number.isInteger(jobs) && jobs > 0 && Number.isInteger(processes) && processes > 0, 'invalid race plan size');
  const bins = jobs * processes;
  const inventory = listing.split(/\r?\n/).filter(line => /^(?:Test|Example|Fuzz)\S*$/.test(line));
  assert.equal(new Set(inventory).size, inventory.length, 'duplicate test inventory');
  const listed = new Set(inventory);
  assert.equal(new Set(serial).size, serial.length, 'duplicate serial test');
  // A stale serial list must fail loudly instead of silently running nothing.
  for (const name of serial) assert.ok(listed.has(name), `serial test ${name} is not in the inventory; update the serial list`);
  const serialSet = new Set(serial);
  const names = inventory.filter(name => !serialSet.has(name));
  assert.ok(names.length >= bins, 'test inventory is missing or incomplete');
  validateTimings(timings);
  const weight = name => Object.hasOwn(timings.test_seconds, name) ? timings.test_seconds[name] : timings.fallback_seconds;
  const lexical = (a, b) => a < b ? -1 : a > b ? 1 : 0;
  const groups = Array.from({length: bins}, () => []);
  const estimatedSeconds = Array.from({length: bins}, () => 0);
  for (const name of [...names].sort((a, b) => weight(b) - weight(a) || lexical(a, b))) {
    let index = 0;
    for (let i = 1; i < bins; i++) {
      if (estimatedSeconds[i] < estimatedSeconds[index] ||
        (estimatedSeconds[i] === estimatedSeconds[index] && groups[i].length < groups[index].length)) index = i;
    }
    groups[index].push(name);
    estimatedSeconds[index] += weight(name);
  }
  for (const group of groups) assert.ok(group.length, 'empty race process');
  assert.ok(Math.max(...estimatedSeconds) <= raceProcessTargetSeconds,
    'predicted race process exceeds the 14-minute target; refresh timings or add jobs');
  const toPattern = group => `^(${[...group].sort(lexical).map(escape).join('|')})$`;
  const patterns = groups.map(toPattern);
  const serialPattern = serial.length ? toPattern(serial) : null;
  const compiled = [...patterns, ...(serialPattern ? [serialPattern] : [])].map(pattern => new RegExp(pattern));
  for (const name of inventory) assert.equal(compiled.filter(pattern => pattern.test(name)).length, 1, `${name} must belong to exactly one race process`);
  const serialSeconds = serial.reduce((sum, name) => sum + weight(name), 0);
  assert.ok(serialSeconds <= raceProcessTargetSeconds, 'serial race tests exceed the 14-minute target');
  return {count: inventory.length, groups, patterns, estimatedSeconds, serial: [...serial], serialPattern, serialSeconds,
    knownTimingCount: inventory.filter(name => Object.hasOwn(timings.test_seconds, name)).length};
}

// Matrix size for the package: the parallel jobs plus the serial job, if any.
export function raceJobCount(name, serial = loadRaceSerial(name)) {
  return racePackages[name].jobs + (serial.length ? 1 : 0);
}

export function racePlanForJob(name, listing, job, timings = loadRaceTimings(name), serial = loadRaceSerial(name)) {
  const config = racePackages[name];
  assert.ok(config, `unknown race package ${name}`);
  const total = raceJobCount(name, serial);
  assert.ok(Number.isInteger(job) && job >= 0 && job < total, `race job must be an integer from 0 to ${total - 1}`);
  const plan = racePlan(listing, timings, config, serial);
  if (job === config.jobs) return {...plan, jobPatterns: [plan.serialPattern], jobSeconds: [plan.serialSeconds]};
  const from = job * config.processes;
  return {...plan, jobPatterns: plan.patterns.slice(from, from + config.processes),
    jobSeconds: plan.estimatedSeconds.slice(from, from + config.processes)};
}

// CLI: `node dev/race-plan.mjs <monitor|monthly> <job>` prints that job's
// process patterns, one per line; `--verify` instead only checks coverage.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [name, raw] = process.argv.slice(2);
    const listing = readFileSync(0, 'utf8');
    if (raw === '--verify') {
      assert.ok(racePackages[name], `unknown race package ${name}`);
      const plan = racePlan(listing, loadRaceTimings(name), racePackages[name], loadRaceSerial(name));
      console.log(JSON.stringify({package: name, tests: plan.count, processes: plan.patterns.length,
        serialTests: plan.serial.length, jobs: raceJobCount(name), measured: plan.knownTimingCount,
        maxEstimatedSeconds: Math.max(...plan.estimatedSeconds), serialEstimatedSeconds: plan.serialSeconds}));
    } else {
      const job = Number(raw);
      assert.ok(raw !== undefined && String(job) === raw, 'race job index required');
      const plan = racePlanForJob(name, listing, job);
      console.error(`${name} race: ${plan.count} tests (${plan.knownTimingCount} measured); job ${job} process estimates ${plan.jobSeconds.join(',')}s; all processes max ${Math.max(...plan.estimatedSeconds)}s`);
      process.stdout.write(plan.jobPatterns.join('\n') + '\n');
    }
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
