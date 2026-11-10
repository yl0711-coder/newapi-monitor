import assert from 'node:assert/strict';
import {readFileSync, readdirSync, statSync, writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {racePackages} from './race-plan.mjs';

// Refreshes dev/<package>-race-timings.json from the timing artifacts of one
// or more completed CI runs (download them with
//   gh run download <run-id> -p '<package>-race-*' -D <dir>
// ). A test measured in the artifacts gets its new duration (the slowest one
// seen if several runs are given); a test that failed or did not finish keeps
// its previous value, so a broken run cannot shrink the estimates. Tests no
// longer in the race-built inventory are dropped when a listing is given.
const packageNames = {monitor: 'monitor', monthly: 'monthlydraft'};

export function collectDurations(jsonlTexts, goPackageSuffix) {
  const seconds = {};
  for (const text of jsonlTexts) {
    for (const line of text.split(/\r?\n/)) {
      const start = line.indexOf('{');
      if (start < 0) continue;
      let event;
      try { event = JSON.parse(line.slice(start)); } catch { continue; }
      const name = event?.Test;
      if (typeof name !== 'string' || name.includes('/') || event.Action !== 'pass') continue;
      if (typeof event.Package !== 'string' || event.Package.split('/').pop() !== goPackageSuffix) continue;
      const value = Math.max(1, Math.ceil(Number.isFinite(event.Elapsed) ? event.Elapsed : 0));
      seconds[name] = Math.max(seconds[name] ?? 0, value);
    }
  }
  return seconds;
}

export function refreshTimings(previous, measured, {runIds, listing}) {
  assert.equal(previous?.format_version, 1, 'unsupported race timing format');
  assert.ok(Array.isArray(runIds) && runIds.length && runIds.every(Number.isInteger), 'CI run ids required');
  assert.ok(Object.keys(measured).length, 'no passed tests found in the artifacts');
  const merged = {...previous.test_seconds, ...measured};
  if (listing !== undefined) {
    const names = new Set(listing.split(/\r?\n/).filter(line => /^(?:Test|Example|Fuzz)\S*$/.test(line)));
    assert.ok(names.size, 'empty test listing');
    for (const name of Object.keys(merged)) if (!names.has(name)) delete merged[name];
  }
  const sorted = Object.fromEntries(Object.entries(merged).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0));
  return {format_version: 1, fallback_seconds: previous.fallback_seconds,
    source: {run_ids: runIds, note: 'Completed top-level race test times (one test process per CI runner), rounded up to seconds; slowest of the listed runs. No logs or customer data included.'},
    test_seconds: sorted};
}

function jsonlFiles(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) out.push(...jsonlFiles(path));
    else if (entry.endsWith('.jsonl')) out.push(path);
  }
  return out;
}

// usage: node dev/update-race-timings.mjs <monitor|monthly> --run <id> [--run <id>...] [--listing <file>] <artifact-dir>...
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [name, ...rest] = process.argv.slice(2);
    assert.ok(racePackages[name], 'package must be monitor or monthly');
    const runIds = [], dirs = [];
    let listing;
    for (let i = 0; i < rest.length; i++) {
      if (rest[i] === '--run') runIds.push(Number(rest[++i]));
      else if (rest[i] === '--listing') listing = readFileSync(rest[++i], 'utf8');
      else dirs.push(rest[i]);
    }
    assert.ok(dirs.length, 'artifact directory required');
    const texts = dirs.flatMap(jsonlFiles).map(path => readFileSync(path, 'utf8'));
    const measured = collectDurations(texts, packageNames[name]);
    const file = new URL(racePackages[name].timings, new URL('./race-plan.mjs', import.meta.url));
    const previous = JSON.parse(readFileSync(file, 'utf8'));
    const next = refreshTimings(previous, measured, {runIds, listing});
    writeFileSync(file, JSON.stringify(next, null, 1) + '\n');
    console.log(`${name}: ${Object.keys(measured).length} tests measured, ${Object.keys(next.test_seconds).length} recorded`);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
