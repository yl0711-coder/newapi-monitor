import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';

export const monthlyNativeOnlyTests = Object.freeze([
  'TestPartitionedCapturedGenerationLargeCompleteMonth',
  'TestRetentionFullReceiverPayloadPerformance',
]);
export const monthlyBoundedRaceTests = Object.freeze([
  'TestPartitionedCapturedGenerationBoundedRaceMonth',
  'TestRetentionBoundedReceiverPayloadRace',
]);

function inventory(listing) {
  const names = listing.split(/\r?\n/).filter(line => /^(?:Test|Example|Fuzz)\S*$/.test(line));
  assert.ok(names.length, 'monthly test inventory is missing');
  assert.equal(new Set(names).size, names.length, 'duplicate monthly test inventory');
  return names.sort();
}

// Every test must be accounted for. A native-only test is a mandatory separate
// gate, never a general allow-list for skipped or unregistered race coverage.
export function checkMonthlyTestCoverage(allListing, raceListing) {
  const all = inventory(allListing);
  const race = inventory(raceListing);
  for (const name of monthlyNativeOnlyTests) {
    assert.ok(all.includes(name), `required native scale test is missing: ${name}`);
    assert.ok(!race.includes(name), `native scale test unexpectedly belongs to race inventory: ${name}`);
  }
  for (const name of monthlyBoundedRaceTests) {
    assert.ok(race.includes(name), `shared bounded race test is missing: ${name}`);
  }
  const expectedRace = all.filter(name => !monthlyNativeOnlyTests.includes(name));
  assert.deepEqual(race, expectedRace, 'unaccounted native-only, race-only or missing monthly test');
  return {
    allCount: all.length,
    raceCount: race.length,
    nativeOnly: [...monthlyNativeOnlyTests],
    nativePattern: `^(${monthlyNativeOnlyTests.join('|')})$`,
  };
}

export function checkMonthlyNativeResults(jsonl) {
  const completed = new Map();
  let packagePassed = false;
  for (const line of jsonl.split(/\r?\n/).filter(Boolean)) {
    const row = JSON.parse(line);
    if (!row.Test) {
      assert.notEqual(row.Action, 'fail', 'native scale package failed');
      if (row.Action === 'pass') packagePassed = true;
    } else if (monthlyNativeOnlyTests.includes(row.Test) && ['pass', 'fail', 'skip'].includes(row.Action)) {
      assert.equal(row.Action, 'pass', `mandatory native scale test did not pass: ${row.Test}`);
      assert.ok(!completed.has(row.Test), `duplicate native scale completion: ${row.Test}`);
      completed.set(row.Test, true);
    }
  }
  assert.ok(packagePassed, 'native scale package completion is missing');
  for (const name of monthlyNativeOnlyTests) assert.ok(completed.has(name), `mandatory native scale result missing: ${name}`);
  return {passed: completed.size};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  assert.equal(process.argv.length, 4, 'provide complete native and race go test inventories');
  if (process.argv[2] === '--results') {
    const result = checkMonthlyNativeResults(readFileSync(process.argv[3], 'utf8'));
    console.log(`Mandatory native scale: ${result.passed} passed; production deadlines retained`);
  } else {
    const result = checkMonthlyTestCoverage(readFileSync(process.argv[2], 'utf8'), readFileSync(process.argv[3], 'utf8'));
    console.error(`Monthly coverage: ${result.allCount} total = ${result.raceCount} race + ${result.nativeOnly.length} mandatory native scale`);
    console.log(result.nativePattern);
  }
}
