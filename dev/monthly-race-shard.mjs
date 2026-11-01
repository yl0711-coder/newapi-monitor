import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';

export const monthlyRaceShardCount = 4;

// The actual go test inventory is authoritative. Stable round-robin assignment
// includes new tests automatically and keeps every top-level test in one shard.
export function monthlyRaceShards(listing) {
  const names = listing.split(/\r?\n/).filter(line => /^(?:Test|Example|Fuzz)\S*$/.test(line));
  assert.ok(names.length >= monthlyRaceShardCount, 'monthly test inventory is missing or incomplete');
  assert.equal(new Set(names).size, names.length, 'duplicate monthly test inventory');
  const groups = Array.from({length: monthlyRaceShardCount}, () => []);
  for (const [index, name] of names.sort().entries()) groups[index % monthlyRaceShardCount].push(name);
  for (const group of groups) assert.ok(group.length, 'empty monthly race shard');
  const escape = value => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const patterns = groups.map(group => `^(${group.map(escape).join('|')})$`);
  for (const name of names) assert.equal(patterns.filter(pattern => new RegExp(pattern).test(name)).length, 1);
  return {count: names.length, groups, patterns};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const raw = process.argv[2];
  assert.match(raw ?? '', /^[0-3]$/, 'monthly race shard must be 0, 1, 2 or 3');
  const result = monthlyRaceShards(readFileSync(0, 'utf8'));
  console.error(`Monthly race coverage: ${result.count} tests, shard sizes ${result.groups.map(group => group.length).join(',')}`);
  console.log(result.patterns[Number(raw)]);
}
