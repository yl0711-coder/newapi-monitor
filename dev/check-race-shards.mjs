import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

export function readRaceShards(workflow) {
  // Read the actual CI matrix, not a second copy of its routing rules.
  const afterJobHeader = workflow.split(/^  race-monitor:[ \t]*$/m)[1];
  const job = afterJobHeader?.split(/^  [\w-]+:/m)[0];
  const blocks = [...(job ?? '').matchAll(/^          - shard: ([\w-]+)\n((?:^            [^\n]*\n)*)/gm)];
  if (!blocks.length) throw new Error('No monitor race shard patterns found');
  const shards = blocks.map(([, name, body]) => {
    const pattern = body.match(/^            pattern: '([^']+)'\s*$/m)?.[1];
    const skip = body.match(/^            skip: '([^']+)'\s*$/m)?.[1];
    if (!pattern) throw new Error(`Missing pattern for shard ${name}`);
    // Rules select whole top-level tests. A slash would instead select subtests in Go.
    if (pattern.includes('/') || skip?.includes('/')) throw new Error('Subtest shard patterns are not allowed');
    return { name, pattern: new RegExp(pattern), skip: skip ? new RegExp(skip) : null };
  });
  if (new Set(shards.map(shard => shard.name)).size !== shards.length) throw new Error('Duplicate shard name');
  return shards;
}

export function verifyRaceShards(workflow, listing) {
  const shards = readRaceShards(workflow);
  const names = listing.split(/\r?\n/).filter(name => /^(Test|Example|Fuzz)\S*$/.test(name));
  if (!names.length || new Set(names).size !== names.length) throw new Error('Empty or duplicate monitor test inventory');
  const counts = shards.map(() => 0);
  for (const name of names) {
    const matching = shards.flatMap((shard, index) => shard.pattern.test(name) && !shard.skip?.test(name) ? [index] : []);
    if (matching.length !== 1) throw new Error(`${name} belongs to ${matching.length} shards; expected exactly one`);
    counts[matching[0]]++;
  }
  if (counts.includes(0)) throw new Error('Empty monitor race shard');
  return { tests: names.length, counts };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const workflow = readFileSync(new URL('../.github/workflows/ci.yml', import.meta.url), 'utf8');
    const result = verifyRaceShards(workflow, readFileSync(0, 'utf8'));
    if (process.argv[2]) {
      const index = readRaceShards(workflow).findIndex(shard => shard.name === process.argv[2]);
      if (index < 0) throw new Error('Unknown monitor race shard');
      result.selected = { shard: process.argv[2], tests: result.counts[index] };
    }
    console.log(JSON.stringify(result));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
