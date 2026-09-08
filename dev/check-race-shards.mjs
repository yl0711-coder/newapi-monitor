import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

export function verifyRaceShards(workflow, listing) {
  // Read the actual CI matrix, not a second copy of its routing rules.
  const afterJobHeader = workflow.split(/^  race-monitor:[ \t]*$/m)[1];
  const job = afterJobHeader?.split(/^  [\w-]+:/m)[0];
  const patterns = [...(job ?? '').matchAll(/^\s+pattern: '([^']+)'\s*$/gm)].map(match => new RegExp(match[1]));
  if (!patterns.length) throw new Error('No monitor race shard patterns found');
  const names = listing.split(/\r?\n/).filter(name => /^(Test|Example|Fuzz)\S*$/.test(name));
  if (!names.length || new Set(names).size !== names.length) throw new Error('Empty or duplicate monitor test inventory');
  const counts = patterns.map(() => 0);
  for (const name of names) {
    const matching = patterns.flatMap((pattern, index) => pattern.test(name) ? [index] : []);
    if (matching.length !== 1) throw new Error(`${name} belongs to ${matching.length} shards; expected exactly one`);
    counts[matching[0]]++;
  }
  if (counts.includes(0)) throw new Error('Empty monitor race shard');
  return { tests: names.length, counts };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const result = verifyRaceShards(readFileSync(new URL('../.github/workflows/ci.yml', import.meta.url), 'utf8'), readFileSync(0, 'utf8'));
    console.log(JSON.stringify(result));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
