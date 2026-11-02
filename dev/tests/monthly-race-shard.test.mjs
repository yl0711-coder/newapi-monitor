import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {monthlyRaceShards,monthlyRaceShardCount,monthlyRaceShardTargetSeconds} from '../monthly-race-shard.mjs';

test('monthly race inventory is covered exactly once, including examples, fuzz seeds and new tests', () => {
  const names = ['TestOne', 'TestTwo', 'TestThree', 'TestNewFeature', 'ExampleBill', 'FuzzAmounts',
    ...Array.from({length:monthlyRaceShardCount},(_,i)=>`TestAdditional${i}`)];
  const listing = `${names.join('\n')}\nok example.invalid/monthlydraft 0.1s\n`;
  const result = monthlyRaceShards(listing);
  assert.equal(result.count, names.length);
  assert.equal(new Set(result.groups.flat()).size, names.length);
  for (const name of names) assert.equal(result.patterns.filter(p => new RegExp(p).test(name)).length, 1);
  for (const pattern of result.patterns) assert.equal(new RegExp(pattern).test('TestOneExtra'), false);
  assert.deepEqual(monthlyRaceShards([...names].reverse().join('\n')), result);
});

test('monthly race shard rejects empty, failed and duplicate inventories', () => {
  const names=Array.from({length:monthlyRaceShardCount},(_,i)=>`TestInventory${i}`);
  for (const listing of ['', 'FAIL example.invalid/monthlydraft [setup failed]', names.slice(1).join('\n'),
    [...names,names[0]].join('\n')]) assert.throws(() => monthlyRaceShards(listing));
});

test('measured workload is balanced deterministically without selecting tests from historical timings', () => {
  const heavy=Array.from({length:24},(_,i)=>`TestHeavy${i}`);
  const light=Array.from({length:72},(_,i)=>`TestLight${i}`);
  const names=[...heavy,...light,'TestNewUnmeasured'];
  const timings={format_version:1,fallback_seconds:90,test_seconds:{
    ...Object.fromEntries(heavy.map(n=>[n,120])),...Object.fromEntries(light.map(n=>[n,1])),TestDeletedHistorical:900}};
  const before=JSON.stringify(timings);
  const result=monthlyRaceShards(names.join('\n'),timings);
  assert.equal(result.count,names.length);
  assert.equal(result.knownTimingCount,96);
  assert.equal(result.groups.flat().includes('TestDeletedHistorical'),false);
  assert.equal(result.groups.flat().filter(n=>n==='TestNewUnmeasured').length,1);
  assert.ok(Math.max(...result.estimatedSeconds)-Math.min(...result.estimatedSeconds)<=120);
  assert.equal(result.estimatedSeconds.reduce((a,b)=>a+b,0),24*120+72+90);
  assert.deepEqual(monthlyRaceShards([...names].reverse().join('\n'),timings),result);
  assert.equal(JSON.stringify(timings),before);
});

test('invalid timing metadata and insufficient timeout headroom fail before execution',()=>{
  const names=Array.from({length:monthlyRaceShardCount},(_,i)=>`TestTiming${i}`).join('\n');
  const valid={format_version:1,fallback_seconds:90,test_seconds:{}};
  for(const config of [
    {...valid,format_version:2},{...valid,fallback_seconds:0},{...valid,fallback_seconds:Infinity},
    {...valid,test_seconds:[]},{...valid,test_seconds:{bad:5}},
    {...valid,test_seconds:{TestBad:-1}},{...valid,test_seconds:{TestBad:NaN}},
    {...valid,test_seconds:{TestTiming0:monthlyRaceShardTargetSeconds+1}},
  ])assert.throws(()=>monthlyRaceShards(names,config));
});

test('recorded CI durations leave headroom without increasing shard count or business timeouts',()=>{
  const timings=JSON.parse(readFileSync(new URL('../monthly-race-timings.json',import.meta.url),'utf8'));
  assert.equal(timings.source.run_id,41373090040);
  const names=[...Object.keys(timings.test_seconds),...Array.from({length:3},(_,i)=>`TestFutureUnmeasured${i}`)];
  const result=monthlyRaceShards(names.join('\n'));
  assert.equal(result.groups.length,12);
  assert.ok(Math.max(...result.estimatedSeconds)<=monthlyRaceShardTargetSeconds);
  assert.ok(Math.max(...result.estimatedSeconds)-Math.min(...result.estimatedSeconds)<=2);
});

test('monthly shard CLI supports double-digit indices and rejects ambiguous or out-of-range indices',()=>{
  const input=Array.from({length:monthlyRaceShardCount*2},(_,i)=>`TestCLI${i}`).join('\n');
  const script=fileURLToPath(new URL('../monthly-race-shard.mjs',import.meta.url));
  for(const index of [0,9,10,monthlyRaceShardCount-1]){
    const result=spawnSync(process.execPath,[script,String(index)],{input,encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);
    assert.equal(result.stdout.trim(),monthlyRaceShards(input).patterns[index]);
  }
  for(const index of ['-1','01','1.0','NaN',String(monthlyRaceShardCount)]){
    const result=spawnSync(process.execPath,[script,index],{input,encoding:'utf8'});
    assert.notEqual(result.status,0);
  }
});

test('release gates wait for all monthly race shards without weakening the time limit', () => {
  const ci = readFileSync(new URL('../../.github/workflows/ci.yml', import.meta.url), 'utf8');
  const job = ci.split('\n  race-monthly:\n')[1]?.split(/^  [\w-]+:/m)[0] ?? '';
  assert.match(job, /fail-fast: false/);
  const matrix=job.match(/shard: \[([\d, ]+)\]/)?.[1].split(',').map(v=>Number(v.trim()));
  assert.deepEqual(matrix,Array.from({length:monthlyRaceShardCount},(_,i)=>i));
  assert.match(job, /set -euo pipefail/);
  assert.match(job, /go test -race -list \. \.\/internal\/monthlydraft/);
  assert.match(job, /node dev\/monthly-race-shard\.mjs "\$MONTHLY_SHARD"/);
  assert.match(job, /go test -race -count=1 -json -timeout 20m -run "\$pattern" \.\/internal\/monthlydraft \| tee/);
  assert.match(job, /node dev\/summarize-race-tests\.mjs/);
  assert.match(job, /name: monthly-race-\$\{\{ matrix.shard \}\}/);
  assert.doesNotMatch(job, /continue-on-error|\|\|\s*true|(?:^|\s)-short(?:\s|$)/);
  assert.ok(ci.includes("grep -Ev '/(monitor$|internal/monthlydraft$)'"));
  for (const name of ['image-security', 'build-push', 'build-nginx-ops-tools']) {
    const body = ci.split(`\n  ${name}:\n`)[1]?.split(/^  [\w-]+:/m)[0] ?? '';
    assert.match(body, /needs: \[[^\n]*race-monthly[^\n]*\]/);
  }
});
