import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import test from 'node:test';

function fixture(t,mutate){
  const dir=mkdtempSync(path.join(process.env.MONITOR_TEST_TMPDIR||os.tmpdir(),'frt-reread-'));
  t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const valid={mean_ms:1000,observed:1001,p95_ms:2000,p99_ms:3000,max_ms:4000,mean_unavailable_reason:''};
  const zero={mean_ms:null,observed:0,mean_unavailable_reason:'no_valid_samples'};
  for(const [label,hours] of [['24h',24],['3d',72],['7d',168]]){
    const to=1792721400,from=to-hours*3600;
    const r={from:new Date(from*1000).toISOString(),to:new Date(to*1000).toISOString(),coverage:{status:'complete'},channels:[
      {channel_id:1,frt:valid,groups:[{group:'g',frt:valid,models:[{model:'m',frt:valid}]}]},
      {channel_id:2,frt:zero,groups:[{group:'empty',frt:zero,models:[{model:'zero',frt:zero}]}]}
    ]};
    const second=structuredClone(r);if(label==='24h')mutate?.(second);
    const source={source:'production.logs',from_ts:from,to_ts:to,rows:[
      {channel_id:1,grp:'g',model_name:'m',sum_ms:1001000,observed:1001},
      {channel_id:2,grp:'empty',model_name:'zero',sum_ms:0,observed:0}
    ]};
    for(const [name,data] of [[`frt-${label}`,r],[`frt2-${label}`,second],[`source-${label}`,source]])writeFileSync(path.join(dir,name+'.json'),JSON.stringify(data));
  }
  return spawnSync(process.execPath,[path.resolve(import.meta.dirname,'../verify-8204-frt-reconcile.mjs'),'--dir',dir],{encoding:'utf8',windowsHide:true});
}
test('complete FRT reread accepts identical three-level values and zero-sample reasons',t=>{
  const r=fixture(t);assert.equal(r.status,0,r.stderr);
});
for(const [name,mutate] of [
  ['changed model mean',r=>r.channels[0].groups[0].models[0].frt.mean_ms+=1000],
  ['deleted model',r=>r.channels[0].groups[0].models.pop()],
  ['deleted group',r=>r.channels[1].groups.pop()],
  ['changed model samples',r=>r.channels[0].groups[0].models[0].frt.observed++],
  ['changed unavailable reason',r=>r.channels[1].groups[0].models[0].frt.mean_unavailable_reason='historical_sum_unavailable'],
])test('complete FRT reread rejects '+name,t=>{
  const r=fixture(t,mutate);assert.notEqual(r.status,0);assert.match(r.stderr,/完整三级结构/);
});
