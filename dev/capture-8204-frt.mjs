// Local 8204 acceptance capture: GET only after local fixture login. Cookies
// stay in memory and no raw production logs are collected by this script.
import assert from 'node:assert/strict';
import {mkdirSync,writeFileSync} from 'node:fs';
import path from 'node:path';
const args=process.argv.slice(2),option=n=>args[args.indexOf(n)+1];
assert.ok(args.includes('--to')&&args.includes('--out'),'--to and --out required');
const to=option('--to'),out=path.resolve(option('--out')),origin='http://127.0.0.3:8204';
mkdirSync(out,{recursive:true});
const login=await fetch(origin+'/login',{method:'POST',headers:{'Content-Type':'application/json'},
  body:JSON.stringify({username:'local',password:'local'}),signal:AbortSignal.timeout(15000)});
assert.equal(login.status,200,'local fixture login failed');
const cookie=login.headers.getSetCookie().map(s=>s.split(';')[0]).join('; ');
const headers=[];
async function capture(name,url){
  const r=await fetch(origin+url,{headers:{Cookie:cookie,Accept:'application/json'},cache:'no-store',signal:AbortSignal.timeout(60000)});
  const text=await r.text();assert.equal(r.status,200,'capture '+name+' HTTP '+r.status);
  const j=JSON.parse(text);writeFileSync(path.join(out,name+'.json'),JSON.stringify(j,null,2));
  headers.push({name,status:r.status,cache_control:r.headers.get('cache-control'),generated_at:j.generated_at});return j;
}
const ready=await capture('ready','/ready');
const pinned=encodeURIComponent(to),results=[];
for(const window of ['24h','3d','7d']){
  const url='/model-statistics/frt?window='+window+'&to='+pinned;
  const a=await capture('frt-'+window,url),b=await capture('frt2-'+window,url);
  assert.deepEqual(a.channels,b.channels,'fixed-window repeated read changed');
  assert.equal(a.to,to);results.push({window,channels:a.channels.length,computable:a.channels.filter(c=>c.frt.mean_ms!==null).length,coverage:a.coverage.status,actual_through_ts:a.actual_through_ts});
}
const status=await capture('frt-status','/admin/frt-sum/recompute?to='+pinned);
await capture('model-statistics','/model-statistics/report?window=24h');
await capture('alerts','/alerts/rejections');
writeFileSync(path.join(out,'capture-meta.json'),JSON.stringify({generated_at:new Date().toISOString(),to,version:ready.image_version,head:ready.git_sha,results,headers},null,2));
console.log(JSON.stringify({version:ready.image_version,to,results,status:status.job?.status,windows:status.windows},null,2));
