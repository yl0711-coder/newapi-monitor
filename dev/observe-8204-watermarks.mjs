// Read-only observation of real local APIs. No collector settings or jobs change.
import assert from 'node:assert/strict';
import {mkdirSync,writeFileSync} from 'node:fs';
import path from 'node:path';
const args=process.argv.slice(2),arg=(k,d)=>args.includes(k)?args[args.indexOf(k)+1]:d;
const out=path.resolve(arg('--out','.tmp-review-20261023/before-watermarks'));
const samples=Number(arg('--samples','7')),interval=Number(arg('--interval-seconds','60'));
const expectedImage=arg('--image','');
assert.ok(samples>=1&&samples<=10&&interval>=30&&interval<=300);
mkdirSync(out,{recursive:true});
const origin='http://127.0.0.3:8204';
const login=await fetch(origin+'/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:'local',password:'local'}),signal:AbortSignal.timeout(10000)});
assert.equal(login.status,200);
const cookie=login.headers.getSetCookie().map(s=>s.split(';')[0]).join('; ');
const events=[];
for(let i=0;i<samples;i++){
 const started=Date.now();
 // hours follows the stability contract and closes at the last whole hour.
 // The actual alerts page queries a CST day, whose published cutoff can advance.
 for(const url of ['/ready','/model-statistics/report?window=24h','/model-statistics/frt?window=24h','/alerts/rejections?days=1&limit=20']){
  const start=Date.now(),r=await fetch(origin+url,{headers:{Cookie:cookie},cache:'no-store',signal:AbortSignal.timeout(45000)});
  assert.equal(r.status,200,url);const j=await r.json();
  if(url==='/ready'){if(expectedImage)assert.equal(j.image_version,expectedImage);writeFileSync(path.join(out,'ready-'+i+'.json'),JSON.stringify(j,null,2));continue}
  const through=j.actual_through_ts;assert.ok(through===null||Number.isFinite(through),'actual data watermark must be explicit, including unknown/null');
  const e={sample:i,observed_at:new Date().toISOString(),url,elapsed_ms:Date.now()-start,generated_at:j.generated_at,from_ts:j.from_ts??j.from,to_ts:j.to_ts??j.to,actual_through_ts:through,age_seconds:through?Math.floor(Date.now()/1000)-through:null,target:j.target??j.target_ts,timing:j.timing,lag_seconds:j.lag_seconds,collection_failed:j.collection_failed,coverage:j.coverage??j.source,coverage_complete:j.coverage_complete,total:j.total??j.summary,rows:j.rows?.length,has_more:j.has_more,cache_control:r.headers.get('cache-control')};
  events.push(e);console.log(JSON.stringify({sample:i,url,through,age:e.age_seconds,lag:e.lag_seconds,elapsed_ms:e.elapsed_ms}));
 }
 writeFileSync(path.join(out,'watermarks.json'),JSON.stringify({scope:'real unchanged 8204 APIs; API generation time is not data time',events},null,2));
 if(i+1<samples)await new Promise(r=>setTimeout(r,Math.max(0,interval*1000-(Date.now()-started))));
}
