// Read-only Token runtime observations. Default: seven samples, five minutes
// apart. This captures checkpoints/queues; it does not prove row reconciliation.
// node dev/observe-token-pipeline.mjs --image v163 --out <evidence-directory>
import assert from 'node:assert/strict';
import {mkdirSync,writeFileSync} from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';

const args=process.argv.slice(2),option=(key,fallback)=>args.includes(key)?args[args.indexOf(key)+1]:fallback;
const expectedImage=option('--image',''),out=option('--out','');
assert.ok(expectedImage&&out,'--image and --out are required');
const samples=Number(option('--samples','7')),interval=Number(option('--interval-seconds','300'));
const sqliteContainer=option('--sqlite-container','');
assert.ok(!sqliteContainer||/^[A-Za-z0-9_-]+$/.test(sqliteContainer));
assert.ok(Number.isInteger(samples)&&samples>=1&&samples<=10);
assert.ok(Number.isInteger(interval)&&interval>=30&&interval<=300);
const directory=path.resolve(out),origin='http://127.0.0.3:8204';
mkdirSync(directory,{recursive:true});
const waitSeconds=Number(option('--wait-seconds','0'));
assert.ok(Number.isInteger(waitSeconds)&&waitSeconds>=0&&waitSeconds<=3600);
if(waitSeconds){
 const deadline=Date.now()+waitSeconds*1000;let listening=false;
 while(Date.now()<deadline){
  try{listening=(await fetch(origin+'/live',{signal:AbortSignal.timeout(3000)})).status===200}catch{}
  if(listening)break;
  await new Promise(resolve=>setTimeout(resolve,5000));
 }
 assert.ok(listening,'startup wait budget exhausted; runtime observation has not begun');
}
const login=await fetch(origin+'/login',{method:'POST',headers:{'Content-Type':'application/json'},
 body:JSON.stringify({username:process.env.MONITOR_LOCAL_USER||'local',password:process.env.MONITOR_LOCAL_PASSWORD||'local'}),signal:AbortSignal.timeout(10000)});
assert.equal(login.status,200,'local login failed');
const cookie=login.headers.getSetCookie().map(v=>v.split(';')[0]).join('; ');
const observations=[],started=Date.now();
const endpoints=['/ready','/anomalies/token/health','/anomalies/token/detection/health'];
for(let index=0;index<samples;index++){
 const wait=started+index*interval*1000-Date.now();
 if(wait>0)await new Promise(resolve=>setTimeout(resolve,wait));
 const sample={index,observed_at:new Date().toISOString(),elapsed_ms:Date.now()-started,endpoints:[]};
 for(const endpoint of endpoints){
  const at=Date.now();
  try{
   const response=await fetch(origin+endpoint,{headers:{Cookie:cookie},cache:'no-store',signal:AbortSignal.timeout(15000)});
   const body=await response.json();
   sample.endpoints.push({endpoint,http_status:response.status,elapsed_ms:Date.now()-at,body});
  }catch(error){sample.endpoints.push({endpoint,elapsed_ms:Date.now()-at,error:error?.name||'read_failed'})}
 }
 const [runtime,facts,detection]=sample.endpoints.map(v=>v.body);
 if(sqliteContainer){
  const epoch=facts?.source_epoch;
  if(typeof epoch==='string'&&/^[A-Za-z0-9._:-]+$/.test(epoch)){
   const queries={
    projections:"SELECT 'pending' AS projection,cursor_epoch,cursor_id,ready FROM token_report_pending_migration_v1 WHERE id=1 UNION ALL SELECT 'state',cursor_epoch,cursor_id,ready FROM token_report_state_migration_v1 WHERE id=1",
    recent_windows:"SELECT start_at,end_at,lane,cursor_at,cursor_id,done,source_complete,read_page_rows,read_success_pages,read_worst_ms,read_wait_ms,read_query_ms,read_execute_ms,read_receive_ms,next_at,last_reason FROM token_metering_windows WHERE source_epoch='"+epoch+"' AND start_at>="+(Math.floor(Date.now()/1000)-1800)+" ORDER BY start_at DESC LIMIT 8"
   };
   sample.local_checkpoints={};
   for(const [name,query] of Object.entries(queries)){
    const at=Date.now();
    try{
     const raw=execFileSync('docker',['exec','-i','--user','1000:1000',sqliteContainer,'timeout','3','sqlite3','-readonly','-json','/token-data/token.db'],{input:'.timeout 1000\nPRAGMA query_only=ON;\n'+query+';\n',encoding:'utf8',windowsHide:true,timeout:5000,maxBuffer:1024**2,stdio:['pipe','pipe','pipe']});
     sample.local_checkpoints[name]={rows:JSON.parse(raw.trim()||'[]'),elapsed_ms:Date.now()-at};
    }catch(error){sample.local_checkpoints[name]={error:'bounded_local_checkpoint_read_failed',exit_code:error.status??null,elapsed_ms:Date.now()-at}}
   }
  }
 }
 sample.identity_matches=runtime?.image_version===expectedImage;
 sample.facts_available=facts?.available===true;
 sample.source_available=facts?.source_available===true;
 sample.detection_available=detection?.available===true;
 observations.push(sample);
 const record={read_only:true,source_queries:false,expected_image:expectedImage,requested_samples:samples,interval_seconds:interval,
  observation_complete:observations.length===samples,observed_span_seconds:sample.elapsed_ms/1000,
  thirty_minutes_observed:observations.length===samples&&sample.elapsed_ms>=1800000,
  reconciliation_performed:false,observations};
 writeFileSync(path.join(directory,'token-watermarks.json'),JSON.stringify(record,null,2)+'\n');
 console.log(JSON.stringify({sample:index,image:runtime?.image_version,identity_matches:sample.identity_matches,
  facts_available:sample.facts_available,source_available:sample.source_available,scan_through:facts?.state?.scan_through,
  complete_through:facts?.state?.complete_through,scan_lag_seconds:facts?.scan_lag_seconds,facts:facts?.facts,
  pending_links:facts?.pending_links,pending_windows:facts?.pending_windows,failed_windows:facts?.failed_windows,
  detection_available:sample.detection_available,detection_last_success_at:detection?.state?.last_success_at,
  current_results:detection?.current_results,pending_sample_count:detection?.pending_sample_count,
  detection_states:detection?.detection_states,retry_reasons:detection?.retry_reasons}));
 assert.ok(sample.identity_matches,'runtime identity missing or changed; saved observations are incomplete');
}
