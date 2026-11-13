// Authorized restart verification. Reads exact captured identities; never
// scans production or rewrites a cursor. Requires a completed observation.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync,execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {createGunzip} from 'node:zlib';
import {createInterface} from 'node:readline';

const args=process.argv.slice(2),opt=(k,d)=>args.includes(k)?args[args.indexOf(k)+1]:d;
// Open-window recovery fields are a schema capability, independent of whether
// the prerequisite observation measured latency or thirty minutes of uptime.
const openWindowCursors=args.includes('--latest-observation')||args.includes('--open-window-cursors');
const container='newapi-monitor-prod-readonly-monitor-1';
const out=path.resolve(opt('--out',''));
const docker=(argv,extra={})=>execFileSync('docker',argv,{encoding:'utf8',windowsHide:true,timeout:12000,maxBuffer:8*1024**2,stdio:['pipe','pipe','pipe'],...extra});
const inspect=()=>JSON.parse(docker(['inspect',container]))[0];
const save=(name,value)=>fs.writeFileSync(path.join(out,name+'.json'),JSON.stringify(value,null,2)+'\n');
const queryPlan=(epoch,ids)=>{
 // Match the existing fixed-window browser limit and seek exact identities.
 // This avoids shrinking a fully reconciled minute to a convenient subset.
 assert.match(epoch,/^[A-Za-z0-9._:-]+$/);assert.ok(ids.length>0&&ids.length<=600&&ids.every(Number.isSafeInteger));
 const identity="source_epoch='"+epoch+"' AND log_id IN ("+ids.join(',')+')';
 return {
  state:'SELECT source_epoch,start_at,scan_through,complete_through,fact_count,migration_version,migration_cursor FROM token_metering_states ORDER BY source_epoch',
  windows:'SELECT source_epoch,start_at,end_at,lane,cursor_at,cursor_id,done,source_complete,replay,attempts,next_at,read_page_rows,read_success_pages,read_worst_ms,rows_committed'+(openWindowCursors?',open_observed_at,seal_started':'')+' FROM token_metering_windows ORDER BY source_epoch,start_at',
  pending_projection:'SELECT id,cursor_epoch,cursor_id,ready FROM token_report_pending_migration_v1 WHERE id=1',
  state_projection:'SELECT id,cursor_epoch,cursor_id,ready FROM token_report_state_migration_v1 WHERE id=1',
  fixed_facts:'SELECT source_epoch,log_id,created_at,request_id,upstream_request_id,user_id,channel_id,model,user_group,request_path,request_method,source_facts FROM token_metering_facts INDEXED BY sqlite_autoindex_token_metering_facts_1 WHERE '+identity+' ORDER BY log_id',
  fixed_refs:'SELECT source_epoch,log_id,key_id,hash FROM token_metering_refs INDEXED BY sqlite_autoindex_token_metering_refs_1 WHERE '+identity+' ORDER BY log_id,key_id'
 };
};
function digest(snapshot){
 snapshot.fixed_facts=snapshot.fixed_facts.map(({source_facts,...row})=>({...row,source_facts_sha256:createHash('sha256').update(source_facts).digest('hex')}));
 return snapshot;
}
if(args.includes('--native-snapshot')){
 const runtime=inspect();assert.equal(runtime.State.Running,false);assert.equal(runtime.Image,opt('--image-id'));
 const {DatabaseSync}=await import('node:sqlite');
 const db=new DatabaseSync(opt('--db'),{readOnly:true,timeout:1000});
 try{
  db.exec('PRAGMA query_only=ON; BEGIN;');
  const queries=JSON.parse(fs.readFileSync(opt('--queries'),'utf8'));
  const snapshot=Object.fromEntries(Object.entries(queries).map(([k,sql])=>[k,db.prepare(sql).all()]));
  db.exec('COMMIT;');console.log(JSON.stringify(digest(snapshot)));
 }finally{db.close()}
 process.exit(0);
}
for(const flag of ['--image','--out','--source','--observation','--db'])assert.ok(args.includes(flag),flag+' required');
assert.ok(/^D:/i.test(out),'evidence must be on D:');
const observation=JSON.parse(fs.readFileSync(opt('--observation'),'utf8'));
if(args.includes('--latest-observation')){
 assert.equal(observation.image,opt('--image'));
 assert.equal(observation.actual_data,true);assert.equal(observation.response_replacement,false);
 assert.equal(observation.error,null);assert.ok(observation.elapsed_seconds>=120&&observation.samples>=4&&observation.visible_rows>0,'complete multiple real latest-data cycles before restarting');
}else{
 assert.equal(observation.expected_image,opt('--image'));
 assert.equal(observation.thirty_minutes_observed,true,'complete the continuous observation before restarting');
}
const source=path.resolve(opt('--source')),proof=JSON.parse(fs.readFileSync(path.join(source,'reconciliation.json'),'utf8'));
assert.equal(proof.complete,true,'use a fully captured fixed source window');
const ids=[];
for(const name of proof.segments?.map(v=>v.file)||['row-reconciliation.jsonl.gz']){
 assert.equal(path.basename(name),name);
 const reader=createInterface({input:fs.createReadStream(path.join(source,name)).pipe(createGunzip()),crlfDelay:Infinity});
 for await(const line of reader)if(line)ids.push(JSON.parse(line).source.id);
}
assert.equal(ids.length,proof.rows);assert.equal(new Set(ids).size,ids.length);
fs.mkdirSync(out,{recursive:true});
const queries=queryPlan(proof.source_epoch,ids);save('queries',queries);
// Check both point-lookup plans before the one authorized restart. A missing
// index or incompatible schema must fail while the original process is live.
const plans={};
for(const key of ['fixed_facts','fixed_refs']){
 const text=docker(['exec','-i','--user','1000:1000',opt('--sqlite-container','token-review-audit-db'),'timeout','5','sqlite3','-readonly','-json','/token-data/token.db'],{input:'.timeout 1000\n.explain off\nPRAGMA query_only=ON;\nEXPLAIN QUERY PLAN '+queries[key]+';\n'});
 plans[key]=JSON.parse(text.trim()||'[]');assert.ok(plans[key].some(row=>/SEARCH.*source_epoch=\? AND log_id=\?/.test(row.detail)),key+' must use identity seeks');
}
save('identity-query-plans',plans);
const baseline=inspect();assert.equal(baseline.Config.Image,'newapi-monitor:'+opt('--image'));assert.ok(baseline.State.Running);
const report={started_at:new Date().toISOString(),image:opt('--image'),image_id:baseline.Image,container_id:baseline.Id,observation_scope:args.includes('--latest-observation')?'latest_data_cycles':'thirty_minutes',source_queries:0,failures:[],open_window_overlap_resumes:[],pass:false};
let before;
// The finally block restores the same container even when a local query fails.
try{
 docker(['stop','--timeout','40',container],{timeout:50000});
 const child=spawnSync(process.execPath,['--disable-warning=ExperimentalWarning',import.meta.filename,'--native-snapshot','--db',path.resolve(opt('--db')),'--queries',path.join(out,'queries.json'),'--image-id',baseline.Image],{encoding:'utf8',windowsHide:true,timeout:20000,maxBuffer:8*1024**2,env:{...process.env,TEMP:out,TMP:out}});
 assert.equal(child.status,0,'bounded stopped snapshot failed: '+(child.error?.code||child.status));
 before=JSON.parse(child.stdout);save('before',before);
 console.log(JSON.stringify({stage:'stopped_snapshot',windows:before.windows.length,fixed_rows:before.fixed_facts.length}));
}catch(e){report.failures.push(e.message)}
finally{
 if(!inspect().State.Running)docker(['start',container],{timeout:20000});
 save('verification',report);
}
const origin='http://127.0.0.3:8204',wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
let live=false,runtime;
const deadline=Date.now()+180000;
while(Date.now()<deadline){
 runtime=inspect();
 try{live=(await fetch(origin+'/live',{signal:AbortSignal.timeout(3000)})).status===200}catch{live=false}
 if(live&&runtime.State.Health?.Status==='healthy')break;
 await wait(5000);
}
report.live=live;report.health=runtime.State.Health?.Status;
report.same_container=runtime.Id===baseline.Id&&runtime.Image===baseline.Image;
report.new_start=runtime.State.StartedAt!==baseline.State.StartedAt;
report.started_after=runtime.State.StartedAt;
if(!live||report.health!=='healthy'||!report.same_container||!report.new_start)report.failures.push('runtime_restart_identity_or_health');
console.log(JSON.stringify({stage:'started',live,health:report.health,same_container:report.same_container}));
save('verification',report);
if(before&&live){
 await wait(45000);
 try{
  const after={};
  for(const [key,sql] of Object.entries(queries)){
   const raw=docker(['exec','-i','--user','1000:1000',opt('--sqlite-container','token-review-audit-db'),'timeout','8','sqlite3','-readonly','-json','/token-data/token.db'],{input:'.timeout 1000\nPRAGMA query_only=ON;\n'+sql+';\n'});
   after[key]=JSON.parse(raw.trim()||'[]');
  }
  digest(after);save('after',after);
  const expect=(ok,reason)=>{if(!ok)report.failures.push(reason)};
  for(const s of before.state){const a=after.state.find(v=>v.source_epoch===s.source_epoch);expect(a&&a.scan_through>=s.scan_through&&a.complete_through>=s.complete_through&&a.migration_cursor>=s.migration_cursor&&a.fact_count>=s.fact_count,'state_regressed:'+s.source_epoch)}
  const windows=new Map(after.windows.map(w=>[w.source_epoch+':'+w.start_at,w]));
  let active=0,unchanged=0;
  for(const b of before.windows){
   const a=windows.get(b.source_epoch+':'+b.start_at);expect(!!a,'window_missing:'+b.start_at);if(!a)continue;
   if(!b.done&&!b.replay&&!a.replay){
    active++;const monotone=a.cursor_at>b.cursor_at||a.cursor_at===b.cursor_at&&a.cursor_id>=b.cursor_id;
    // Open EOF may restart a bounded overlap, or reread its own bucket on
    // first seal. Durable fact identity/values are checked independently below.
    const overlap=openWindowCursors&&b.lane==='realtime_tail'&&!b.seal_started&&a.cursor_at>=b.start_at&&(a.seal_started||a.cursor_at>=Math.max(b.start_at,b.cursor_at-60));
    expect(monotone||overlap,'prefix_regressed:'+b.start_at);
    if(!monotone&&overlap)report.open_window_overlap_resumes.push({start_at:b.start_at,before:[b.cursor_at,b.cursor_id],after:[a.cursor_at,a.cursor_id],first_seal:!!a.seal_started});
   }
   if(a.rows_committed===b.rows_committed&&a.attempts===b.attempts&&a.replay===b.replay&&a.done===b.done){
    unchanged++;
    // A resumed timeout can legitimately reset adaptive feedback without a
    // committed row. Preserve the before/after values for that distinction.
   }
  }
  for(const k of ['pending_projection','state_projection'])for(const b of before[k]){const a=after[k].find(v=>v.id===b.id);expect(a&&(a.cursor_epoch>b.cursor_epoch||a.cursor_epoch===b.cursor_epoch&&a.cursor_id>=b.cursor_id)&&a.ready>=b.ready,'projection_regressed:'+k)}
  report.fixed_source_fields_equal=JSON.stringify(after.fixed_facts)===JSON.stringify(before.fixed_facts);
  report.fixed_refs_equal=JSON.stringify(after.fixed_refs)===JSON.stringify(before.fixed_refs);
  expect(report.fixed_source_fields_equal,'fixed_source_fields_changed');expect(report.fixed_refs_equal,'fixed_refs_changed');
  expect(after.fixed_facts.length===ids.length,'fixed_facts_missing');
  expect(new Set(after.fixed_facts.map(v=>v.source_epoch+':'+v.log_id)).size===ids.length,'fixed_facts_duplicate');
  report.windows_checked=before.windows.length;report.active_prefixes_checked=active;report.fixed_rows=ids.length;report.windows_without_commit_or_attempt_change=unchanged;
 }catch(e){report.failures.push('after_query_or_verification_failed:'+e.message)}
}
report.completed_at=new Date().toISOString();report.pass=report.failures.length===0;save('verification',report);console.log(JSON.stringify(report));
process.exitCode=report.pass?0:1;
