// Offline index preparation for a Windows bind-mounted Token store. Runs only
// while the exact local container is stopped; no fact/queue/cursor DML is allowed.
// Index definitions mirror the named indexes in monitor/token_* files.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync,spawnSync} from 'node:child_process';
import {DatabaseSync} from 'node:sqlite';
const args=process.argv.slice(2),opt=(k,d)=>args.includes(k)?args[args.indexOf(k)+1]:d;
const file=path.resolve(opt('--db','.local-test-kit/prod-readonly/token-data/token.db'));
const out=path.resolve(opt('--out','')),image=opt('--image-id',''),container=opt('--container','newapi-monitor-prod-readonly-monitor-1');
const inspect=()=>JSON.parse(execFileSync('docker',['inspect',container],{encoding:'utf8',windowsHide:true,timeout:10000}))[0];
const definitions=[
 ['idx_token_link_due_identity_v1','token_metering_facts','source_epoch,link_next_at,log_id','link_next_at>0'],
 ['idx_token_fact_request_owner_v1','token_metering_facts','request_id,source_epoch,log_id',''],
 ['idx_token_fact_event_owner_v1','token_metering_facts','entry_event_id,source_epoch,log_id',''],
 ['idx_token_ref_exact_owner_v1','token_metering_refs','key_id,hash,source_epoch,log_id',''],
 ['idx_token_fact_cleanup_v1','token_metering_facts','source_epoch,created_at,log_id,link_state,link_next_at,revision,link_revision,stored_bytes',''],
 ['idx_token_detection_cleanup_v1','token_detection_currents','source_epoch,expires_at,log_id',''],
 ['idx_token_detection_discovery_v1','token_report_pending_members_v1','source_epoch,created_at,log_id,next_at,retry_present,head_present,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,start_at,fact_revision,link_revision',''],
 ['idx_token_detection_changed_group_v1','token_report_pending_groups_v1','source_epoch,retry_present,head_present,start_at','base_pending=1'],
 ['idx_token_detection_changed_identity_v1','token_report_pending_members_v1','source_epoch,created_at,log_id','base_pending=1 AND head_present=1'],
 ['idx_token_detection_retry_schedule_v1','token_detection_retries','source_epoch,next_at,log_id',''],
 ['idx_token_report_pending_no_head_v1','token_report_pending_members_v1','source_epoch,created_at,log_id,user_id,channel_id,model,user_group','head_present=0'],
 ['idx_token_report_summary_candidate_v1','token_detection_report_summaries_v1','source_epoch,candidate,detection_state,created_at,log_id',''],
 ['idx_token_detection_work_head_v2','token_detection_currents','source_epoch,log_id,fact_revision,link_revision,policy_hash,reference_generation,reference_bucket,source_complete,next_at,detection_state',''],
 ['idx_token_detection_epoch_state_v1','token_detection_currents','source_epoch,detection_state',''],
 ['idx_token_detection_epoch_retry_reason_v1','token_detection_retries','source_epoch,reason',''],
];
const definition=definition=>{const [name,table,columns,predicate]=definition;return 'CREATE INDEX IF NOT EXISTS '+name+' ON '+table+'('+columns+')'+(predicate?' WHERE '+predicate:'')};
if(args.includes('--one-index')){
 assert.ok(image,'worker image identity required');const stopped=inspect();assert.equal(stopped.State.Running,false);assert.equal(stopped.Image,image);
 const row=definitions.find(v=>v[0]===opt('--one-index'));assert.ok(row,'unknown index');
 const db=new DatabaseSync(file,{timeout:1000});
 try{
  db.exec('PRAGMA temp_store=MEMORY; PRAGMA cache_size=-131072; BEGIN IMMEDIATE;');
  if(row[0]==='idx_token_report_summary_candidate_v1'){
   // Match initTokenReportSummaryStore exactly. This changes only the derived
   // schema; its bounded application migration still owns all backfill work.
   const columns=db.prepare('PRAGMA table_info(token_detection_report_summaries_v1)').all();
   if(!columns.some(c=>c.name==='detection_state'))db.exec("ALTER TABLE token_detection_report_summaries_v1 ADD COLUMN detection_state TEXT NOT NULL DEFAULT ''");
   const old=db.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(row[0]);
   if(old&&!old.sql.toLowerCase().includes('detection_state'))db.exec('DROP INDEX idx_token_report_summary_candidate_v1');
  }
  db.exec(definition(row));db.exec('COMMIT;');console.log(JSON.stringify(db.prepare('SELECT name,sql FROM sqlite_schema WHERE name=?').get(row[0])));
 }finally{db.close()}
 process.exit(0);
}
if(args.includes('--snapshot')){console.log(JSON.stringify(localSnapshot()));process.exit(0)}
assert.ok(image&&args.includes('--out')&&args.includes('--backup'),'image, output and verified backup manifest are required');
assert.ok(/^D:/i.test(out),'migration evidence/temp must stay on D:');fs.mkdirSync(out,{recursive:true});
const beforeContainer=inspect();assert.equal(beforeContainer.State.Running,false);assert.equal(beforeContainer.Image,image);
const backup=JSON.parse(fs.readFileSync(opt('--backup'),'utf8'));assert.equal(backup.verified,true);assert.ok(backup.files.some(v=>v.name==='token.db.gz'&&v.decompression_verified));
assert.equal(path.resolve(backup.token_directory).toLowerCase(),path.dirname(file).toLowerCase(),'backup and database directory differ');
const capacity=()=>{const p=fs.statfsSync(file);assert.ok(p.bavail*p.bsize>=8*1024**3,'disk reserve exhausted')};capacity();
function localSnapshot(){
 const db=new DatabaseSync(file,{readOnly:true,timeout:1000});
 try{
  db.exec('PRAGMA query_only=ON');
  const counts={};for(const table of ['token_metering_facts','token_metering_refs','token_detection_currents','token_detection_revisions','token_detection_references','token_report_pending_members_v1'])counts[table]=db.prepare('SELECT COUNT(*) AS n FROM '+table).get().n;
  const states=db.prepare('SELECT source_epoch,start_at,scan_through,complete_through,fact_count,fact_bytes,migration_cursor FROM token_metering_states ORDER BY source_epoch').all();
  const windows=db.prepare('SELECT source_epoch,start_at,end_at,cursor_at,cursor_id,done,source_complete,replay,attempts,next_at FROM token_metering_windows ORDER BY source_epoch,start_at').all();
  return {counts,states,windows};
 }finally{db.close()}
}
const snapshot=()=>{const r=spawnSync(process.execPath,['--disable-warning=ExperimentalWarning',import.meta.filename,'--db',file,'--snapshot'],{encoding:'utf8',windowsHide:true,timeout:120000,maxBuffer:4*1024**2,env:{...process.env,TEMP:out,TMP:out}});assert.equal(r.status,0,'bounded readonly count/cursor snapshot failed');return JSON.parse(r.stdout)};
const report={started_at:new Date().toISOString(),container,image_id:image,read_only_source:true,fact_writes:false,backup:opt('--backup'),indices:[],before:snapshot(),verified:false};
fs.writeFileSync(path.join(out,'before.json'),JSON.stringify(report.before,null,2)+'\n');
const started=Date.now();
try{
 for(const row of definitions){
  assert.ok(Date.now()-started<12*60*1000,'overall index preparation budget exhausted');capacity();
  const c=inspect();assert.equal(c.State.Running,false);assert.equal(c.State.FinishedAt,beforeContainer.State.FinishedAt);
  const at=Date.now();console.log(JSON.stringify({stage:'index',name:row[0]}));
  const r=spawnSync(process.execPath,['--disable-warning=ExperimentalWarning',import.meta.filename,'--db',file,'--one-index',row[0],'--image-id',image,'--container',container],{encoding:'utf8',windowsHide:true,timeout:120000,env:{...process.env,TEMP:out,TMP:out}});
  const record={name:row[0],sql:definition(row),elapsed_ms:Date.now()-at,exit_code:r.status,error:r.error?.code||null,output:r.stdout?.trim(),stderr:r.stderr?.trim()};report.indices.push(record);console.log(JSON.stringify(record));
  fs.writeFileSync(path.join(out,'migration.json'),JSON.stringify(report,null,2)+'\n');
  assert.equal(r.status,0,'bounded index preparation failed: '+row[0]);
 }
 report.after=snapshot();assert.deepEqual(report.after,report.before,'facts/ref/result counts or durable cursors changed');
 const last=inspect();assert.equal(last.State.Running,false);assert.equal(last.State.FinishedAt,beforeContainer.State.FinishedAt);
 capacity();report.verified=true;
}catch(error){report.error=error.message;process.exitCode=1}
finally{report.completed_at=new Date().toISOString();fs.writeFileSync(path.join(out,'migration.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({stage:'finished',verified:report.verified,error:report.error,elapsed_ms:Date.now()-started}));}
