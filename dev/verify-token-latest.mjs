// Observe the real latest-data pipeline without changing source records or responses.
// Request created_at is NOT a database commit timestamp. Source-to-page timing
// is interval-censored by complete source polls; unavailable bounds stay null.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync, spawn} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {browserSession} from './browser-session.mjs';
import {tokenHeadColumns,tokenHeadJoins,tokenHeadStaleReasons} from './token-detection-validity.mjs';

const args=process.argv.slice(2),opt=(k,d)=>args.includes(k)?args[args.indexOf(k)+1]:d;
for(const k of ['--browser','--scratch','--out','--image','--sqlite-container'])assert.ok(args.includes(k),k);
const out=path.resolve(opt('--out')),image=opt('--image'),helper=opt('--sqlite-container');
const duration=Number(opt('--seconds','600')),interval=Number(opt('--interval','20'));
assert.ok(duration>=120&&duration<=1200&&interval>=10&&interval<=60&&/^[A-Za-z0-9_-]+$/.test(helper));
const sourceRows=Number(opt('--source-rows','128'));
assert.ok(Number.isInteger(sourceRows)&&sourceRows>=32&&sourceRows<=256);
fs.mkdirSync(out,{recursive:true});
const write=(n,v)=>fs.writeFileSync(path.join(out,n),JSON.stringify(v,null,2)+'\n');
const docker=(a,o={})=>execFileSync('docker',a,{encoding:'utf8',windowsHide:true,timeout:12000,maxBuffer:4*1024**2,stdio:['pipe','pipe','pipe'],...o});
const runtime=JSON.parse(docker(['inspect','newapi-monitor-prod-readonly-monitor-1']))[0];
assert.equal(runtime.Config.Image,'newapi-monitor:'+image);
const env=Object.fromEntries(runtime.Config.Env.map(v=>{const i=v.indexOf('=');return[v.slice(0,i),v.slice(i+1)]}));
const dsn=env.NEWAPI_LOG_DSN?.match(/^(nexus_ro|monitor_ro):(.+)@tcp\(([^)]+)\)\/([A-Za-z0-9_]+)(?:\?.*)?$/);
assert.ok(dsn,'configured read-only log source required');
const [host,port='3306']=dsn[3].split(':'),networks=Object.keys(runtime.NetworkSettings.Networks);
assert.equal(networks.length,1);
function connectSource(){
const name='token-latest-observer-'+randomUUID().slice(0,8);
const sourceProc=spawn('docker',['run','--rm','-i','--name',name,'--pull=never','--read-only','--cap-drop=ALL','--network',networks[0],...(runtime.HostConfig.ExtraHosts||[]).flatMap(v=>['--add-host',v]),'--env','MYSQL_PWD','--entrypoint','mysql','mysql:8.0','--connect-timeout=5','--default-character-set=utf8mb4','--batch','--raw','--unbuffered','--force','--skip-column-names','--host='+host,'--port='+port,'--user='+dsn[1],dsn[4]],{windowsHide:true,env:{...process.env,MYSQL_PWD:dsn[2]},stdio:['pipe','pipe','pipe']});
let pending=null,buffer='';
sourceProc.stdout.setEncoding('utf8');sourceProc.stderr.setEncoding('utf8');
sourceProc.stdout.on('data',chunk=>{buffer+=chunk;for(let i;(i=buffer.indexOf('\n'))>=0;){const line=buffer.slice(0,i).replace(/\r$/,'');buffer=buffer.slice(i+1);if(!pending)continue;if(line===pending.marker){const q=pending;pending=null;clearTimeout(q.timer);q.error?q.reject(Error(q.error)):q.resolve(q.lines.join('\n'));}else if(pending.lines.length<512)pending.lines.push(line);else pending.error='source_result_limit';}});
sourceProc.stderr.on('data',chunk=>{if(pending&&/ERROR\s+\d+/.test(chunk))pending.error='source_mysql_'+chunk.match(/ERROR\s+(\d+)/)[1];});
sourceProc.on('error',()=>{if(pending){clearTimeout(pending.timer);pending.reject(Error('source_process_failed'));pending=null;}});
sourceProc.on('close',()=>{if(pending){const q=pending;pending=null;clearTimeout(q.timer);q.reject(Error(q.error||'source_connection_closed'));}});
const source=sql=>new Promise((resolve,reject)=>{assert.equal(pending,null);const marker='end_'+randomUUID().replaceAll('-','');pending={resolve,reject,marker,lines:[],timer:setTimeout(()=>{pending=null;reject(Error('source_read_deadline'));},15000)};sourceProc.stdin.write('SET SESSION MAX_EXECUTION_TIME=3000; START TRANSACTION READ ONLY; '+sql+"; COMMIT; SELECT '"+marker+"';\n");});
return {source,close(){sourceProc.stdin.end('quit\n');try{docker(['stop','--time','2',name],{timeout:5000});}catch{}sourceProc.kill();}};
}
let sourceSession=null,nextSourceAt=0;
const sourceFailures=[];
const literal=v=>"'"+String(v).replaceAll("'","''")+"'";
const local=sql=>JSON.parse(docker(['exec','-i','--user','1000:1000',helper,'timeout','5','sqlite3','-readonly','-json','/token-data/token.db'],{input:".timeout 1000\nPRAGMA query_only=ON;\nATTACH DATABASE 'file:/data/nginx-evidence.db?mode=ro' AS entry;\nBEGIN;\n"+sql+';\nCOMMIT;\n'}).trim()||'[]');
const origin='http://127.0.0.3:8204',b=await browserSession({browser:opt('--browser'),scratch:opt('--scratch'),origin});
const {evaluate:ev,call,waitFor}=b,records=new Map(),samples=[],started=Date.now();let error=null,previousSource=null;
const probe="(()=>{const f=window.fetch.bind(window);window.__latest={reports:[]};window.fetch=async(...a)=>{const r=await f(...a);if(String(a[0]).includes('/anomalies/token/detection/query'))window.__latest.reports.push({at:Date.now(),body:await r.clone().json()});return r;};})()";
try{
 await call('Page.addScriptToEvaluateOnNewDocument',{source:probe});
 assert.equal(await b.authenticate(),200);
 // Read the policy before opening the application, whose normal first load
 // already requests health. Do not add a duplicate health scan to that burst.
 await call('Page.navigate',{url:origin+'/live'});await waitFor("location.pathname==='/live'&&document.readyState==='complete'",15000);
 assert.equal(await ev("fetch('/ready').then(r=>r.json()).then(r=>r.image_version)"),image);
 const health=await ev("fetch('/anomalies/token/health').then(r=>r.json())");
 const epoch=health.source_epoch;assert.ok(typeof epoch==='string'&&epoch.length>0,'facts health source epoch required');
 write('initial-facts-health.json',health);
 const detectionHealth=await ev("fetch('/anomalies/token/detection/health').then(r=>r.json())");
 write('initial-detection-health.json',detectionHealth);
 const policy=detectionHealth.policy_hash;assert.ok(typeof policy==='string'&&/^[a-f0-9]{64}$/.test(policy),'initial detection health has no policy hash; preserve response and wait for readiness');
 await call('Page.navigate',{url:origin+'/#tab=anomalies'});await waitFor("typeof window.monitorNavigate==='function'&&!!document.getElementById('taFrom')",20000);
 await ev("window.monitorNavigate('anomalies',{view:'token'})");
 let checkedPlan=false;
 for(let cycle=0;Date.now()-started<=duration*1000;cycle++){
  // Read an explicitly bounded future endpoint as well. A complete preceding
  // poll then proves absence for newly created rows in the next poll without
  // assuming that created_at is their database commit timestamp.
  const from=Math.floor(Date.now()/1000)-30,to=Math.floor(Date.now()/1000)+60;
  const sql="SELECT /*+ MAX_EXECUTION_TIME(3000) */ JSON_OBJECT('id',id,'created_at',created_at,'request_id',request_id,'prompt_tokens',CAST(prompt_tokens AS CHAR),'completion_tokens',CAST(completion_tokens AS CHAR),'quota',CAST(quota AS CHAR)) FROM logs FORCE INDEX(idx_created_at_type) WHERE type=2 AND created_at>="+from+' AND created_at<'+to+' ORDER BY created_at DESC,id DESC LIMIT '+sourceRows;
  const sourceBefore=Date.now();let rows=[],sourceStatus='backoff',sourceError=null;
  if(sourceBefore>=nextSourceAt){
   try{
    sourceSession??=connectSource();
    if(!checkedPlan){const plan=await sourceSession.source('EXPLAIN '+sql);assert.ok(/idx_created_at_type/.test(plan)&&!/\tALL\t/.test(plan));write('source-plan.json',{sql,plan,read_only:true,budget_ms:3000});checkedPlan=true;}
    rows=(await sourceSession.source(sql)).trim().split('\n').filter(Boolean).map(s=>JSON.parse(s));sourceStatus='read';
   }catch(e){
    // A failed connection is evidence, not a zero-row result or the end of
    // observation. Release it, keep watching local/UI state, and retry only
    // after a minute; one source connection and the original SQL budget remain.
    sourceStatus='failed';sourceError=e.message;sourceFailures.push({cycle,at:new Date().toISOString(),error:sourceError});
    sourceSession?.close();sourceSession=null;nextSourceAt=Date.now()+60000;previousSource=null;
   }
  }
  const sourceAt=Date.now();
  for(const row of rows)if(!records.has(row.id)){assert.ok(Number.isSafeInteger(row.id));records.set(row.id,{source:row,source_first_observed_at:sourceAt,source_commit_after:previousSource?.complete&&row.created_at>=previousSource.from&&row.created_at<previousSource.to?previousSource.before:null,history:[]});}
  if(sourceStatus==='read')previousSource={from,to,before:sourceBefore,complete:rows.length<sourceRows};
  // The observer and UI may both tick every thirty seconds. Sampling exactly
  // as the UI starts its next refresh otherwise repeatedly records the busy
  // state even though each real response renders successfully between polls.
  // Wait within a hard bound; retain a timeout as evidence of an unsettled UI.
  const pageWaitStarted=Date.now();let pageWaitError=null;
  try{await waitFor("document.getElementById('taList')?.getAttribute('aria-busy')==='false'",10000)}catch(e){pageWaitError=e.message}
  const page=await ev("(()=>{const r=window.__latest.reports.at(-1);return {report:r,settled:document.getElementById('taList')?.getAttribute('aria-busy')==='false',text:document.getElementById('taList')?.innerText||'',report_count:window.__latest.reports.length};})()");
  const pageAt=Date.now(),report=page.report?.body;
  // Continue observing already-seen source identities after they leave the
  // source read window; the local indexed probe has an independent hard cap.
  // Keep probing older unresolved identities as arrivals continue. Probing
  // only the newest 256 hid requests that waited after leaving that suffix.
  const recent=[...records.values()].sort((a,b)=>b.source.created_at-a.source.created_at||b.source.id-a.source.id).slice(0,128);
  const unresolved=[...records.values()].filter(r=>!r.local_latest?.audit_current).sort((a,b)=>(a.last_local_probe_at||0)-(b.last_local_probe_at||0)||a.source.id-b.source.id);
  const ids=[...new Set([...unresolved.slice(0,128),...recent].map(r=>r.source.id))];
  const localRows=ids.length?local("SELECT f.log_id,f.request_id,f.created_at,f.revision,f.link_revision,f.link_state,f.first_seen_at,c.fact_revision AS decision_fact_revision,c.link_revision AS decision_link_revision,c.evaluated_at,c.detection_state,c.candidate,c.result_json,"+tokenHeadColumns+",(SELECT MAX(e.received_at) FROM json_each(CASE WHEN json_valid(f.evidence_json) THEN f.evidence_json ELSE '[]' END) j JOIN entry.nginx_request_evidences e ON e.event_id=json_extract(j.value,'$.event_id')) AS entry_received_at FROM token_metering_facts f LEFT JOIN token_detection_currents c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id"+tokenHeadJoins+"WHERE f.source_epoch="+literal(epoch)+' AND f.log_id IN ('+ids.join(',')+')'):[];
  const localAt=Date.now();
  for(const id of ids)records.get(id).last_local_probe_at=localAt;
  for(const row of localRows){const r=records.get(row.log_id);if(!r)continue;assert.equal(row.request_id,r.source.request_id);row.audit_stale_reasons=tokenHeadStaleReasons({...row,detected_fact_revision:row.decision_fact_revision,detected_link_revision:row.decision_link_revision},policy);row.audit_current=row.audit_stale_reasons.length===0;r.local_latest={...row,observed_at:localAt,result_json:undefined};const ready=Math.max(row.first_seen_at,row.entry_received_at)*1000,matched=row.audit_current;if(row.entry_received_at&&!matched&&localAt-ready>60000)r.proven_over_minute_pending??={log_id:r.source.id,evidence_available_at:ready,still_pending_at:localAt,proven_wait_ms:localAt-ready};const signature=JSON.stringify([row.revision,row.link_revision,row.decision_fact_revision,row.decision_link_revision,row.detection_state,row.entry_received_at,row.audit_stale_reasons]);if(signature!==r.last_signature){r.history.push({observed_at:localAt,...row,result_json:JSON.parse(row.result_json||'null')});r.last_signature=signature;}}
  if(page.settled&&report?.available)for(const item of report.items||[]){const r=records.get(item.request.log_id);if(!r||!page.text.includes(r.source.request_id))continue;if(!r.page_first_visible_at){r.page_first_visible_at=pageAt;r.source_observed_to_page_ms=Math.max(0,pageAt-r.source_first_observed_at);r.source_commit_to_page_upper_ms=r.source_commit_after==null?null:pageAt-r.source_commit_after;}assert.equal(item.display.recorded_input_tokens,r.source.prompt_tokens);assert.equal(item.display.output_tokens,r.source.completion_tokens);assert.equal(item.display.recorded_quota,r.source.quota);assert.ok(Array.isArray(item.display.missing_fields));r.page_latest={at:pageAt,display:item.display,decision:item.request,result:item.result};}
  samples.push({cycle,observed_at:new Date().toISOString(),source_range:{from,to},source_status:sourceStatus,source_error:sourceError,source_rows:rows.length,source_sample_limit:sourceRows,source_sample_truncated:rows.length===sourceRows,source_read_ms:sourceAt-sourceBefore,page_wait_ms:pageAt-pageWaitStarted,page_wait_error:pageWaitError,local_probe_limit:256,local_rows:localRows.length,report_count:page.report_count,range:report?.range,available:report?.available,settled:page.settled,latest_progress:report?.latest_progress,entry_recent_observations:report?.entry_recent_observations});
  write('observations.json',{image,actual_data:true,response_replacement:false,source_concurrency:1,source_retry_seconds:60,source_failures:sourceFailures,interval_seconds:interval,samples,records:[...records.values()]});
  console.log(JSON.stringify({cycle,source_status:sourceStatus,source_error:sourceError,source_rows:rows.length,local_rows:localRows.length,visible:[...records.values()].filter(r=>r.page_first_visible_at).length,reports:page.report_count,latest:report?.latest_progress}));
  const wait=started+(cycle+1)*interval*1000-Date.now();if(wait>0)await b.pause(Math.min(wait,60000));
 }
 await b.screenshot(path.join(out,'latest-page.png'));
 assert.equal(b.exceptions.length,0);
 const complete=samples.filter(s=>s.available&&s.settled),first=complete[0],last=complete.at(-1);
 assert.ok(complete.length>=3&&last.range.to>first.range.to,'relative query range did not advance');
 assert.ok(samples.filter(s=>s.source_status==='read').length>=3,'fewer than three successful bounded source polls');
 assert.ok(last.latest_progress.collected_through>first.latest_progress.collected_through,'new collected requests did not reach the browser');
 // This field samples the newest 100 identities, not a monotone watermark:
 // a fresh suffix can temporarily contain no completed head. Verify progress
 // across the observed cycles, and separately measure unresolved identities.
 assert.ok(Math.max(...complete.slice(1).map(s=>s.latest_progress.detected_through||0))>(first.latest_progress.detected_through||0),'latest decisions did not advance');
}catch(e){error=e.message;console.error(error);}
finally{
 const rows=[...records.values()],visible=rows.filter(r=>r.page_first_visible_at);
 const evidenceDecisions=rows.flatMap(r=>r.history.filter(h=>h.entry_received_at*1000>=started&&h.audit_current&&h.evaluated_at>=h.entry_received_at).map(h=>({log_id:r.source.id,entry_received_at:h.entry_received_at,decision_recorded_at:h.evaluated_at,decision_first_observed_at:h.observed_at,latency_upper_ms:Math.max(0,h.observed_at-h.entry_received_at*1000)})));
 const overdue=rows.filter(r=>r.proven_over_minute_pending).map(r=>({...r.proven_over_minute_pending,resolved_by_last_observation:r.local_latest?.audit_current===true}));
 if(overdue.length&&!error)error='observed linked requests remained pending for more than one minute';
 write('result.json',{at:new Date().toISOString(),image,actual_data:true,response_replacement:false,elapsed_seconds:(Date.now()-started)/1000,samples:samples.length,source_failures:sourceFailures,successful_source_polls:samples.filter(s=>s.source_status==='read').length,observed_source_rows:rows.length,visible_rows:visible.length,source_commit_timestamp_available:false,source_to_page_method:'upper bound from preceding complete absence poll; otherwise first-observed-to-visible only',source_to_page:visible.map(r=>({log_id:r.source.id,observed_to_page_ms:r.source_observed_to_page_ms,commit_to_page_upper_ms:r.source_commit_to_page_upper_ms})),entry_to_decision_method:'local evidence received_at to first observed matching current revision; upper bound includes polling',entry_to_decision:evidenceDecisions,over_minute_pending:overdue,error,exceptions:b.exceptions,external:b.external});
 sourceSession?.close();await b.close();
}
if(error)process.exitCode=1;
