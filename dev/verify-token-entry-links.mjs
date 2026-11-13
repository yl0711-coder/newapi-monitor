// Bounded local verification using an already captured, complete source window.
// No production/source API calls; association success is checked separately
// from logs -> facts equality so an ambiguous head cannot pass the full chain.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {gunzipSync} from 'node:zlib';
import {createHash} from 'node:crypto';
const args=process.argv.slice(2),opt=k=>args[args.indexOf(k)+1];
for(const k of ['--source','--image','--out','--sqlite-container'])assert.ok(args.includes(k),k);
const dir=path.resolve(opt('--source')),out=path.resolve(opt('--out'));
assert.ok(!fs.existsSync(out),'retain earlier verification output');
const proof=JSON.parse(fs.readFileSync(path.join(dir,'reconciliation.json')));
assert.equal(proof.complete,true);assert.ok(proof.rows>0&&proof.rows<=400);
assert.match(proof.source_epoch,/^[A-Za-z0-9._:-]+$/);
const source=proof.segments.flatMap(s=>{
 assert.equal(path.basename(s.file),s.file);
 const bytes=fs.readFileSync(path.join(dir,s.file));
 assert.equal(createHash('sha256').update(bytes).digest('hex'),s.sha256);
 return gunzipSync(bytes,{maxOutputLength:4*1024**2}).toString().trim().split('\n').filter(Boolean).map(v=>JSON.parse(v).source);
});
assert.equal(source.length,proof.rows);assert.ok(source.every(s=>Number.isSafeInteger(s.id)));
const run=(params,options={})=>execFileSync('docker',params,{encoding:'utf8',windowsHide:true,timeout:10000,maxBuffer:4*1024**2,...options});
const runtime=JSON.parse(run(['inspect','newapi-monitor-prod-readonly-monitor-1']))[0];
assert.equal(runtime.Config.Image,'newapi-monitor:'+opt('--image'));assert.ok(runtime.State.Running);
const origin='http://127.0.0.3:8204';
const login=await fetch(origin+'/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:process.env.MONITOR_LOCAL_USER||'local',password:process.env.MONITOR_LOCAL_PASSWORD||'local'}),signal:AbortSignal.timeout(10000)});
assert.equal(login.status,200);
const cookie=login.headers.getSetCookie().map(v=>v.split(';')[0]).join('; ');
const healthResponse=await fetch(origin+'/anomalies/token/detection/health',{headers:{Cookie:cookie},signal:AbortSignal.timeout(15000)});
assert.equal(healthResponse.status,200);const health=await healthResponse.json();
assert.ok(health.policy_hash,'current policy required for full decision validity');
const rows=[];
// Match the fixed-window browser limit while retaining small identity pages.
// No source query is repeated and no unbounded epoch/time scan is introduced.
for(let offset=0;offset<source.length;offset+=128){
const ids=source.slice(offset,offset+128).map(s=>s.id);
const query=`SELECT f.log_id,f.request_id,f.entry_event_id,f.entry_route,f.entry_method,f.request_length,f.request_length_meaning,f.link_state,f.link_reason,f.link_revision,e.event_id,e.oneapi_request_id,e.route,e.method,e.request_length AS entry_length,e.oneapi_id_hmac,e.hmac_key_id,e.received_at,(SELECT r.hash FROM token_metering_refs r WHERE r.source_epoch=f.source_epoch AND r.log_id=f.log_id AND r.key_id=e.hmac_key_id LIMIT 1) AS source_hmac,c.fact_revision,c.link_revision AS current_link_revision,c.evaluated_at,c.detection_state,f.revision,c.policy_hash,c.reference_generation,COALESCE(b.generation,0) AS expected_reference_generation,c.source_complete,CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END AS expected_source_complete FROM token_metering_facts f INDEXED BY sqlite_autoindex_token_metering_facts_1 LEFT JOIN entry.nginx_request_evidences e ON e.event_id=f.entry_event_id LEFT JOIN token_detection_currents c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300 WHERE f.source_epoch='${proof.source_epoch}' AND f.log_id IN (${ids.join(',')}) ORDER BY f.log_id;`;
const input=".timeout 1000\nPRAGMA query_only=ON;\nATTACH DATABASE 'file:/data/nginx-evidence.db?mode=ro' AS entry;\nBEGIN;\n"+query+'\nCOMMIT;\n';
rows.push(...JSON.parse(run(['exec','-i','--user','1000:1000',opt('--sqlite-container'),'timeout','5','sqlite3','-readonly','-json','/token-data/token.db'],{input})));
}
const sourceByID=new Map(source.map(s=>[s.id,s])),errors=[],states={};
for(const row of rows){
 const raw=sourceByID.get(row.log_id),issues=[];
 states[row.link_state]=(states[row.link_state]||0)+1;
 if(row.request_id!==raw?.request_id)issues.push('raw_id');
 if(row.link_state!=='unique')issues.push('not_unique');
 if(row.oneapi_request_id&&row.oneapi_request_id!==row.request_id)issues.push('entry_raw_id');
 if(!row.event_id||!row.source_hmac||row.oneapi_id_hmac!==row.source_hmac)issues.push('exact_hmac');
 if(row.entry_route!==row.route||row.entry_method!==row.method)issues.push('route_method');
 if(row.request_length!==row.entry_length||row.request_length_meaning!=='whole_http_request_not_body_or_token_count')issues.push('length');
 if(row.revision!==row.fact_revision||row.link_revision!==row.current_link_revision)issues.push('decision_versions');
 if(!row.detection_state)issues.push('decision_missing');
 if(row.policy_hash!==health.policy_hash)issues.push('decision_policy');
 if(row.reference_generation!==row.expected_reference_generation)issues.push('decision_reference_generation');
 if(row.source_complete!==row.expected_source_complete)issues.push('decision_source_window');
 if(issues.length)errors.push({id:row.log_id,issues});
}
const report={at:new Date().toISOString(),image:opt('--image'),source_queries:0,read_only:true,decision_validation:'fact/link/policy/reference-generation/source-window',expected_policy_hash:health.policy_hash,from:proof.from,to:proof.to,source_rows:source.length,rows,link_states:states,errors,pass:rows.length===source.length&&new Set(rows.map(r=>r.log_id)).size===source.length&&errors.length===0};
fs.writeFileSync(out,JSON.stringify(report,null,2)+'\n');
const reasonCounts={};for(const row of errors)for(const reason of row.issues)reasonCounts[reason]=(reasonCounts[reason]||0)+1;
console.log(JSON.stringify({source_rows:source.length,rows:rows.length,states,error_rows:errors.length,reason_counts:reasonCounts,pass:report.pass}));
process.exitCode=report.pass?0:1;
