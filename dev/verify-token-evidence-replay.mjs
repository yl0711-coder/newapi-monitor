// Recheck captured production source rows against the current local runtime.
// This never queries production, and never promotes a partial source capture
// to complete window coverage.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createGunzip,createGzip} from 'node:zlib';
import {createInterface} from 'node:readline';
import {pipeline} from 'node:stream/promises';
import {once} from 'node:events';
import {createHmac,createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {tokenHeadColumns,tokenHeadJoins,tokenHeadStaleReasons} from './token-detection-validity.mjs';
const args=process.argv.slice(2),opt=(k,d)=>args.includes(k)?args[args.indexOf(k)+1]:d;
assert.ok(args.includes('--source')&&args.includes('--out')&&args.includes('--image'));
const sourceDir=path.resolve(opt('--source')),out=path.resolve(opt('--out')),expected=opt('--image');
const proof=JSON.parse(fs.readFileSync(path.join(sourceDir,'reconciliation.json'),'utf8'));
const files=proof.segments?.map(v=>v.file)||['row-reconciliation.jsonl.gz'];
const epoch=proof.source_epoch;assert.match(epoch,/^[A-Za-z0-9._:-]+$/);
fs.mkdirSync(out,{recursive:true});
const docker=(argv,extra={})=>execFileSync('docker',argv,{encoding:'utf8',windowsHide:true,timeout:12000,maxBuffer:8*1024**2,...extra});
const runtime=JSON.parse(docker(['inspect','newapi-monitor-prod-readonly-monitor-1']))[0];assert.equal(runtime.Config.Image,'newapi-monitor:'+expected);assert.ok(runtime.State.Running);
const login=await fetch('http://127.0.0.3:8204/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:process.env.MONITOR_LOCAL_USER||'local',password:process.env.MONITOR_LOCAL_PASSWORD||'local'}),signal:AbortSignal.timeout(10000)});
assert.equal(login.status,200);
const cookie=login.headers.getSetCookie().map(v=>v.split(';')[0]).join('; ');
const healthResponse=await fetch('http://127.0.0.3:8204/anomalies/token/detection/health',{headers:{Cookie:cookie},signal:AbortSignal.timeout(10000)});
assert.equal(healthResponse.status,200);
const policy=(await healthResponse.json()).policy_hash;assert.match(policy,/^[a-f0-9]{64}$/);
const env=Object.fromEntries(runtime.Config.Env.map(v=>{const at=v.indexOf('=');return [v.slice(0,at),v.slice(at+1)]}));
const keys=[];
for(const p of ['MONITOR_NGINX_EVIDENCE','MONITOR_NGINX_EVIDENCE_PREVIOUS','MONITOR_CLOUDWATCH_EVIDENCE']){const id=env[p+'_HMAC_KEY_ID'],secret=env[p+'_HMAC_KEY'];if(id&&secret&&!keys.some(v=>v.id===id))keys.push({id,secret})}
const literal=v=>"'"+String(v).replaceAll("'","''")+"'";
let rows=0,missing=0,mismatched=0,detected=0,stale=0,duplicates=0,error='';const states={},reasons={},staleReasons={},seen=new Set(),start=Date.now();
const stream=createGzip({level:3}),written=pipeline(stream,fs.createWriteStream(path.join(out,'row-reconciliation.jsonl.gz'),{flags:'wx'}));written.catch(()=>{});
async function check(batch){
 const ids=batch.map(v=>v.id);assert.ok(ids.every(Number.isSafeInteger));
 const query="SELECT f.*,c.fact_revision AS detected_fact_revision,c.link_revision AS detected_link_revision,c.detection_state,"+tokenHeadColumns+",(SELECT json_group_array(json_object('key_id',r.key_id,'hash',r.hash)) FROM token_metering_refs r WHERE r.source_epoch=f.source_epoch AND r.log_id=f.log_id) AS refs FROM token_metering_facts f LEFT JOIN token_detection_currents c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id"+tokenHeadJoins+"WHERE f.source_epoch="+literal(epoch)+' AND f.log_id IN ('+ids.join(',')+')';
 const values=JSON.parse(docker(['exec','-i','--user','1000:1000',opt('--sqlite-container','token-review-audit-db'),'timeout','8','sqlite3','-readonly','-json','/token-data/token.db'],{input:'.timeout 1000\nPRAGMA query_only=ON;\n'+query+';\n'}).trim()||'[]');
 const map=new Map(values.map(v=>[v.log_id,v]));duplicates+=values.length-map.size;
 for(const source of batch){
  if(seen.has(source.id))duplicates++;seen.add(source.id);
  const f=map.get(source.id),bad=[];let usage=null,refs=[];
  if(!f){missing++;bad.push('fact_missing')}
  else{
   for(const k of ['created_at','user_id','channel_id','model','user_group','request_id'])if(String(f[k]??'')!==String(source[k]??''))bad.push(k+'_mismatch');
   try{usage=JSON.parse(f.source_facts);refs=JSON.parse(f.refs)}catch{bad.push('invalid_stored_json')}
   for(const k of ['prompt_tokens','completion_tokens','quota'])if(String(usage?.fields?.[k]?.value??'')!==String(source[k]??''))bad.push(k+'_mismatch');
   const other=typeof source.other==='string'?JSON.parse(source.other):source.other;
   for(const [k,value] of Object.entries(other||{})){if(value===null)continue;const actual=k==='upstream_request_id'?f.upstream_request_id:usage?.fields?.[k==='method'?'request_method':k]?.value;if(String(actual??'')!==String(value))bad.push(k+'_source_operand_mismatch')}
   if(source.request_id)for(const key of keys){const hash=createHmac('sha256',key.secret).update('oneapi-request-id\0'+source.request_id.trim()).digest('hex');if(!refs.some(v=>v.key_id===key.id&&v.hash===hash))bad.push('request_hmac_ref_mismatch')}
   if(source.request_id&&!refs.length)bad.push('request_refs_missing');
   const state=f.detection_state||'detection_pending';states[state]=(states[state]||0)+1;
   f.audit_stale_reasons=tokenHeadStaleReasons(f,policy);
   for(const reason of f.audit_stale_reasons)staleReasons[reason]=(staleReasons[reason]||0)+1;
   if(f.detection_state){detected++;if(f.audit_stale_reasons.length)stale++}
  }
  if(bad.length)mismatched++;for(const reason of bad)reasons[reason]=(reasons[reason]||0)+1;
  const line=JSON.stringify({source,fact:f?{...f,source_facts_sha256:createHash('sha256').update(f.source_facts).digest('hex'),refs}:null,reasons:bad})+'\n';if(!stream.write(line))await once(stream,'drain');rows++;
 }
}
try{
 for(const file of files){assert.equal(path.basename(file),file);const reader=createInterface({input:fs.createReadStream(path.join(sourceDir,file)).pipe(createGunzip()),crlfDelay:Infinity});let batch=[];
  for await(const line of reader){assert.ok(Date.now()-start<300000,'local replay budget exhausted');if(!line)continue;batch.push(JSON.parse(line).source);if(batch.length===16){await check(batch);batch=[]}}
  if(batch.length)await check(batch);
 }
}catch(e){error=e.message}
finally{stream.end();try{await written}catch{error=error||'evidence_write_failed'}}
const result={at:new Date().toISOString(),image:expected,source_capture:sourceDir,source_queries:0,source_epoch:epoch,from:proof.from,to:proof.to,source_capture_complete:proof.complete===true,replayed_all_captured_rows:!error&&rows===proof.rows,rows,missing_facts:missing,mismatched_rows:mismatched,mismatch_reasons:reasons,duplicate_identities:duplicates,detected,stale,detection_states:states,error,elapsed_seconds:(Date.now()-start)/1000};
result.policy_hash=policy;result.stale_reasons=staleReasons;result.validity_scope='fact/link/policy/reference-generation/source-window';
result.pass=result.source_capture_complete&&result.replayed_all_captured_rows&&rows>0&&!missing&&!mismatched&&!duplicates&&detected===rows&&!stale;
fs.writeFileSync(path.join(out,'reconciliation.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));process.exitCode=result.pass?0:1;
