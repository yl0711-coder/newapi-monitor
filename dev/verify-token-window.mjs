// Bounded, read-only production logs -> local facts/refs/current reconciliation.
// A partial run is evidence of a partial run, never a coverage certificate.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync,spawn} from 'node:child_process';
import {createHmac,createHash,randomUUID} from 'node:crypto';
import {createGzip} from 'node:zlib';
import {pipeline} from 'node:stream/promises';
import {once} from 'node:events';

const args=process.argv.slice(2),option=(key,fallback)=>args.includes(key)?args[args.indexOf(key)+1]:fallback;
const from=Number(option('--from')),to=Number(option('--to')),out=path.resolve(option('--out',''));
const image=option('--image',''),helper=option('--sqlite-container','token-review-audit-db');
const seconds=Number(option('--budget-seconds','300')),pageRows=Number(option('--page-rows','128'));
assert.ok(args.includes('--out')&&image&&Number.isSafeInteger(from)&&Number.isSafeInteger(to)&&from>0&&to>from&&to-from<=4*86400);
assert.ok(Number.isInteger(seconds)&&seconds>=10&&seconds<=1800);
assert.ok(Number.isInteger(pageRows)&&pageRows>=8&&pageRows<=128);
fs.mkdirSync(out,{recursive:true});
const write=(name,value)=>fs.writeFileSync(path.join(out,name),JSON.stringify(value,null,2)+'\n');
const durableWrite=(name,value)=>{const target=path.join(out,name),temporary=target+'.partial',fd=fs.openSync(temporary,'w');try{fs.writeFileSync(fd,JSON.stringify(value,null,2)+'\n');fs.fsyncSync(fd)}finally{fs.closeSync(fd)}fs.renameSync(temporary,target)};
const docker=(params,extra={})=>execFileSync('docker',params,{encoding:'utf8',windowsHide:true,timeout:15000,maxBuffer:8*1024**2,stdio:['pipe','pipe','pipe'],...extra});
const origin='http://127.0.0.3:8204';
const login=await fetch(origin+'/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:process.env.MONITOR_LOCAL_USER||'local',password:process.env.MONITOR_LOCAL_PASSWORD||'local'}),signal:AbortSignal.timeout(10000)});
assert.equal(login.status,200);
const cookie=login.headers.getSetCookie().map(v=>v.split(';')[0]).join('; ');
async function api(endpoint,body){const r=await fetch(origin+endpoint,{method:body?'POST':'GET',headers:{Cookie:cookie,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(15000)});assert.equal(r.status,200,endpoint);return r.json()}
assert.equal((await api('/ready')).image_version,image);
const health=await api('/anomalies/token/health'),epoch=health.source_epoch;
assert.match(epoch,/^[A-Za-z0-9._:-]+$/);
write('health-before.json',health);
// Resume only a finalized, fsynced evidence segment. The per-page checkpoint
// remains diagnostic: a crash cannot promote buffered gzip output to proof.
const previous=args.includes('--resume')?JSON.parse(fs.readFileSync(path.join(out,'durable-checkpoint.json'),'utf8')):null;
if(previous){
 assert.equal(previous.source_epoch,epoch);assert.equal(previous.from,from);assert.equal(previous.to,to);assert.equal(previous.image,image);
 assert.equal(previous.complete,false,'this fixed window already finished');
 for(const segment of previous.segments){assert.equal(path.basename(segment.file),segment.file);const data=fs.readFileSync(path.join(out,segment.file));assert.equal(createHash('sha256').update(data).digest('hex'),segment.sha256,'evidence segment changed');}
}else assert.ok(!fs.existsSync(path.join(out,'durable-checkpoint.json')),'use --resume for an existing audit');
const runtime=JSON.parse(docker(['inspect','newapi-monitor-prod-readonly-monitor-1']))[0];
const networks=Object.keys(runtime.NetworkSettings.Networks);assert.equal(networks.length,1,'unambiguous runtime network required');
const networkArgs=['--network',networks[0],...(runtime.HostConfig.ExtraHosts||[]).flatMap(v=>['--add-host',v])];
const env=Object.fromEntries(runtime.Config.Env.map(v=>{const at=v.indexOf('=');return [v.slice(0,at),v.slice(at+1)]}));
const dsn=env.NEWAPI_LOG_DSN?.match(/^(nexus_ro|monitor_ro):(.+)@tcp\(([^)]+)\)\/([A-Za-z0-9_]+)(?:\?.*)?$/);
assert.ok(dsn,'configured read-only log DSN required');
const [host,port='3306']=dsn[3].split(':');
const keys=[];
for(const prefix of ['MONITOR_NGINX_EVIDENCE','MONITOR_NGINX_EVIDENCE_PREVIOUS','MONITOR_CLOUDWATCH_EVIDENCE']){
 const keyID=env[prefix+'_HMAC_KEY_ID'],secret=env[prefix+'_HMAC_KEY'];
 if(keyID&&secret&&!keys.some(v=>v.keyID===keyID))keys.push({keyID,secret});
}
const literal=v=>"'"+String(v).replaceAll("'","''")+"'";
function local(query){
 const raw=docker(['exec','-i','--user','1000:1000',helper,'timeout','8','sqlite3','-readonly','-json','/token-data/token.db'],{input:'.timeout 1000\n.explain off\nPRAGMA query_only=ON;\nBEGIN;\n'+query+';\nCOMMIT;\n',timeout:12000});
 return JSON.parse(raw.trim()||'[]');
}
// Keep one read-only connection warm across bounded pages. Re-authenticating
// through the tunnel per page would dominate the audit and its source budget.
const sourceContainer='token-review-source-'+randomUUID().slice(0,8);
const mysql=spawn('docker',['run','--rm','-i','--name',sourceContainer,'--pull=never','--read-only','--cap-drop=ALL',...networkArgs,'--env','MYSQL_PWD','--entrypoint','mysql','mysql:8.0','--connect-timeout=5','--default-character-set=utf8mb4','--batch','--raw','--unbuffered','--force','--skip-column-names','--host='+host,'--port='+port,'--user='+dsn[1],dsn[4]],{windowsHide:true,env:{...process.env,MYSQL_PWD:dsn[2]},stdio:['pipe','pipe','pipe']});
let active=null,received='';
mysql.stdout.setEncoding('utf8');mysql.stderr.setEncoding('utf8');
mysql.stdout.on('data',chunk=>{received+=chunk;for(let at;(at=received.indexOf('\n'))>=0;){const line=received.slice(0,at).replace(/\r$/,'');received=received.slice(at+1);if(!active)continue;if(line===active.marker){const q=active;active=null;clearTimeout(q.timer);q.error?q.reject(Error(q.error)):q.resolve(q.lines.join('\n'));}else{active.bytes+=Buffer.byteLength(line);if(active.bytes>4*1024**2)active.error='source_page_byte_budget_exhausted';else active.lines.push(line)}}});
mysql.stderr.on('data',chunk=>{if(active&&/ERROR\s+\d+/.test(chunk))active.error='readonly_source_mysql_'+chunk.match(/ERROR\s+(\d+)/)[1]});
mysql.on('error',()=>{if(active){const q=active;active=null;clearTimeout(q.timer);q.reject(Error('readonly_source_process_failed'))}});
mysql.on('close',()=>{if(active){const q=active;active=null;clearTimeout(q.timer);q.reject(Error(q.error||'readonly_source_connection_closed'))}});
async function source(query){assert.equal(active,null);return new Promise((resolve,reject)=>{
 const marker='audit_end_'+randomUUID().replaceAll('-','');
 const timer=setTimeout(()=>{active=null;reject(Error('readonly_source_transport_budget_exhausted'));},15000);
 active={marker,timer,resolve,reject,lines:[],bytes:0,error:''};
 mysql.stdin.write('SET SESSION MAX_EXECUTION_TIME=3000; START TRANSACTION READ ONLY; '+query+"; COMMIT; SELECT '"+marker+"';\n");
})}
const fields=['request_path','request_method','method','upstream_model_name','upstream_request_id','request_length','cache_tokens','cache_creation_tokens','cache_creation_tokens_5m','cache_creation_tokens_1h'];
const other="CASE WHEN JSON_VALID(other) AND LENGTH(other)<=65536 THEN JSON_OBJECT("+fields.map(k=>literal(k)+",JSON_EXTRACT(other,'$."+k+"')").join(',')+") ELSE NULL END";
let cursorAt=previous?.cursor_at??from,cursorID=previous?.cursor_id??0,pages=previous?.pages??0,rows=previous?.rows??0,missing=previous?.missing_facts??0,mismatched=previous?.mismatched_rows??0,duplicate=previous?.duplicate_identities??0,detected=previous?.detected??0,stale=previous?.stale??0,bytes=0,compressedBytes=0,completed=false,error='';
const states={...previous?.detection_states},mismatchReasons={...previous?.mismatch_reasons},started=Date.now(),ids=new Set();
const segments=[...(previous?.segments||[])],segmentFile=segments.length?'row-reconciliation-'+String(segments.length+1).padStart(4,'0')+'.jsonl.gz':'row-reconciliation.jsonl.gz';
const stream=createGzip({level:3});stream.on('data',chunk=>compressedBytes+=chunk.length);
const written=pipeline(stream,fs.createWriteStream(path.join(out,segmentFile+'.partial'),{flags:'wx'}));
written.catch(()=>{});
let segmentWritten=false,runPages=0;
try{
 while(Date.now()-started<seconds*1000&&bytes<2*1024**3&&compressedBytes<256*1024**2){
  const query="SELECT /*+ MAX_EXECUTION_TIME(3000) */ JSON_OBJECT('id',id,'created_at',created_at,'user_id',user_id,'channel_id',channel_id,'model',model_name,'user_group',COALESCE(`group`,''),'request_id',request_id,'prompt_tokens',CAST(prompt_tokens AS CHAR),'completion_tokens',CAST(completion_tokens AS CHAR),'quota',CAST(quota AS CHAR),'other',"+other+") FROM logs FORCE INDEX(idx_created_at_type) WHERE type=2 AND created_at>="+from+' AND created_at<'+to+' AND (created_at>'+cursorAt+' OR (created_at='+cursorAt+' AND id>'+cursorID+')) ORDER BY created_at,id LIMIT '+pageRows;
  if(!runPages)write('source-query-plan.json',{read_only:true,query,plan:await source('EXPLAIN '+query)});
  const batch=(await source(query)).trim().split('\n').filter(Boolean).map(v=>JSON.parse(v));
  if(!batch.length){completed=true;break}
  const batchIDs=batch.map(v=>v.id);assert.ok(batchIDs.every(Number.isSafeInteger));
  const sql="SELECT f.*,c.fact_revision AS detected_fact_revision,c.link_revision AS detected_link_revision,c.detection_state,c.evaluated_at,(SELECT json_group_array(json_object('key_id',r.key_id,'hash',r.hash,'version',r.version)) FROM token_metering_refs r WHERE r.source_epoch=f.source_epoch AND r.log_id=f.log_id) AS refs FROM token_metering_facts f LEFT JOIN token_detection_currents c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id WHERE f.source_epoch="+literal(epoch)+' AND f.log_id IN ('+batchIDs.join(',')+')';
  if(!runPages)write('local-query-plan.json',{read_only:true,query:sql,plan:local('EXPLAIN QUERY PLAN '+sql)});
  const facts=local(sql),map=new Map(facts.map(v=>[v.log_id,v]));duplicate+=facts.length-map.size;
  for(const row of batch){
   assert.ok(row.created_at>cursorAt||row.created_at===cursorAt&&row.id>cursorID,'source keyset not monotone');
   if(ids.has(row.id))duplicate++;ids.add(row.id);
   const f=map.get(row.id),reasons=[];let usage,refs=[];
   if(!f){missing++;reasons.push('fact_missing')}
   else{
    for(const k of ['created_at','user_id','channel_id','model','user_group','request_id'])if(String(f[k]??'')!==String(row[k]??''))reasons.push(k+'_mismatch');
    try{usage=JSON.parse(f.source_facts);refs=JSON.parse(f.refs)}catch{reasons.push('invalid_stored_json')}
    for(const k of ['prompt_tokens','completion_tokens','quota'])if(String(usage?.fields?.[k]?.value??'')!==String(row[k]??''))reasons.push(k+'_mismatch');
    const sourceOther=typeof row.other==='string'?JSON.parse(row.other):row.other;
    for(const [k,v] of Object.entries(sourceOther||{})){
     if(v===null)continue;
     const actual=k==='upstream_request_id'?f.upstream_request_id:usage?.fields?.[k==='method'?'request_method':k]?.value;
     if(String(actual??'')!==String(v))reasons.push(k+'_source_operand_mismatch');
    }
    if(row.request_id&&keys.length)for(const key of keys){const hash=createHmac('sha256',key.secret).update('oneapi-request-id\0'+row.request_id.trim()).digest('hex');if(!refs.some(r=>r.key_id===key.keyID&&r.hash===hash))reasons.push('request_hmac_ref_mismatch')}
    if(row.request_id&&!refs.length)reasons.push('request_refs_missing');
    const state=f.detection_state||'detection_pending';states[state]=(states[state]||0)+1;
    if(f.detection_state){detected++;if(f.detected_fact_revision!==f.revision||f.detected_link_revision!==f.link_revision)stale++}
   }
   if(reasons.length)mismatched++;
   for(const reason of reasons)mismatchReasons[reason]=(mismatchReasons[reason]||0)+1;
   const operands=Object.fromEntries(['prompt_tokens','completion_tokens','quota',...fields].map(k=>[k,usage?.fields?.[k]?.value??null]));
   const evidence={source:row,fact:f?{source_epoch:epoch,log_id:f.log_id,request_id:f.request_id,created_at:f.created_at,user_id:f.user_id,channel_id:f.channel_id,model:f.model,user_group:f.user_group,request_path:f.request_path,request_method:f.request_method,request_length:f.request_length,source_operands:operands,source_facts_sha256:createHash('sha256').update(f.source_facts).digest('hex'),missing_fields_sha256:createHash('sha256').update(f.missing_fields||'').digest('hex'),revision:f.revision,link_revision:f.link_revision,link_state:f.link_state,link_reason:f.link_reason,refs,detection_state:f.detection_state,detected_fact_revision:f.detected_fact_revision,detected_link_revision:f.detected_link_revision}:null,reasons};
   const line=JSON.stringify(evidence)+'\n';if(!stream.write(line))await once(stream,'drain');bytes+=Buffer.byteLength(line);rows++;cursorAt=row.created_at;cursorID=row.id;
  }
  pages++;runPages++;
  write('checkpoint.json',{from,to,source_epoch:epoch,cursor_at:cursorAt,cursor_id:cursorID,rows,pages,complete:false,observed_at:new Date().toISOString()});
  if(pages%10===0)console.log(JSON.stringify({pages,rows,missing,mismatched,duplicate,elapsed_seconds:(Date.now()-started)/1000}));
 }
}catch(e){error=e.message;}
finally{stream.end();try{await written;const temporary=path.join(out,segmentFile+'.partial'),fd=fs.openSync(temporary,'r+');try{fs.fsyncSync(fd)}finally{fs.closeSync(fd)}fs.renameSync(temporary,path.join(out,segmentFile));segments.push({file:segmentFile,sha256:createHash('sha256').update(fs.readFileSync(path.join(out,segmentFile))).digest('hex'),bytes:compressedBytes});segmentWritten=true}catch{error=error||'evidence_write_failed';completed=false}mysql.stdin.end('quit\n');try{docker(['stop','--time','2',sourceContainer],{timeout:5000})}catch{}mysql.kill();}
const result={observed_at:new Date().toISOString(),image,source:'production.logs',source_epoch:epoch,from,to,read_only:true,source_concurrency:1,source_query_budget_ms:3000,page_rows:pageRows,budget_seconds:seconds,elapsed_seconds:(Date.now()-started)/1000,complete:completed,error,rows,pages,cursor_at:cursorAt,cursor_id:cursorID,missing_facts:missing,mismatched_rows:mismatched,mismatch_reasons:mismatchReasons,duplicate_identities:duplicate,detected,stale,detection_states:states,ref_keys_verified:keys.map(v=>v.keyID),row_evidence_bytes:bytes,compressed_bytes:compressedBytes,pass:completed&&!error&&rows>0&&missing===0&&mismatched===0&&duplicate===0&&stale===0&&detected===rows};
result.resumed=!!previous;result.segments=segments;result.run_pages=runPages;result.elapsed_total_seconds=(previous?.elapsed_total_seconds||0)+result.elapsed_seconds;
if(segmentWritten)durableWrite('durable-checkpoint.json',result);
write('reconciliation.json',result);console.log(JSON.stringify(result));
process.exitCode=result.pass?0:1;
