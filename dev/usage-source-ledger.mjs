// Independent production ledger: closed CST windows, existing time index,
// raw log types grouped in SQL. No product pagination or local facts are used.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';

export function reconcileUsageSource({from,to,ids,cells,out,scratch}) {
 assert.ok(Number.isSafeInteger(from)&&Number.isSafeInteger(to)&&to>from&&to-from<=30*86400);
 assert.equal((from+28800)%86400,0);assert.equal((to+28800)%86400,0);
 assert.ok(to<=Math.floor((Date.now()/1000+28800)/86400)*86400-28800,'closed CST days only');
 ids=[...ids].sort((a,b)=>a-b);
 assert.ok(ids.length>0&&ids.length<=50&&new Set(ids).size===ids.length&&ids.every(n=>Number.isSafeInteger(n)&&n>0));
 fs.mkdirSync(out,{recursive:true});fs.mkdirSync(scratch,{recursive:true});
 assert.ok(!fs.existsSync(path.join(out,'source-ledger.json')),'never overwrite source evidence');
 const write=(name,data)=>fs.writeFileSync(path.join(out,name),JSON.stringify(data,null,2)+'\n');
 let env={};for(const file of ['.env','D:/monitorcode/suidao/NexusAPI-readonly-tunnel-windows-x64/config/program-docker.env'])if(fs.existsSync(file))for(const line of fs.readFileSync(file,'utf8').split(/\r?\n/)){const m=line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);if(m)env[m[1]]=m[2].replace(/^['"]|['"]$/g,'')}
 const dsn=env.NEWAPI_LOG_DSN?.match(/^(nexus_ro|monitor_ro):(.+)@tcp\(([^)]+)\)\/([^?]+)(?:\?.*)?$/);assert.ok(dsn,'configured read-only source required');assert.match(dsn[4],/^[A-Za-z0-9_]+$/);const[host,port]=dsn[3].split(':');
 const docker='C:/Users/86177/AppData/Local/Programs/DockerDesktop/resources/bin/docker.exe';
 function sql(text,timeout=18000){try{return execFileSync(docker,['run','--rm','-i','--pull','never','--read-only','--cap-drop','ALL','--cpus=1','--memory=256m','--add-host','host.docker.internal:host-gateway','--env','MYSQL_PWD','--entrypoint','mysql','mysql:8.0','--connect-timeout=5','--default-character-set=utf8mb4','--batch','--raw','--skip-column-names','--host='+host,'--port='+(port||3306),'--user='+dsn[1],dsn[4]],{input:text,encoding:'utf8',windowsHide:true,timeout,maxBuffer:8<<20,env:{...process.env,MYSQL_PWD:dsn[2],TEMP:scratch,TMP:scratch},stdio:['pipe','pipe','pipe']}).trim()}catch(e){throw Error(/3024|interrupted|timeout/i.test(e.stderr||'')?'independent source statement budget exhausted; stopped':e.code==='ETIMEDOUT'?'independent source overall budget exhausted; stopped':'independent source connection/query failed; details suppressed')}}
 const span=3*3600;
 const query=start=>`SELECT JSON_OBJECT('date','${new Date((start+28800)*1000).toISOString().slice(0,10)}','user_id',user_id,'type',type,'rows',COUNT(*),'quota',CAST(COALESCE(SUM(quota),0) AS SIGNED)) FROM logs FORCE INDEX (idx_created_at_type) WHERE created_at>=${start} AND created_at<${Math.min(start+span,to)} AND type IN (2,6) AND user_id IN (${ids.join(',')}) AND NOT (type=2 AND COALESCE(token_name,'')='模型测试' AND COALESCE(content,'')='模型测试') GROUP BY user_id,type ORDER BY user_id,type`;
 const proof={at:new Date().toISOString(),from_ts:from,to_ts:to,member_ids:ids,read_only:true,source_concurrency:1,statement_budget_ms:3500,window_seconds:span,consistency:'one read-only repeatable-read consistent snapshot',status:'running'};
 try {
  const plan=JSON.parse(sql('SET SESSION MAX_EXECUTION_TIME=3500; EXPLAIN FORMAT=JSON '+query(to-span)+';'));
  assert.match(JSON.stringify(plan),/"access_type":"range"/);assert.match(JSON.stringify(plan),/"key":"idx_created_at_type"/);write('source-plan.json',plan);
  const statements=['SET SESSION MAX_EXECUTION_TIME=3500','SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ','START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY'];
  for(let start=from;start<to;start+=span){statements.push(query(start));statements.push(`SELECT JSON_OBJECT('completed_start',${start})`)}statements.push('COMMIT');
  const text=statements.join(';\n')+';\n';fs.writeFileSync(path.join(out,'source-ledger.sql'),text);
  const started=Date.now(),raw=sql(text,180000);proof.elapsed_including_helper_ms=Date.now()-started;
  const rows=raw.split('\n').filter(Boolean).map(s=>JSON.parse(s)),done=rows.filter(r=>r.completed_start).map(r=>r.completed_start),source=rows.filter(r=>!r.completed_start);
  assert.deepEqual(done,Array.from({length:(to-from)/span},(_,i)=>from+i*span),'every bounded interval must be read');
  const indexed=new Map(),keys=['requests','refund_records','consume_quota','refund_quota'];
  for(const row of source){
   for(const field of ['user_id','type','rows','quota'])assert.ok(Number.isSafeInteger(row[field]),field);
   assert.ok(ids.includes(row.user_id)&&[2,6].includes(row.type)&&row.rows>0);
   const key=row.date+'/'+row.user_id,c=indexed.get(key)||{date:row.date,user_id:row.user_id,requests:0,refund_records:0,consume_quota:0,refund_quota:0};
   if(row.type===2){c.requests+=row.rows;c.consume_quota+=row.quota}else{c.refund_records+=row.rows;c.refund_quota+=row.quota}indexed.set(key,c);
  }
  const sort=data=>data.map(c=>({date:c.date,user_id:c.user_id,...Object.fromEntries(keys.map(k=>[k,c[k]]))})).sort((a,b)=>a.date.localeCompare(b.date)||a.user_id-b.user_id);
  proof.completed_intervals=done;proof.type_groups=source;proof.cells=sort([...indexed.values()]);
  proof.totals=Object.fromEntries(keys.map(k=>[k,proof.cells.reduce((n,c)=>n+c[k],0)]));
  for(const n of Object.values(proof.totals))assert.ok(Number.isSafeInteger(n));
  write('source-ledger.json',proof);
  assert.deepEqual(sort(cells),proof.cells,'product cells differ from independent production SQL');
  proof.status='passed';return {cells:proof.cells,totals:proof.totals,intervals:done.length,elapsed_ms:proof.elapsed_including_helper_ms};
 }catch(e){proof.status='failed';proof.failure=e.message;throw e}
 finally{proof.finished_at=new Date().toISOString();write('source-ledger.json',proof)}
}
