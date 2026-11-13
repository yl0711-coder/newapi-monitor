// Bounded read-only diagnosis of the actual unfinished Usage day. No feature
// switch, schema/index change or source write; at most one measured query.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';import {execFileSync} from 'node:child_process';
const root=process.cwd(),base=path.join(root,'.local-test-kit/incident-review-20261023/v128');
const out=path.join(base,'usage-diagnosis');fs.mkdirSync(out,{recursive:true});
const scratch=path.join(root,'.tmp-incident-review-20261023/usage-diagnosis');fs.mkdirSync(scratch,{recursive:true});
const prior=JSON.parse(fs.readFileSync(path.join(base,'usage/result.json'))),last=prior.attempts.at(-1);
assert.equal(last.available,false);assert.ok(last.available_days>0&&last.available_days<30);
const from=Date.parse(last.requested_from+'T00:00:00+08:00')/1000+last.available_days*86400,to=from+86400,day=(from+28800)/86400;
assert.ok(Number.isSafeInteger(day));
const snapshot=new DatabaseSync(path.join(base,'recovery/monitor-before.db'),{readOnly:true});
const ids=snapshot.prepare('SELECT user_id FROM tracked_users ORDER BY user_id').all().map(r=>r.user_id);snapshot.close();
assert.ok(ids.length>0&&ids.length<=50&&ids.every(x=>Number.isSafeInteger(x)&&x>0));
const excluded="((type=2 AND COALESCE(token_name,'')='模型测试' AND COALESCE(content,'')='模型测试') OR (type=5 AND user_id=1 AND COALESCE(token_id,0)=0 AND COALESCE(token_name,'')='' AND COALESCE(request_id,'')=''))";
const query=(constant,sqlite=false)=>`SELECT user_id,${constant?day:`(created_at+28800) ${sqlite?'/':'DIV'} 86400`} AS day_idx,SUM(CASE WHEN type=2 THEN 1 ELSE 0 END) AS requests,SUM(CASE WHEN type=6 THEN 1 ELSE 0 END) AS refunds,SUM(CASE WHEN type=2 THEN COALESCE(quota,0) ELSE 0 END) AS consume_quota,SUM(CASE WHEN type=6 THEN COALESCE(quota,0) ELSE 0 END) AS refund_quota FROM logs ${sqlite?'':'FORCE INDEX (idx_user_created_type)'} WHERE type IN (2,6) AND created_at>=${from} AND created_at<${to} AND user_id IN (${ids.join(',')}) AND NOT (${excluded}) GROUP BY user_id${constant?'':',day_idx'}`;
const local=new DatabaseSync(':memory:');local.exec('CREATE TABLE logs(id INTEGER PRIMARY KEY,user_id INTEGER,created_at INTEGER,type INTEGER,quota INTEGER,token_name TEXT,content TEXT,token_id INTEGER,request_id TEXT); CREATE INDEX idx_user_created_type ON logs(user_id,created_at,type);');
const insert=local.prepare('INSERT INTO logs(user_id,created_at,type,quota,token_name,content) VALUES(?,?,?,?,?,?)');
for(const uid of ids)for(const ts of [from-1,from,from+1,to-1,to])for(const type of [2,5,6]){insert.run(uid,ts,type,123,'','');insert.run(uid,ts,type,456,'模型测试','模型测试')}
const sorted=rows=>rows.sort((a,b)=>a.user_id-b.user_id);assert.deepEqual(sorted(local.prepare(query(false,true)).all()),sorted(local.prepare(query(true,true)).all()));
const localPlans=[false,true].map(constant=>({constant,plan:local.prepare('EXPLAIN QUERY PLAN '+query(constant,true)).all()}));local.close();
let env={};for(const name of ['.env','D:/monitorcode/suidao/NexusAPI-readonly-tunnel-windows-x64/config/program-docker.env'])if(fs.existsSync(name))for(const line of fs.readFileSync(name,'utf8').split(/\r?\n/)){const m=line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);if(m)env[m[1]]=m[2].replace(/^['"]|['"]$/g,'')}
const dsn=env.NEWAPI_LOG_DSN?.match(/^(nexus_ro|monitor_ro):(.+)@tcp\(([^)]+)\)\/([^?]+)(?:\?.*)?$/);assert.ok(dsn);assert.match(dsn[4],/^[A-Za-z0-9_]+$/);const [host,port]=dsn[3].split(':');
const docker='C:/Users/86177/AppData/Local/Programs/DockerDesktop/resources/bin/docker.exe';
function mysql(sql){try{return execFileSync(docker,['run','--rm','--pull','never','--read-only','--cap-drop','ALL','--add-host','host.docker.internal:host-gateway','--env','MYSQL_PWD','--entrypoint','mysql','mysql:8.0','--connect-timeout=5','--default-character-set=utf8mb4','--batch','--raw','--skip-column-names','--host='+host,'--port='+(port||3306),'--user='+dsn[1],dsn[4],'-e','SET SESSION MAX_EXECUTION_TIME=3500; START TRANSACTION READ ONLY; '+sql+'; COMMIT;'],{encoding:'utf8',windowsHide:true,timeout:15000,maxBuffer:2<<20,env:{...process.env,MYSQL_PWD:dsn[2],TEMP:scratch,TMP:scratch},stdio:['ignore','pipe','pipe']}).trim()}catch(e){throw Error(/3024|interrupted|timeout/i.test(e.stderr||'')?'query budget exhausted; stopped':'read-only plan/query failed; connection details suppressed')}}
const proof={at:new Date().toISOString(),scope:'local equivalence and read-only production plans; optional one bounded measurement',from_ts:from,to_ts:to,member_count:ids.length,local_equivalence:true,local_plans:localPlans,source_plans:[]};
const save=()=>fs.writeFileSync(path.join(out,'result.json'),JSON.stringify(proof,null,2)+'\n');
try{
 for(const constant of [false,true]){const sql=query(constant),plan=JSON.parse(mysql('EXPLAIN FORMAT=JSON '+sql));assert.match(JSON.stringify(plan),/"access_type":"range"/);proof.source_plans.push({constant,sql,plan});save()}
 if(process.argv.includes('--measure')){const raw=mysql('SET @diagnostic_start=NOW(6); '+query(true)+"; SELECT 'elapsed_us',TIMESTAMPDIFF(MICROSECOND,@diagnostic_start,NOW(6))");const lines=raw.split('\n'),timing=lines.pop().split('\t');assert.equal(timing[0],'elapsed_us');proof.measurement={variant:'constant day, group only by user_id',max_execution_ms:3500,elapsed_sql_ms:Number(timing[1])/1000,rows:lines.map(l=>l.split('\t').map(Number))}}
}catch(e){proof.failure=e.message;process.exitCode=1}finally{proof.finished_at=new Date().toISOString();save();console.log(JSON.stringify(proof))}
