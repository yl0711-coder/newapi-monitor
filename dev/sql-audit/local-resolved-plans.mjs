import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';

const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..');
const dir=path.join(repo,'docs/sql-audit-20261126');
const container='monitor-sql-closure-20261127',start=Date.now(),records=[];
const report={warning:'Actual isolated MySQL EXPLAIN of current builders/captured calls and source-verified static metadata. 1500 synthetic logs/200 users. Business SELECT only for small metadata cases, never production data or latency claims.',records};
function command(args,input){const r=spawnSync('docker',args,{input,encoding:'utf8',timeout:5000,maxBuffer:1024*1024});if(r.error||r.status!==0)throw new Error(String(r.stderr||r.error||'docker failure').slice(0,1500));return r.stdout.trim();}
function sql(q){if(Date.now()-start>60000)throw new Error('whole diagnostic budget exceeded');return command(['exec','-i',container,'mysql','-uroot','--protocol=socket','--batch','--raw','--skip-column-names'],q);}
const text=s=>`CONVERT(0x${Buffer.from(s).toString('hex')} USING utf8mb4)`;
const value=v=>typeof v==='number'&&Number.isSafeInteger(v)?String(v):typeof v==='string'?text(v):(()=>{throw new Error('not a supported synthetic parameter')})();
function execute(q,args,explain){const bindings=args.map((v,i)=>`SET @p${i}=${value(v)};`).join('');const using=args.length?' USING '+args.map((_,i)=>`@p${i}`).join(','):'';return sql(`USE resolved_sql;${bindings}SET @q=${text((explain?'EXPLAIN FORMAT=JSON ':'')+q)};PREPARE inspected FROM @q;EXECUTE inspected${using};DEALLOCATE PREPARE inspected;`);}
try{
  const meta=command(['inspect','--format','{{index .Config.Labels "ai.task"}} {{.HostConfig.NetworkMode}}',container]);if(meta!=='monitor-sql-closure-20261127 none')throw new Error('refusing non-owned/network-enabled container');
  sql(`CREATE DATABASE resolved_sql;USE resolved_sql;CREATE TABLE users(id BIGINT PRIMARY KEY,created_at BIGINT,quota BIGINT,used_quota BIGINT);
CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT,created_at BIGINT,type INT,quota BIGINT,token_id BIGINT,token_name VARCHAR(64),content TEXT,other TEXT,INDEX idx_user_created_type(user_id,created_at,type),INDEX idx_created_at_type(created_at,type));`);
  sql('USE resolved_sql;INSERT INTO users VALUES'+Array.from({length:200},(_,n)=>`(${201+n},1788192000,10,20)`).join(',')+';INSERT INTO logs VALUES'+Array.from({length:1500},(_,n)=>`(${n+1},${201+n%200},${1790784000+Math.floor(n/10)},${n%7+1},${n%5},${n%3+1},'audit','synthetic',${text('{"billing_source":"wallet"}')})`).join(',')+';ANALYZE TABLE users;ANALYZE TABLE logs;');
  const {Cases}=JSON.parse(fs.readFileSync(path.join(dir,'monthlysource-catalog-resolved-20261127.json'),'utf8'));
  const old=JSON.parse(fs.readFileSync(path.join(dir,'internal-static-plans-20261127.json'),'utf8'));
  const metadata=old.entries.filter(x=>x.status==='requires_manual_resolution'&&/FROM (users|logs)\b/.test(x.source_sql));
  for(const c of Cases){
    const r={owner:c.Owner,variant:c.Variant,sql:c.SQL,parameters:c.Args.length,status:'not_explained'};records.push(r);
    r.plan=JSON.parse(execute(c.SQL,c.Args,true));r.status='explained_isolated_mysql';
    if(c.Variant.startsWith('actual-call/')){const result=execute(c.SQL,c.Args,false);r.rows=result?result.split('\n').length:0;if(r.rows!==c.Args.length)throw new Error('metadata result scope changed');r.result_sha256=createHash('sha256').update(result).digest('hex');}
  }
  if(metadata.length!==6)throw new Error('metadata gap list changed');
  report.source_sha256={};
  for(const c of metadata){
    const source=fs.readFileSync(path.join(repo,c.file),'utf8');if(!source.includes(c.source_sql))throw new Error('old metadata template no longer matches source');
    report.source_sha256[c.file]=createHash('sha256').update(source).digest('hex');
    const args=Array.from({length:c.source_sql.split('?').length-1},()=>201);
    const r={owner:c.file,variant:'source-verified-static-metadata',sql:c.source_sql,parameters:args.length,status:'not_explained'};records.push(r);
    r.plan=JSON.parse(execute(c.source_sql,args,true));const result=execute(c.source_sql,args,false);r.rows=result?result.split('\n').length:0;
    if(r.rows!==1)throw new Error('metadata point lookup lost row');
    r.result_sha256=createHash('sha256').update(result).digest('hex');r.status='explained_and_metadata_executed';
  }
  report.stopped=false;
}catch(e){report.stopped=true;report.error=e.message;process.exitCode=1;}
finally{report.elapsed_ms=Date.now()-start;fs.writeFileSync(path.join(dir,'resolved-mysql-plans-20261127.json'),JSON.stringify(report,null,2)+'\n',{mode:0o600});console.log(JSON.stringify({cases:records.length,stopped:report.stopped,error:report.error,elapsed_ms:report.elapsed_ms}));}
