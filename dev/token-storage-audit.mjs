// Read-only, bounded local SQLite capacity inspection. It never checkpoints,
// deletes, vacuums or copies the database. Index sizes are explicit lower bounds
// when the page/time budget cannot measure the complete index.
import {DatabaseSync} from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const args=process.argv.slice(2);
const option=(name,fallback)=>args.includes(name)?args[args.indexOf(name)+1]:fallback;
const file=path.resolve(option('--db','.local-test-kit/prod-readonly/token-data/token.db'));
// Each exact index measurement runs in a separate killable process. Parent
// timeout bounds even a blocked/native SQLite call without touching a writer.
if(args.includes('--index-name')){
 const db=new DatabaseSync(file,{readOnly:true,timeout:100});
 try{db.exec('PRAGMA query_only=ON');console.log(JSON.stringify(db.prepare('SELECT pgsize AS bytes FROM dbstat WHERE name=? AND aggregate=TRUE').get(option('--index-name'))))}finally{db.close()}
 process.exit(0);
}
const out=path.resolve(option('--out','.local-test-kit/token-review-evidence/storage-audit.json'));
const cap=Math.min(1024,Math.max(1,Number(option('--page-limit','256'))));
const exact=args.includes('--exact-indices');
const budget=Math.min(exact?30000:5000,Math.max(100,Number(option('--budget-ms','1500'))));
if(!Number.isFinite(cap)||!Number.isFinite(budget)||!fs.existsSync(file))throw Error('Invalid bounded audit arguments');
const report={observed_at:new Date().toISOString(),database:file,read_only:true,page_limit_per_index:cap,budget_ms:budget,files:[],indices:[],index_bytes_exact:null};
for(const entry of fs.readdirSync(path.dirname(file),{withFileTypes:true})){
 if(!entry.isFile())continue;
 if(!/token.*(?:\.db|\.bak|backup)|sqlite|wal|shm/i.test(entry.name))continue;
 const p=path.join(path.dirname(file),entry.name),s=fs.statSync(p);
 report.files.push({name:entry.name,bytes:s.size,modified_at:s.mtime.toISOString()});
}
const db=new DatabaseSync(file,{readOnly:true,timeout:100});
try{
 db.exec('PRAGMA query_only=ON');
 report.pragmas={};
 for(const p of ['page_count','page_size','freelist_count'])report.pragmas[p]=db.prepare('PRAGMA '+p).get()[p];
 report.database_logical_bytes=report.pragmas.page_count*report.pragmas.page_size;
 report.freelist_bytes=report.pragmas.freelist_count*report.pragmas.page_size;
 const indices=db.prepare("SELECT name,tbl_name AS table_name,rootpage FROM sqlite_schema WHERE type='index' ORDER BY name").all();
 const started=Date.now();
 for(const index of indices){
  const row={...index,measured_bytes:0,complete:false};
  if(Date.now()-started>=budget){row.reason='time_budget_exhausted';report.indices.push(row);continue}
  try{
   const pages=db.prepare('SELECT pgsize FROM dbstat WHERE name=? LIMIT ?').all(index.name,cap+1);
   row.measured_bytes=pages.slice(0,cap).reduce((n,p)=>n+p.pgsize,0);
   row.complete=pages.length<=cap;
   row.reason=row.complete?'measured':'page_budget_exhausted';
   if(exact&&!row.complete&&Date.now()-started<budget){
    const result=spawnSync(process.execPath,['--disable-warning=ExperimentalWarning',fileURLToPath(import.meta.url),'--db',file,'--index-name',index.name],{timeout:Math.max(1,Math.min(1500,budget-(Date.now()-started))),encoding:'utf8',windowsHide:true});
    if(result.status===0){const measured=JSON.parse(result.stdout);if(Number.isSafeInteger(measured?.bytes)){row.measured_bytes=measured.bytes;row.complete=true;row.reason='measured'}}
    else row.reason='exact_measurement_timed_out_or_failed';
   }
  }catch(error){row.reason='read_failed';row.error=error.message}
  report.indices.push(row);
 }
 report.index_bytes_measured_lower_bound=report.indices.reduce((n,p)=>n+p.measured_bytes,0);
 if(report.indices.every(p=>p.complete))report.index_bytes_exact=report.index_bytes_measured_lower_bound;
 report.elapsed_ms=Date.now()-started;
}finally{db.close();fs.mkdirSync(path.dirname(out),{recursive:true});fs.writeFileSync(out,JSON.stringify(report,null,2)+'\n')}
console.log(JSON.stringify({files:report.files,pragmas:report.pragmas,indices:report.indices.length,complete_indices:report.indices.filter(i=>i.complete).length,index_bytes_exact:report.index_bytes_exact,index_bytes_measured_lower_bound:report.index_bytes_measured_lower_bound,elapsed_ms:report.elapsed_ms}));
