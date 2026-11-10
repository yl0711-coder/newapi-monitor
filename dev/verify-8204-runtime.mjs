// Read-only local API state. No source queries, jobs or feature switch writes.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
const args=process.argv.slice(2),opt=k=>args[args.indexOf(k)+1];assert.ok(args.includes('--out')&&args.includes('--image'));
const out=path.resolve(opt('--out')),origin='http://127.0.0.3:8204';fs.mkdirSync(out,{recursive:true});
const login=await fetch(origin+'/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:'local',password:'local'}),signal:AbortSignal.timeout(10000)});assert.equal(login.status,200);
const cookie=login.headers.getSetCookie().map(s=>s.split(';')[0]).join('; '),results=[];
for(const endpoint of ['/ready','/usage/cache-stats','/admin/frt-sum/recompute','/anomalies/token/health','/anomalies/token/detection/health','/anomalies/token/notifications/health','/incidents/health']){
 const begin=Date.now(),r=await fetch(origin+endpoint,{headers:{Cookie:cookie},cache:'no-store',signal:AbortSignal.timeout(15000)});assert.equal(r.status,200,endpoint);const body=await r.json();
 if(endpoint==='/ready')assert.equal(body.image_version,opt('--image'));
 results.push({endpoint,http_status:r.status,elapsed_ms:Date.now()-begin,body});
}
fs.writeFileSync(path.join(out,'api-state.json'),JSON.stringify({at:new Date().toISOString(),read_only:true,source_queries:false,results},null,2)+'\n');
console.log(JSON.stringify(results.map(({endpoint,http_status,elapsed_ms,body})=>({endpoint,http_status,elapsed_ms,image:body.image_version,status:body.status,enabled:body.enabled,source_available:body.source_available,policy_configured:body.policy_configured,current_results:body.current_results,source_budget:body.source_budget}))));
