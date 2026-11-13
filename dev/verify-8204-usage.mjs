// Bounded final-image Usage reconciliation. No concurrent production requests.
// SQL is independently aggregated from logs; facts/API cells are not its input.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {reconcileUsageSource} from './usage-source-ledger.mjs';
const args=process.argv.slice(2),opt=k=>args[args.indexOf(k)+1];assert.ok(args.includes('--out')&&args.includes('--scratch'));
const out=path.resolve(opt('--out')),scratch=path.resolve(opt('--scratch')),origin='http://127.0.0.3:8204',expected=args.includes('--image')?opt('--image'):'v128';
fs.mkdirSync(out,{recursive:true});fs.mkdirSync(scratch,{recursive:true});if(process.platform==='win32')assert.match(scratch,/^D:/i);
const write=(file,value)=>fs.writeFileSync(path.join(out,file+'.json'),JSON.stringify(value,null,2)+'\n');
const proof={at:new Date().toISOString(),expected_image:expected,read_only:true,source_concurrency:1,attempts:[],checks:[]};let cookie='',failure;
async function get(p,timeout=26000){const start=Date.now(),r=await fetch(origin+p,{headers:{Cookie:cookie},cache:'no-store',signal:AbortSignal.timeout(timeout)});assert.equal(r.status,200,'HTTP '+r.status+' '+p);return {body:await r.json(),elapsed_ms:Date.now()-start}}
function projectFollowup(r){return {...r,companies:(r.companies||[]).map(c=>({group_id:c.group_id,spend_30d_usd:c.spend_30d_usd,urgency:c.urgency,members:(c.members||[]).map(m=>({user_id:m.user_id,spend_30d_usd:m.spend_30d_usd,days_idle:m.days_idle,last_active:m.last_active,urgency:m.urgency,level:m.level}))}))}}
function comparableFollowup(r){
 const p=projectFollowup(r),members=new Set();
 assert.equal(new Set(p.companies.map(c=>c.group_id)).size,p.companies.length);
 const descending=list=>{for(let i=1;i<list.length;i++)assert.ok(list[i-1].urgency>=list[i].urgency,'existing urgency sort contract')};
 descending(p.companies);
 for(const company of p.companies){descending(company.members);for(const member of company.members){assert.ok(!members.has(member.user_id),'duplicate member');members.add(member.user_id)}company.members.sort((a,b)=>b.urgency-a.urgency||a.user_id-b.user_id)}
 // The existing API orders only by urgency. Compare every node/value while
 // allowing unspecified ties; never discard or merely compare company totals.
 p.companies.sort((a,b)=>b.urgency-a.urgency||a.group_id-b.group_id);return p;
}
function projectMatrix(r){return {...r,matrix:{...r.matrix,users:r.matrix?.users?.map(u=>({user_id:u.user_id,group_id:u.group_id,consume_quota:u.consume_quota,refund_quota:u.refund_quota,net_quota:u.net_quota}))}}}
const pause=ms=>new Promise(r=>setTimeout(r,ms));
try{
 const login=await fetch(origin+'/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:'local',password:'local'}),signal:AbortSignal.timeout(10000)});assert.equal(login.status,200);cookie=login.headers.getSetCookie().map(s=>s.split(';')[0]).join('; ');
 const ready=(await get('/ready')).body;assert.equal(ready.image_version,expected);proof.ready_before={image:ready.image_version,status:ready.status};
 proof.cache_before=(await get('/usage/cache-stats')).body;let final,previousSlices=-1,window;
 for(let i=0;i<8;i++){
  const stats=(await get('/usage/cache-stats')).body;assert.equal(stats.source_budget.in_use,0,'wait for existing query use to finish before independent acceptance');
  const {body,elapsed_ms}=await get('/usage/followups');assert.equal(body.enabled,true);assert.equal(body.requested_days,30);
  const current={from:body.requested_from,to:body.requested_to};if(window)assert.deepEqual(current,window,'CST day rolled over during acceptance');else window=current;
  proof.attempts.push({attempt:i+1,elapsed_ms,...projectFollowup(body)});write('result-in-progress',proof);
  if(body.available){assert.equal(body.available_days,30);assert.equal(body.range_partial,false);final=body;break}
  assert.equal(body.companies.length,0,'partial source must not publish follow-up decisions');assert.ok(body.message);
  assert.equal(body.resume_available,true,'source failure or non-resumable result; stop querying');
  assert.ok(Number.isSafeInteger(body.completed_slices)&&body.completed_slices>previousSlices,'source page progress stalled; stop querying');previousSlices=body.completed_slices;
  await pause(2000);
 }
 assert.ok(final,'did not obtain a complete 30-day follow-up decision within eight bounded attempts');
 proof.checks.push({name:'complete closed 30-day followups',status:'pass',attempts:proof.attempts.length,window});
 const qs=new URLSearchParams(window),{body:matrix,elapsed_ms}=await get('/usage/matrix?'+qs);assert.equal(matrix.matrix_available,true);assert.equal(matrix.range_partial,false);assert.equal(matrix.membership_partial,false);assert.equal(matrix.data_stale,false);
 assert.equal(matrix.matrix.from,window.from);assert.equal(matrix.matrix.to,window.to);assert.equal(matrix.matrix.days.length,30);write('matrix',projectMatrix(matrix));proof.matrix_elapsed_ms=elapsed_ms;
 const byUser=new Map(matrix.matrix.users.map(u=>[u.user_id,u]));let members=0;
 for(const company of final.companies){let sum=0;for(const member of company.members||[]){const user=byUser.get(member.user_id);assert.ok(user);assert.equal(user.group_id,company.group_id);assert.ok(Math.abs(member.spend_30d_usd-user.consume_quota/500000)<1e-6);sum+=member.spend_30d_usd;members++}assert.ok(Math.abs(sum-company.spend_30d_usd)<1e-6)}assert.equal(members,final.member_total);
 const second=(await get('/usage/followups')).body;assert.equal(second.available,true);assert.deepEqual(comparableFollowup(second),comparableFollowup(final));
 proof.order_contract='Existing descending urgency order checked on both full responses; equal-urgency company/member ties have no secondary ordering contract in HEAD and are normalized only for value comparison.';
 const cacheAfter=(await get('/usage/cache-stats')).body;assert.equal(cacheAfter.source_budget.in_use,0);proof.cache_after_api=cacheAfter;
 proof.checks.push({name:'shared matrix, members/company sums, repeated complete read',status:'pass',member_total:members,cells:matrix.matrix.cells.length});
 if(!args.includes('--source-sql')){proof.independent_source='not tested: pass --source-sql to execute bounded read-only production SQL';write('result',proof);process.exit(0)}
 const ids=[...byUser.keys()].sort((a,b)=>a-b);assert.ok(ids.length>0&&ids.length<=50&&ids.every(x=>Number.isSafeInteger(x)&&x>0));
 const from=Date.parse(window.from+'T00:00:00+08:00')/1000,to=Date.parse(window.to+'T00:00:00+08:00')/1000+86400;assert.equal(to-from,30*86400);
 const independent=reconcileUsageSource({from,to,ids,cells:matrix.matrix.cells,out,scratch});
 for(const user of matrix.matrix.users){const rows=independent.cells.filter(c=>c.user_id===user.user_id);assert.equal(rows.reduce((n,c)=>n+c.consume_quota,0),user.consume_quota);assert.equal(rows.reduce((n,c)=>n+c.refund_quota,0),user.refund_quota);assert.equal(user.net_quota,user.consume_quota-user.refund_quota)}
 proof.checks.push({name:'independent production logs SQL equals full matrix cells and user totals',status:'pass',days:30,cells:independent.cells.length,totals:independent.totals});proof.independent_source='passed';
}catch(e){failure=e;proof.failure=e.message;console.error(e.message)}finally{proof.finished_at=new Date().toISOString();write('result',proof)}
if(failure)process.exitCode=1;
