// Real final-image shared/UI checks; exact existing-request tasks are opt-in.
// No feature switches, response replacement or production test records.
import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';
import {browserSession} from './browser-session.mjs';
const args=process.argv.slice(2),opt=(k,d)=>args.includes(k)?args[args.indexOf(k)+1]:d;
const out=path.resolve(opt('--out')),scratch=path.resolve(opt('--scratch')),mode=opt('--mode','extras'),origin='http://127.0.0.3:8204';
assert.ok(['extras','shared','tasks'].includes(mode));fs.mkdirSync(out,{recursive:true});
const b=await browserSession({browser:opt('--browser'),scratch,origin}),{call,evaluate:ev,click,waitFor,pause}=b;
const checks=[],sha=s=>createHash('sha256').update(typeof s==='string'?s:JSON.stringify(s)).digest('hex');let failure,activeRow='',activeTask='';
const probe=String.raw`(()=>{const original=window.fetch.bind(window),p=window.__sharedReview={events:[],list:null,tasks:{},history:[]};window.fetch=async function(input,options={}){const u=new URL(typeof input==='string'?input:input.url,location.href),e={path:u.pathname.replace(/(\/investigations\/)[^/]+/,'$1<task>'),method:options.method||'GET',at:Date.now()};p.events.push(e);options.signal?.addEventListener('abort',()=>e.aborted=true,{once:true});try{const r=await original(input,options);e.status=r.status;e.received=Date.now();if(u.pathname==='/logchain/requests'||u.pathname.startsWith('/logchain/investigations')){const data=await r.clone().json();if(u.pathname==='/logchain/requests')p.list=data;else{p.tasks[u.pathname]=data;p.history.push({id:data.investigation_id,status:data.status,at:Date.now()})}}return r}catch(err){e.error=err.name;throw err}}})()`;
const count=()=>ev("window.__sharedReview.events.filter(e=>e.path==='/logchain/requests').length");
const controls=()=>ev("Object.fromEntries(['lcDate','lcToDate','lcFromTime','lcToTime','lcUserID','lcStream'].map(id=>[id,document.getElementById(id).value]))");
const rowIDs=()=>ev("[...document.querySelectorAll('#lcTableBody tr[data-lc-id]')].map(e=>e.dataset.lcId)");
async function lcDone(){await waitFor("/已更新/.test(lcStatus.innerText)&&!!window.__sharedReview.list",45000)}
async function next(n){await waitFor(`window.__sharedReview.events.filter(e=>e.path==='/logchain/requests'&&e.status===200).length>${n}`,50000);await lcDone()}
async function activate(){const other=await call('Target.createTarget',{url:'about:blank'},null);await call('Target.activateTarget',{targetId:other.targetId},null);await pause(300);await call('Target.activateTarget',{targetId:b.targetId},null);await call('Page.bringToFront');await call('Target.closeTarget',{targetId:other.targetId},null)}
async function nav(tab,context={}){await ev(`window.monitorNavigate(${JSON.stringify(tab)},${JSON.stringify(context)})`)}
async function shot(name){await ev("(()=>{let s=document.getElementById('shared-review-redaction');if(!s){s=document.createElement('style');s.id='shared-review-redaction';s.textContent='#lcTableBody,#alTableBody,#chRows,#usageMatrix,#usageSummary,#usageFollowUps,#cmBody{filter:blur(10px)}';document.head.append(s)}})()");await b.screenshot(path.join(out,name+'.png'))}
function save(){fs.writeFileSync(path.join(out,'result.json'),JSON.stringify({at:new Date().toISOString(),mode,origin,expected_image:opt('--image','v128'),browser:b.version,scope:'unchanged final-image assets and actual APIs; source capabilities/budgets unchanged',checks,failure:failure?.message||null},null,2)+'\n')}
async function check(name,fn){try{checks.push({name,status:'pass',evidence:await fn()});console.log('PASS '+name);save()}catch(e){checks.push({name,status:'fail',error:e.message});throw e}}
async function cancel(){if(!activeRow||!activeTask)return;const selector=`[data-lc-cw-cancel="${activeRow}"]`;if(await ev(`!!document.querySelector(${JSON.stringify(selector)})`)){await click(selector);await waitFor(`window.__sharedReview.tasks[${JSON.stringify('/logchain/investigations/'+activeTask+'/cancel')}]?.status==='cancelled'`,15000)}}
try{
 await call('Page.addScriptToEvaluateOnNewDocument',{source:probe});await call('Page.navigate',{url:origin+'/login'});await pause(500);
 assert.equal(await ev("fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:'local',password:'local'})}).then(r=>r.status)"),200);
 await call('Page.navigate',{url:origin+'/?#tab=anomalies'});await waitFor("typeof window.monitorNavigate==='function'",15000);
 assert.equal(await ev("fetch('/ready').then(r=>r.json()).then(r=>r.image_version)"),opt('--image','v128'));
 if(mode==='extras'){
  const proof=JSON.parse(fs.readFileSync('.local-test-kit/incident-review-20261023/v128/data/production-request-projection.json'));
  const window=JSON.parse(fs.readFileSync('.local-test-kit/incident-review-20261023/v128/data/window.json'));
  assert.ok(proof.rows.length,'existing error row required for real prefill/row validation');
  const date=ts=>new Date(ts*1000).toLocaleDateString('sv-SE',{timeZone:'Asia/Shanghai'}),time=ts=>new Date(ts*1000).toLocaleTimeString('sv-SE',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit'});
  const range={date:date(window.from_ts),to_date:date(window.to_ts-1),from_time:time(window.from_ts),to_time:time(window.to_ts-60),scope:'error'};
  await check('Token manual-query boundary across focus, visibility, two timer cycles and return',async()=>{
   await ev(`window.logChainOpen(${JSON.stringify({...range,preset:'token_local_review',user_id:String(proof.rows[0].user_id)})})`);await waitFor('/仅预填/.test(lcStatus.innerText)');assert.equal(await count(),0);const selected=await controls();
   await activate();await ev("window.dispatchEvent(new Event('focus'));document.dispatchEvent(new Event('visibilitychange'))");await pause(33000);await nav('anomalies');await pause(300);await nav('logchain');await pause(33000);assert.equal(await count(),0);assert.deepEqual(await controls(),selected);
   return {production_queries_before_submit:0,waited_ms:66000,entry:'actual public token_local_review navigation; real detector remains disabled'};
  });
  await check('Manual submit enables normal automatic refresh, preserving date/filter',async()=>{const selected=await controls();await click('#lcApply');await next(0);const n=await count();await next(n);assert.deepEqual(await controls(),selected);return {queries:await count(),date:range.date,hour_range:[range.from_time,range.to_time]}});
  await check('Real pagination and expanded evidence remain across timer cycles',async()=>{
   // The selected day is bounded by the existing UI query budget. It provides
   // enough genuine rows for pagination without fabricating result content.
   await ev(`lcUserID.value='';lcDate.value=${JSON.stringify(range.date)};lcToDate.value=${JSON.stringify(range.date)};lcFromTime.value='00:00';lcToTime.value='23:59'`);await click('#lcApply');await lcDone();await waitFor('!lcMore.hidden&&!lcMore.disabled',30000);
   const first=await rowIDs();await click('#lcMore');await waitFor(`document.querySelectorAll('#lcTableBody tr[data-lc-id]').length>${first.length}`,45000);const all=await rowIDs(),selected=await controls(),n=await count();assert.equal(new Set(all).size,all.length);
   await activate();await pause(65000);assert.equal(await count(),n);assert.deepEqual(await rowIDs(),all);assert.deepEqual(await controls(),selected);assert.match(await ev('lcStatus.innerText'),/分页|快照/);
   await click('#lcApply');await lcDone();await click('#lcTableBody tr[data-lc-id]');await pause(300);const html=sha(await ev('lcTableBody.innerHTML')),opened=await count();await activate();await pause(65000);assert.equal(await count(),opened);assert.equal(sha(await ev('lcTableBody.innerHTML')),html);
   await click('#lcTableBody tr[data-lc-id]');await ev("window.dispatchEvent(new Event('focus'))");await next(opened);return {first_page:first.length,paged_rows:all.length,page_wait_ms:65000,detail_wait_ms:65000,queries_while_protected:0,refresh_resumed:true};
  });
  await check('Five modules: actual dark/light layouts at 320/375',async()=>{const evidence=[];for(const theme of ['dark','light'])for(const width of [320,375]){
   await ev(`document.body.setAttribute('theme-mode',${JSON.stringify(theme)})`);await call('Emulation.setDeviceMetricsOverride',{width,height:950,deviceScaleFactor:1,mobile:false});
   for(const [tab,context] of [['model-statistics',{view:'requests'}],['model-statistics',{view:'frt'}],['alerts',{}],['logchain',{}],['customer-health',{}]]){await nav(tab,context);await pause(650);const actual=await ev('({width:innerWidth,scroll:document.documentElement.scrollWidth})');assert.ok(actual.scroll<=width+2);await ev('window.scrollTo(0,0)');await shot(tab+(context.view||'')+'-'+theme+'-'+width);evidence.push({tab,view:context.view,theme,...actual})}
  }return evidence});
 }else if(mode==='shared'){
  for(const tab of ['usage','stability','channels'].filter(tab=>!args.includes('--tabs')||opt('--tabs').split(',').includes(tab)))await check(tab+' actual read-only UI and HTTP responses',async()=>{
   const before=await ev('window.__sharedReview.events.length');await nav(tab);const selector=tab==='usage'?'#usageSummary':tab==='stability'?'#stDeliveryBody':'#cmBody';await waitFor(`!!document.querySelector(${JSON.stringify(selector)})?.textContent.trim()`,45000);
   if(tab==='usage')await waitFor('usageMxCache?._matrixAvailable===true&&followAvailable===true',230000);await pause(3500);
   const data=await ev(`(()=>{const e=document.querySelector(${JSON.stringify(selector)});return {visible:!document.getElementById(${JSON.stringify('tab-'+tab)}).hidden,rendered_children:e.children.length,rendered_characters:e.textContent.trim().length,loading:!!e.querySelector('.stability-loading,.cm-loading'),error:${JSON.stringify(tab)}==='usage'?(usageErr.querySelector('.alert')?.textContent.trim()||''):'',usage_complete:${JSON.stringify(tab)}==='usage'?{matrix:usageMxCache?._matrixAvailable,followups:followAvailable,range:usageRangeParams().toString(),source_checked_at:usageMxCache?.source_checked_at}:undefined,requests:window.__sharedReview.events.slice(${before}).filter(r=>r.path.startsWith(${JSON.stringify('/'+tab+'/')}))}})()`);
   assert.equal(data.visible,true);assert.equal(data.loading,false);assert.equal(data.error,'');assert.ok(data.requests.some(e=>e.status===200));assert.ok(data.requests.every(e=>e.method==='GET'&&(e.status==null||e.status<400)));return data;
  });
  await check('Incident/Token unauthenticated access remains denied',async()=>{const statuses=[];for(const endpoint of ['/incidents','/incidents/detail?id=1','/incidents/health','/anomalies/token/health']){const r=await fetch(origin+endpoint,{redirect:'manual',signal:AbortSignal.timeout(8000)});assert.ok([401,403,302].includes(r.status));statuses.push({endpoint,status:r.status})}return statuses});
  checks.push({name:'Real customer-portal authenticated session',status:'not_tested',reason:'No actual portal credentials; admin access and local permission tests are not substituted for a customer login.'});
 }else{
  // Choose an existing recent request only; never manufacture production traffic.
  const now=Math.floor(Date.now()/60000)*60,start=now-15*60,date=ts=>new Date(ts*1000).toLocaleDateString('sv-SE',{timeZone:'Asia/Shanghai'}),time=ts=>new Date(ts*1000).toLocaleTimeString('sv-SE',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit'});
  const range={date:date(start),to_date:date(now-1),from_time:time(start),to_time:time(now-60),scope:'error'};
  async function open(index){await ev(`delete window.__sharedReview.tasks['/logchain/investigations'];window.logChainOpen(${JSON.stringify(range)})`);await lcDone();assert.equal(await ev('window.__sharedReview.list.cloudwatch_enabled'),true,'existing task capability must already be enabled');activeRow=await ev(`String(window.__sharedReview.list.rows.filter(r=>r.request_id)[${index}]?.id||'')`);assert.ok(activeRow,'no existing recent exact request');await click(`[data-lc-id="${activeRow}"]`);await waitFor(`!!document.querySelector('[data-lc-cw="${activeRow}"]')`);assert.equal(await ev(`document.querySelector('[data-lc-cw-sensitive="${activeRow}"]')?.checked`),false);await click(`[data-lc-cw="${activeRow}"]`);await waitFor("!!window.__sharedReview.tasks['/logchain/investigations']?.investigation_id",15000);activeTask=await ev("window.__sharedReview.tasks['/logchain/investigations'].investigation_id")}
  const state=()=>ev(`(()=>{const r=window.__sharedReview.tasks[${JSON.stringify('/logchain/investigations/'+activeTask)}];return r?{status:r.status,evidence_count:r.evidence?.length,timeline_count:r.timeline?.length,cost:r.cost,audit_recorded:r.audit_recorded,sensitive_diagnostics_read:r.sensitive_diagnostics_read,sources:(r.source_status||[]).map(s=>({source:s.source,status:s.status,partial:s.partial,truncated:s.truncated,parse_failed:s.parse_failed}))}:null})()`);
  await check('Exact real task state, filter protection, evidence preservation and delivery recheck',async()=>{
   await open(0);const selected=await controls(),n=await count();let blocked=false;
   if(await ev(`!!document.querySelector('[data-lc-cw-cancel="${activeRow}"]')`)){await ev("lcModel.value='acceptance-filter-must-not-submit'");await click('#lcApply');assert.deepEqual(await controls(),selected);assert.equal(await ev('lcModel.value'),'');assert.equal(await count(),n);blocked=true}
   await waitFor(`['complete','partial','failed','pending_delivery'].includes(window.__sharedReview.tasks[${JSON.stringify('/logchain/investigations/'+activeTask)}]?.status)`,65000);const first=await state();await activate();
   // Pending tasks are rechecked by the existing server at five minutes, not
   // a shortened acceptance schedule. Wait through that real first recheck.
   const duration=first.status==='pending_delivery'?335000:65000;await pause(duration);assert.equal(await count(),n);const last=await state();assert.equal(last.sensitive_diagnostics_read,false);assert.ok(await ev(`!!document.querySelector('[data-lc-cw-box="${activeRow}"]')`));
   const history=await ev(`window.__sharedReview.history.filter(e=>e.id===${JSON.stringify(activeTask)}).map(e=>({status:e.status,at:e.at}))`);const transitioned=history.some(e=>e.status==='pending_delivery')&&history.some(e=>e.status==='complete');checks.push({name:'Actual pending_delivery to complete',status:transitioned?'pass':'not_tested',reason:transitioned?undefined:'Actual observed states did not establish this transition; no status was substituted.',history});
   await shot('real-task');if(last.status==='pending_delivery')await cancel();return {task_sha256:sha(activeTask),first,last,history,waited_ms:duration,filter_change_blocked:blocked,list_queries_while_protected:0};
  });
  await check('Second exact real task cancellation and filter unlock',async()=>{await open(1);const task=activeTask;await cancel();const r=await ev(`window.__sharedReview.tasks[${JSON.stringify('/logchain/investigations/'+task+'/cancel')}]`);assert.equal(r?.status,'cancelled');assert.equal(r.sensitive_diagnostics_read,false);await pause(500);assert.equal(await ev(`!!document.querySelector('[data-lc-cw-cancel="${activeRow}"]')`),false);const n=await count();await click('#lcApply');await next(n);return {task_sha256:sha(task),status:r.status,filters_unlocked:true}});
 }
 assert.equal(b.exceptions.length,0);assert.deepEqual(b.external,[]);
}catch(e){failure=e;console.error(e.message)}finally{try{await cancel();fs.writeFileSync(path.join(out,'requests.json'),JSON.stringify(await ev('window.__sharedReview?.events||[]'),null,2)+'\n')}catch{}save();await b.close()}
if(failure)process.exitCode=1;
