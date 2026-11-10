// Actual 8204 UI/API verification; no asset or API response replacement.
// --baseline reads the existing day query only; otherwise verifies diagnostics.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {browserSession} from './browser-session.mjs';
const args=process.argv.slice(2),opt=(k,d)=>args.includes(k)?args[args.indexOf(k)+1]:d;
const out=path.resolve(opt('--out')),origin='http://127.0.0.3:8204',baseline=args.includes('--baseline'),layoutOnly=args.includes('--layout-only'),image=opt('--image');
fs.mkdirSync(out,{recursive:true});
const hash=x=>createHash('sha256').update(typeof x==='string'||Buffer.isBuffer(x)?x:JSON.stringify(x)).digest('hex');
const b=await browserSession({browser:opt('--browser'),scratch:opt('--scratch'),origin});
const {call,evaluate:ev,waitFor,click,input,pause}=b;
const checks=[];let failure,ownedTask;
const probe=String.raw`(()=>{
 const original=window.fetch.bind(window),p=window.__diagnosticReview={events:[],list:null,tasks:{}};
 window.fetch=async function(input,options={}){
  const u=new URL(typeof input==='string'?input:input.url,location.href);
  if(!u.pathname.startsWith('/logchain/')&&!['/customer-health/report','/alerts/rejections','/model-statistics/report'].includes(u.pathname))return original(input,options);
  const e={path:u.pathname.replace(/(\/investigations\/)[^/]+/,'$1<task>'),method:options.method||'GET',at:Date.now(),query:Object.fromEntries(u.searchParams)};p.events.push(e);
  try{const r=await original(input,options);e.status=r.status;const d=await r.clone().json();e.elapsed_ms=Date.now()-e.at;
   if(u.pathname==='/logchain/requests'){e.rows=d.rows?.length;e.scope=d.scope;e.error=d.error||null;p.list=d}
   else if(u.pathname.startsWith('/logchain/investigations'))p.tasks[u.pathname]=d;
   e.done=true;return r;
  }catch(err){e.error=err.name;e.done=true;throw err}
 };
})()`;
function save(){fs.writeFileSync(path.join(out,'result.json'),JSON.stringify({at:new Date().toISOString(),image,baseline,origin,browser:b.version,checks,failure:failure?.message||null},null,2)+'\n')}
async function check(name,fn){try{checks.push({name,status:'pass',evidence:await fn()});console.log('PASS '+name);save()}catch(e){checks.push({name,status:'fail',error:e.message});throw e}}
const count=()=>ev("window.__diagnosticReview.events.filter(e=>e.path==='/logchain/requests'&&e.done).length");
async function next(n){await waitFor(`window.__diagnosticReview.events.filter(e=>e.path==='/logchain/requests'&&e.done).length>${n}`,60000);await pause(200);const r=await ev('window.__diagnosticReview.list');assert.ok(Array.isArray(r.rows),r.error||'list not received');return r}
const controls=()=>ev("Object.fromEntries(['lcDate','lcToDate','lcFromTime','lcToTime','lcUserID','lcUsername','lcRequestID','lcKeyword'].map(id=>[id,document.getElementById(id)?.value||'']))");
async function screenshot(name){await ev("(()=>{let s=document.getElementById('diagnostics-review-redaction');if(!s){s=document.createElement('style');s.id='diagnostics-review-redaction';s.textContent='#lcTableBody tr[data-lc-id] td:nth-child(-n+4),.lc-raw,.lc-review-private,#lcUserID,#lcUsername,#lcRequestID{filter:blur(10px)}';document.head.append(s)}for(const kv of document.querySelectorAll('.lc-detail .lc-kv')){const label=kv.firstElementChild?.textContent||'';kv.classList.toggle('lc-review-private',!['错误原因','责任方','错误产生层','处理建议','建议执行方','判定依据'].includes(label)&&!label.startsWith('待补：'))}})()");await b.screenshot(path.join(out,name+'.png'))}
async function verifyNarrowDetail(){
 await call('Emulation.setEmulatedMedia',{features:[{name:'prefers-color-scheme',value:'dark'}]});await call('Emulation.setDeviceMetricsOverride',{width:430,height:920,deviceScaleFactor:1,mobile:false});
 await call('Browser.grantPermissions',{origin,permissions:['clipboardReadWrite','clipboardSanitizedWrite']},null);
 const expected=await ev("document.querySelector('.lc-detail [data-lc-copy]')?.dataset.lcCopy");assert.ok(expected);await click('.lc-detail [data-lc-copy]');await pause(200);assert.equal(await ev('navigator.clipboard.readText()'),expected);
 const visible=await ev("(()=>{const e=[...document.querySelectorAll('.lc-detail .lc-kv')].find(e=>e.firstElementChild?.textContent.trim()==='错误原因');if(!e)return null;e.scrollIntoView({block:'center',inline:'center'});const r=e.getBoundingClientRect();return {left:r.left,right:r.right,top:r.top,bottom:r.bottom,width:innerWidth,height:innerHeight}})()");
 assert.ok(visible&&visible.left>=0&&visible.right<=visible.width&&visible.top>=0&&visible.bottom<=visible.height,'specific cause must be viewable using the existing horizontal table scroll');
 await screenshot('narrow-dark');assert.equal(await ev('lcDetailTh.textContent'),'错误原因');return {width:430,theme:'dark',copy_operable:true,error_cause_visible:visible,horizontal_table_scroll:'existing layout'};
}
try{
 await call('Page.addScriptToEvaluateOnNewDocument',{source:probe});
 assert.equal(await b.authenticate(),200);
 await call('Page.navigate',{url:origin+'/?#tab=logchain'});await waitFor("typeof window.monitorNavigate==='function'",15000);
 await check('actual runtime identity and embedded asset',async()=>{
  const ready=await ev("fetch('/ready',{cache:'no-store'}).then(r=>r.json())");assert.equal(ready.image_version,image);
  const asset=await ev("fetch('/logchain.js',{cache:'no-store'}).then(r=>r.text())"),digest=hash(asset);
  if(!baseline)assert.equal(digest,hash(fs.readFileSync('monitor/logchain.js')));
  return {image:ready.image_version,ready:ready.status,logchain_js_sha256:digest};
 });
 let initial;
 await check('default today query is the full CST day',async()=>{
  await waitFor("window.__diagnosticReview.events.some(e=>e.path==='/logchain/requests'&&e.done)",60000);await pause(250);
  const events=await ev("window.__diagnosticReview.events.filter(e=>e.path==='/logchain/requests'&&e.done)"),first=events[0];initial=await ev('window.__diagnosticReview.list');
  assert.equal(first.status,200,first.error||'day query failed');assert.ok(initial.rows.length>0,'no day rows');
  const selected=await controls();assert.equal(selected.lcFromTime,'00:00');assert.equal(selected.lcToTime,'23:59');assert.equal(selected.lcDate,selected.lcToDate);
  assert.equal(first.query.from_time,'00:00');assert.equal(first.query.to_time,'23:59');
  return {controls:selected,request:first.query,scope:initial.scope,rows:initial.rows.length,has_more:initial.has_more,elapsed_ms:first.elapsed_ms,coverage:initial.coverage_status};
 });
 if(layoutOnly){
  const r=initial.rows.find(r=>r.diagnosis);assert.ok(r);await click(`[data-lc-id="${r.id}"]`);await waitFor("!!document.querySelector('.lc-detail')");
  await check('dark narrow detail copy and horizontally scrolled error cause are visible',verifyNarrowDetail);
 }
 if(!baseline&&!layoutOnly){
  await check('row diagnoses and original detail remain visible',async()=>{
   assert.equal(await ev('lcDetailTh.textContent'),'错误原因');const r=initial.rows.find(r=>r.diagnosis&&r.type===5&&r.content);assert.ok(r,'day has no error diagnosis with original content');
   await click(`[data-lc-id="${r.id}"]`);await waitFor("!!document.querySelector('.lc-detail')");
   const text=await ev('lcTableBody.innerText');for(const s of [r.diagnosis.reason,r.diagnosis.responsibility,'处理建议','判定依据','待补'])assert.ok(text.includes(s),s);
   assert.ok(await ev("!!document.querySelector('.lc-raw')"));await screenshot('day-detail');await click(`[data-lc-id="${r.id}"]`);
   return {log_id:r.id,request_sha256:hash(r.request_id),rule:r.diagnosis.rule_id,reason:r.diagnosis.reason,responsibility:r.diagnosis.responsibility,missing:r.diagnosis.missing,rows_with_diagnosis:initial.rows.filter(x=>x.diagnosis).length};
  });
  if(initial.has_more)await check('day pagination has no duplicate source log',async()=>{
   const n=await count();await click('#lcMore');const later=await next(n),ids=await ev("[...document.querySelectorAll('#lcTableBody tr[data-lc-id]')].map(e=>e.dataset.lcId)");assert.equal(new Set(ids).size,ids.length);assert.ok(ids.length>initial.rows.length);return {first:initial.rows.length,second:later.rows.length,rendered:ids.length};
  });
  const sample=initial.rows.find(r=>r.request_id&&r.user_id>0&&r.type===5&&r.diagnosis?.cause_known);assert.ok(sample,'need a diagnosed error with request and customer');
  await check('customer filter retains day window',async()=>{
   await input('#lcUserID',String(sample.user_id));const n=await count();await click('#lcApply');const list=await next(n);assert.ok(list.rows.length);assert.ok(list.rows.every(r=>r.user_id===sample.user_id));return {rows:list.rows.length,scope:list.scope,customer_sha256:hash(String(sample.user_id))};
  });
  await check('exact Request ID and refresh preserve selection and expansion',async()=>{
   await input('#lcRequestID',sample.request_id);const n=await count();await click('#lcApply');const list=await next(n);assert.ok(list.rows.length);assert.ok(list.rows.every(r=>r.request_id===sample.request_id));const r=list.rows[0],selected=await controls();
   const beforeRefresh=await count();await ev("window.dispatchEvent(new Event('focus'))");const refreshed=await next(beforeRefresh);assert.ok(refreshed.rows.every(x=>x.request_id===sample.request_id));assert.deepEqual(await controls(),selected);
   await click(`[data-lc-id="${r.id}"]`);const whileExpanded=await count();
   await ev("window.dispatchEvent(new Event('focus'));document.dispatchEvent(new Event('visibilitychange'))");await pause(1200);assert.deepEqual(await controls(),selected);assert.ok(await ev(`document.querySelector('[data-lc-id="${r.id}"]')?.nextElementSibling?.classList.contains('lc-detail')`));
   assert.equal(await count(),whileExpanded,'expanded detail should pause background refresh');
   return {rows:list.rows.length,request_sha256:hash(sample.request_id),log_id:r.id,actual_focus_refresh:true,filters_preserved:true,detail_preserved:true,refresh_while_expanded:'paused by existing policy'};
  });
  await check('copy retains original error text',async()=>{
   const expected=await ev("document.querySelector('.lc-detail [data-lc-copy]')?.dataset.lcCopy");assert.ok(expected);
   await call('Browser.grantPermissions',{origin,permissions:['clipboardReadWrite','clipboardSanitizedWrite']},null);
   await click('.lc-detail [data-lc-copy]');await pause(200);
   assert.equal(await ev('navigator.clipboard.readText()'),expected);return {original_sha256:hash(expected),length:expected.length};
  });
  await check('original keyword query and scope switches keep error cause header',async()=>{
   const keyword=/status_code/i.test(sample.content)?'status_code':sample.content.match(/[A-Za-z_][A-Za-z_-]{3,}/)?.[0];assert.ok(keyword);
   // Raw error search belongs to type=5; the existing API intentionally rejects
   // combining it with consumption anomaly scopes whose error content is empty.
   let n=await count();await click('[data-lc-scope="error"]');await next(n);
   await input('#lcKeyword',keyword);n=await count();await click('#lcApply');let list=await next(n);assert.ok(list.rows.length);assert.ok(list.rows.every(r=>r.type===5&&r.content.toLowerCase().includes(keyword.toLowerCase())));
   await input('#lcKeyword','');n=await count();await click('#lcApply');await next(n);
   assert.equal(await ev('lcDetailTh.textContent'),'错误原因');const scopes=[{scope:'error',rows:list.rows.length}];
   for(const scope of ['stream','billing','err_anom']){n=await count();await click(`[data-lc-scope="${scope}"]`);list=await next(n);assert.equal(await ev('lcDetailTh.textContent'),'错误原因');scopes.push({scope,rows:list.rows.length})}
   await input('#lcKeyword','');n=await count();await click('#lcApply');list=await next(n);assert.ok(list.rows.length);await click(`[data-lc-id="${list.rows[0].id}"]`);
   return {keyword,scopes};
  });
  await check('existing bounded full-chain task preserves specific conclusion',async()=>{
   const r=(await ev('window.__diagnosticReview.list.rows'))[0];assert.ok(await ev(`!!document.querySelector('[data-lc-cw="${r.id}"]')`),'full-chain not enabled');await click(`[data-lc-cw="${r.id}"]`);
   await waitFor("!!window.__diagnosticReview.tasks['/logchain/investigations']?.investigation_id",15000);
   const id=await ev("window.__diagnosticReview.tasks['/logchain/investigations'].investigation_id"),key='/logchain/investigations/'+id;ownedTask=id;
   await waitFor(`['complete','partial','failed','pending_delivery'].includes(window.__diagnosticReview.tasks[${JSON.stringify(key)}]?.status)`,60000);
   const result=await ev(`window.__diagnosticReview.tasks[${JSON.stringify(key)}]`),d=(result.requests||[]).find(x=>x.id===r.id)?.diagnosis||result.request_diagnoses?.find(x=>x.ref===r.diagnostic_ref)?.diagnosis;
   if(r.diagnosis?.cause_known)assert.ok(d?.cause_known,'known cause lost on supplemental query');
   const evidence={status:result.status,queries:result.cost?.queries,summary:result.summary,sources:(result.source_status||[]).map(s=>({source:s.source,status:s.status,partial:s.partial,truncated:s.truncated})),diagnosis:d};
   if(result.status==='pending_delivery'){await click(`[data-lc-cw-cancel="${r.id}"]`);await pause(700)}
   ownedTask=null;
   assert.ok(await ev('lcTableBody.children.length>0'));return evidence;
  });
  await screenshot('request-detail');
  await check('dark and narrow detail remains operable',verifyNarrowDetail);
 }
 if(args.includes('--shared')){
  for(const [tab,endpoint,selector] of [['customer-health','/customer-health/report','[data-ch-row]'],['alerts','/alerts/rejections','#alTableBody tr'],['model-statistics','/model-statistics/report','[data-ms-expand]']]){
   await check(tab+' actual shared UI and HTTP response',async()=>{
    const n=await ev(`window.__diagnosticReview.events.filter(e=>e.path===${JSON.stringify(endpoint)}&&e.done).length`);
    await ev(`window.monitorNavigate(${JSON.stringify(tab)},{})`);
    await waitFor(`window.__diagnosticReview.events.filter(e=>e.path===${JSON.stringify(endpoint)}&&e.done).length>${n}`,60000);
    const event=await ev(`window.__diagnosticReview.events.filter(e=>e.path===${JSON.stringify(endpoint)}&&e.done).at(-1)`);
    assert.equal(event.status,200,event.error||endpoint);await waitFor(`!!document.querySelector(${JSON.stringify('#tab-'+tab+' '+selector)})`,15000);
    return {endpoint,http_status:event.status,elapsed_ms:event.elapsed_ms,visible_rows:await ev(`document.querySelectorAll(${JSON.stringify('#tab-'+tab+' '+selector)}).length`)};
   });
  }
 }
 assert.equal(b.exceptions.length,0,'browser exceptions');assert.deepEqual(b.external,[]);
}catch(e){failure=e;console.error(e.message)}finally{
 if(ownedTask)try{await ev(`fetch('/logchain/investigations/'+${JSON.stringify(ownedTask)}+'/cancel', {method:'POST'}).then(r=>r.status)`)}catch{}
 try{const events=await ev('window.__diagnosticReview?.events||[]');for(const e of events){for(const k of ['request_id','username','token_name','user_id'])if(e.query?.[k])e.query[k+'_sha256']=hash(e.query[k]),delete e.query[k]}fs.writeFileSync(path.join(out,'requests.json'),JSON.stringify(events,null,2)+'\n')}catch{}
 save();await b.close();
}
if(failure)process.exitCode=1;
