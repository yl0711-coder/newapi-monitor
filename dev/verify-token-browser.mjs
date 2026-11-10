// Actual 8204 assets and actual responses; no mocked records or response edits.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {gunzipSync} from 'node:zlib';
import {browserSession} from './browser-session.mjs';
const args=process.argv.slice(2),opt=k=>args[args.indexOf(k)+1];
for(const key of ['--browser','--scratch','--out','--image','--from','--to'])assert.ok(args.includes(key));
const out=path.resolve(opt('--out')),origin='http://127.0.0.3:8204',from=Number(opt('--from')),to=Number(opt('--to'));
assert.ok(Number.isSafeInteger(from)&&Number.isSafeInteger(to)&&to>from);
fs.mkdirSync(out,{recursive:true});
const b=await browserSession({browser:opt('--browser'),scratch:opt('--scratch'),origin}),{call,evaluate:ev,click,waitFor}=b;
const checks=[];let failure;
const probe="(()=>{const native=window.fetch.bind(window);window.__tokenReview={reports:[],details:[],events:[],logchainQueries:0};window.fetch=async(...args)=>{const r=await native(...args),u=new URL(typeof args[0]==='string'?args[0]:args[0].url,location.href);if(u.pathname==='/logchain/requests')window.__tokenReview.logchainQueries++;if(u.pathname.startsWith('/anomalies/token/')){const e={path:u.pathname,status:r.status,at:Date.now()};window.__tokenReview.events.push(e);if(u.pathname.endsWith('/query'))window.__tokenReview.reports.push(await r.clone().json());if(u.pathname.endsWith('/detail'))window.__tokenReview.details.push(await r.clone().json())}return r}})()";
const time=v=>new Date((v+8*3600)*1000).toISOString().slice(0,16);
try{
 await call('Page.addScriptToEvaluateOnNewDocument',{source:probe});
 assert.equal(await b.authenticate(),200);
 await call('Page.navigate',{url:origin+'/#tab=anomalies'});
 await waitFor("location.pathname==='/'&&document.readyState==='complete'&&typeof window.monitorNavigate==='function'&&!!document.getElementById('taFrom')",20000);
 await ev("window.monitorNavigate('anomalies',{view:'token'})");
 await waitFor("!document.getElementById('tab-anomalies').hidden&&!!taFrom.getBoundingClientRect().width",15000);
 const ready=await ev("fetch('/ready').then(r=>r.json())");assert.equal(ready.image_version,opt('--image'));
 // A fixed audit must not be replaced by the live relative-window refresh.
 if(await ev("!!document.getElementById('taTimeMode')")){
  await click('#taTimeMode');await b.key('End');await b.key('Enter');
  assert.equal(await ev('taTimeMode.value'),'0');
 }
 await b.input('#taFrom',time(from));await b.input('#taTo',time(to));
 await click('.ta-more-filters>summary');await click('#taStatusFilter');await b.key('End');await b.key('Enter');
 assert.equal(await ev('taStatusFilter.value'),'');
 const count=await ev('window.__tokenReview.reports.length');await click('#taFilters button[type=submit]');
 const reportSelector=`window.__tokenReview.reports.slice(${count}).findLast(r=>r.range?.from===${from}&&r.range?.to===${to})`;
 await waitFor(`!!(${reportSelector})`,30000);
 const report=await ev(reportSelector);fs.writeFileSync(path.join(out,'actual-report.json'),JSON.stringify(report,null,2)+'\n');
 assert.equal(report.range.from,from);assert.equal(report.range.to,to);assert.equal(report.available,true,'actual report is unavailable');
 assert.ok(report.items.length>0,'actual window has no visible facts');
 // The fetch probe sees a cloned response before the application receives it.
 // load() also waits for health, so response arrival is not DOM completion.
 // Use the real UI lifecycle without changing responses or fabricating rows.
 await waitFor("taList.getAttribute('aria-busy')==='false'",30000);
 const pages=[report],items=[...report.items];let page=report;
 while(page.has_more){
  assert.ok(pages.length<12,'fixed browser audit page budget');
  const last=page.items.at(-1)?.request;assert.ok(last,'nonempty cursor page required');
  const before=await ev('window.__tokenReview.reports.length');await click('#taNext');
  const next=`window.__tokenReview.reports.slice(${before}).findLast(r=>r.range?.from===${from}&&r.range?.to===${to}&&r.range?.after_id===${last.log_id}&&r.range?.after_at===${last.created_at})`;
  await waitFor(`!!(${next})`,30000);page=await ev(next);assert.equal(page.available,true);
  await waitFor("taList.getAttribute('aria-busy')==='false'",30000);
  const expectedRows=items.length+page.items.length;
  if(await ev("taList.querySelectorAll('[data-ta-detail]').length")<expectedRows){
   // Preserve the actual refresh/next-page race as a limitation. One explicit
   // retry verifies recoverability without replacing a response or app asset.
   checks.push({name:'automatic refresh interrupted next-page publication',status:'limited',expected_rows:expectedRows,retry:'one user click'});
   await click('#taNext');
   await waitFor(`taList.getAttribute('aria-busy')==='false'&&taList.querySelectorAll('[data-ta-detail]').length===${expectedRows}`,30000);
  }
  pages.push(page);items.push(...page.items);
 }
 fs.writeFileSync(path.join(out,'actual-pages.json'),JSON.stringify(pages,null,2)+'\n');
 assert.equal(new Set(items.map(r=>r.request.source_epoch+':'+r.request.log_id)).size,items.length,'duplicate browser identities');
 if(args.includes('--source')){
  const dir=path.resolve(opt('--source')),proof=JSON.parse(fs.readFileSync(path.join(dir,'reconciliation.json'),'utf8'));
  assert.equal(proof.complete,true);assert.equal(proof.from,from);assert.equal(proof.to,to);assert.ok(proof.rows<=600);
  const source=[];for(const segment of proof.segments){assert.equal(path.basename(segment.file),segment.file);for(const line of gunzipSync(fs.readFileSync(path.join(dir,segment.file)),{maxOutputLength:16*1024**2}).toString().trim().split('\n'))if(line)source.push(JSON.parse(line).source);}
  assert.deepEqual(items.map(r=>r.request.log_id).sort((a,b)=>a-b),source.map(r=>r.id).sort((a,b)=>a-b),'all fixed source identities must reach the actual browser');
  for(const row of items){const raw=source.find(s=>s.id===row.request.log_id);assert.equal(row.display.request_id,raw.request_id);assert.equal(row.display.recorded_input_tokens,raw.prompt_tokens);assert.equal(row.display.output_tokens,raw.completion_tokens);assert.equal(row.display.recorded_quota,raw.quota);}
  checks.push({name:'all source identities and usage match actual paged browser',status:'pass',rows:items.length,pages:pages.length});
 }
 const list=await ev('taList.innerText');
 for(const row of items){
  assert.ok(row.display.request_id,'row has no raw Request ID: '+row.request.log_id);
  assert.ok(list.includes(row.display.request_id),'Request ID absent from rendered row: '+row.request.log_id);
  assert.ok(Array.isArray(row.display.missing_fields),'missing-field ledger absent');
  assert.ok(row.display.detection_state,'detection stage absent');
 }
 checks.push({name:'actual list raw Request IDs, missing fields and stages',status:'pass',rows:items.length,pages:pages.length});
 await b.screenshot(path.join(out,'list-1280.png'));
 for(let i=0;i<Math.min(3,report.items.length);i++){
  await click('[data-ta-detail="'+i+'"]');
  await waitFor("taDetail.open&&!/正在读取/.test(taDetailBody.innerText)",15000);
  const body=await ev('taDetailBody.innerText');
  assert.ok(body.includes(report.items[i].display.request_id),'detail raw Request ID absent');
  assert.ok(/缺失字段|Missing fields/.test(body),'detail missing-field ledger absent');assert.ok(/来源|字段来源|Usage field source/.test(body),'detail field provenance absent');
  assert.doesNotMatch(body,/\.map is not a function|Cannot read properties/);
  fs.writeFileSync(path.join(out,'detail-'+i+'.txt'),body+'\n');
  await b.screenshot(path.join(out,'detail-'+i+'-1280.png'));
  if(i===0){await call('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:false});await b.screenshot(path.join(out,'detail-390.png'));await call('Emulation.setDeviceMetricsOverride',{width:1280,height:950,deviceScaleFactor:1,mobile:false})}
  if(i===0){
   const before=await ev('window.__tokenReview.reports.length');
   await waitFor(`window.__tokenReview.reports.length>${before}&&taList.getAttribute('aria-busy')==='false'`,45000);
   assert.equal(await ev('taTimeMode.value'),'0');assert.equal(await ev('taFrom.value'),time(from));assert.equal(await ev('taTo.value'),time(to));
   assert.equal(await ev('taDetail.open'),true);assert.ok((await ev('taDetailBody.innerText')).includes(report.items[i].display.request_id));
   const refreshed=await ev('taList.innerText');for(const item of items)assert.ok(refreshed.includes(item.display.request_id),'refresh lost a loaded page identity');
   checks.push({name:'actual automatic refresh preserves fixed range, pages and open detail',status:'pass',rows:items.length});
  }
  await click('#taDetailClose');
 }
 checks.push({name:'actual detail rendering at desktop and 390px',status:'pass',details:Math.min(3,report.items.length)});
 const beforeHour=await ev('window.__tokenReview.reports.length');
 await click('#taTimeMode');await b.key('Home');await b.key('ArrowDown');await b.key('Enter');
 assert.equal(await ev('taTimeMode.value'),'3600');
 const hour=`window.__tokenReview.reports.slice(${beforeHour}).findLast(r=>r.range?.to-r.range?.from===3600)`;
 await waitFor(`!!(${hour})&&taList.getAttribute('aria-busy')==='false'`,30000);const firstHour=await ev(hour);
 await waitFor(`(${hour})?.range?.to>${firstHour.range.to}&&taList.getAttribute('aria-busy')==='false'`,45000);
 const lastHour=await ev(hour);checks.push({name:'actual one-hour mode advances on automatic refresh',status:'pass',first:firstHour.range,last:lastHour.range});
 await click('[data-ta-detail="0"]');await waitFor("taDetail.open&&!/正在读取/.test(taDetailBody.innerText)",15000);
 const beforeTroubleshoot=await ev('window.__tokenReview.logchainQueries');await click('#taTroubleshoot');
 await waitFor("!document.getElementById('tab-logchain').hidden",15000);await call('Page.bringToFront');await b.pause(35000);
 assert.equal(await ev('window.__tokenReview.logchainQueries'),beforeTroubleshoot,'Token prefill must wait for a manual customer-troubleshooting query');
 checks.push({name:'actual Token to customer troubleshooting keeps manual-query boundary',status:'pass',observation_seconds:35,source_query_requests:0});
 assert.equal(b.exceptions.length,0);assert.deepEqual(b.external,[]);
}catch(e){failure=e;console.error(e.message)}
finally{
 try{fs.writeFileSync(path.join(out,'final-dom.json'),JSON.stringify(await ev("({text:taList.innerText,busy:taList.getAttribute('aria-busy'),rows:taList.querySelectorAll('[data-ta-detail]').length,from:taFrom.value,to:taTo.value,mode:taTimeMode.value,error:taError.innerText})"),null,2)+'\n')}catch{}
 let events=[];try{events=await ev('window.__tokenReview.events')}catch{}
 fs.writeFileSync(path.join(out,'browser-results.json'),JSON.stringify({at:new Date().toISOString(),image:opt('--image'),from,to,actual_data:true,response_replacement:false,checks,events,exceptions:b.exceptions,external:b.external,error:failure?.message||null},null,2)+'\n');
 await b.close();
}
if(failure)process.exitCode=1;
