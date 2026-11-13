// Final-image browser regression. Assets are never replaced. Slow/error/late
// response cases are explicit client fault injection, separate from real timers.
import assert from 'node:assert/strict';
import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';
import {browserSession} from './browser-session.mjs';
const args=process.argv.slice(2),opt=k=>args[args.indexOf(k)+1];
assert.ok(args.includes('--out')&&args.includes('--scratch')&&args.includes('--browser'));
const out=path.resolve(opt('--out')),origin='http://127.0.0.3:8204',expected=args.includes('--image')?opt('--image'):'v128';fs.mkdirSync(out,{recursive:true});
const frozenManifest=args.includes('--source-manifest')?JSON.parse(fs.readFileSync(opt('--source-manifest'),'utf8')):null;
const checks=[],sha=x=>createHash('sha256').update(x).digest('hex');
const b=await browserSession({browser:opt('--browser'),scratch:opt('--scratch'),origin});
const {call,evaluate:ev,click,waitFor,pause}=b;let failure;
const probe=String.raw`(()=>{
 const paths=new Set(['/model-statistics/report','/model-statistics/frt','/alerts/rejections','/logchain/requests','/customer-health/report']);
 const original=window.fetch.bind(window),p=window.__review={events:[],delayPath:'',delayMS:0,failPath:'',holdPath:'',release:null};
 window.fetch=async function(input,options={}){
  const u=new URL(typeof input==='string'?input:input.url,location.href);
  if(!paths.has(u.pathname))return original(input,options);
  const e={path:u.pathname,at:Date.now(),query:Object.fromEntries([...u.searchParams].filter(([k])=>['from','to','date','to_date','from_time','to_time','hours','limit','cutoff_ts','cursor','before_ts','before_id'].includes(k)))};p.events.push(e);
  options.signal?.addEventListener('abort',()=>e.cancelled=true,{once:true});
  if(p.failPath===u.pathname){p.failPath='';e.injected='client network failure';throw new TypeError('controlled acceptance network failure')}
  const r=await original(input,options),raw=await r.clone().text(),body=JSON.parse(raw);e.status=r.status;e.received=Date.now();
  e.through=body.actual_through_ts??null;e.generated_at=body.generated_at;e.lag_seconds=body.lag_seconds??null;e.collection_failed=body.collection_failed??false;
  const hold=p.holdPath===u.pathname,delay=p.delayPath===u.pathname?p.delayMS:0;if(hold)p.holdPath='';
  return new Proxy(r,{get(target,key){if(key==='json'||key==='text')return async()=>{if(hold){e.injected='late real body';await new Promise(resolve=>p.release=resolve);p.release=null}if(delay){e.injected='slow real body';await new Promise(resolve=>setTimeout(resolve,delay))}e.delivered=Date.now();return key==='json'?body:raw};const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value}});
 };
})()`;
function save(){fs.writeFileSync(path.join(out,'result.json'),JSON.stringify({at:new Date().toISOString(),origin,expected_image:expected,browser:b.version,kind:'final image, unchanged application assets; explicit client fault injection for slow/error/late cases',checks,exceptions:b.exceptions,external:b.external,failure:failure?.stack||null},null,2)+'\n')}
async function check(name,fn){try{checks.push({name,status:'pass',evidence:await fn()});console.log('PASS '+name);save()}catch(e){checks.push({name,status:'fail',error:e.message});save();if(!args.includes('--continue'))throw e;console.error('FAIL '+name+': '+e.message)}}
const events=p=>ev(`window.__review.events.filter(e=>e.path===${JSON.stringify(p)})`);
const completed=p=>ev(`window.__review.events.filter(e=>e.path===${JSON.stringify(p)}&&e.delivered).length`);
async function completedAfter(p,n,ms=45000){await waitFor(`window.__review.events.filter(e=>e.path===${JSON.stringify(p)}&&e.delivered).length>${n}`,ms);await pause(180)}
async function activate(){const other=await call('Target.createTarget',{url:'about:blank'},null);await call('Target.activateTarget',{targetId:other.targetId},null);await pause(300);await call('Target.activateTarget',{targetId:b.targetId},null);await call('Page.bringToFront');await call('Target.closeTarget',{targetId:other.targetId},null)}
async function navigate(tab,context={}){await ev(`window.monitorNavigate(${JSON.stringify(tab)},${JSON.stringify(context)})`)}
async function screenshot(name){await ev(`(()=>{let el=document.getElementById('acceptance-redaction');if(!el){el=document.createElement('style');el.id='acceptance-redaction';el.textContent='#lcTableBody,#alTableBody,#chRows,#taList tbody,#imList tbody{filter:blur(9px)}';document.head.append(el)}})()`);await b.screenshot(path.join(out,name+'.png'))}
try{
 await call('Page.addScriptToEvaluateOnNewDocument',{source:probe});await call('Page.navigate',{url:origin+'/login'});await pause(400);
 assert.equal(await ev("(async()=>{const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:'local',password:'local'})});return r.status})()"),200);
 await call('Page.navigate',{url:origin+'/?#tab=model-statistics'});await waitFor("typeof window.monitorNavigate==='function'",15000);
 await check('actual image and embedded JS/CSS match the reviewed source',async()=>{
  const ready=await ev("fetch('/ready',{cache:'no-store'}).then(r=>r.json())");assert.equal(ready.image_version,expected);
  const hashes=[];for(const file of ['view_refresh.js','model_frt.js','model_frt.css','model_statistics.js','alerts.js','logchain.js','customer_health.js','token_anomaly.js','incident.js','incident.css']){const text=await ev(`fetch('/'+${JSON.stringify(file.replaceAll('_','-'))},{cache:'no-store'}).then(r=>{if(!r.ok)throw Error('asset unavailable');return r.text()})`);const digest=sha(text),expectedDigest=frozenManifest?frozenManifest.find(f=>f.path==='monitor/'+file)?.sha256:sha(fs.readFileSync(path.join('monitor',file)));assert.equal(digest,expectedDigest,file);hashes.push({file,sha256:digest})}return {image:ready.image_version,status:ready.status,source_manifest_sha256:frozenManifest?sha(fs.readFileSync(opt('--source-manifest'))):null,assets:hashes};
 });
 const specs=[
  {tab:'model-statistics',context:{view:'frt'},path:'/model-statistics/frt',root:'#msFRTView',rows:'[data-frt-key]',ready:'.frt-channel',scroll:2600,interval:30000,dates:['frtDateStart','frtDateEnd']},
  {tab:'model-statistics',context:{view:'requests'},path:'/model-statistics/report',root:'#msRequestsView',rows:'[data-ms-expand]',ready:'[data-ms-expand]',scroll:861,interval:30000,dates:['msDateStart','msDateEnd']},
  {tab:'alerts',context:{},path:'/alerts/rejections',root:'#tab-alerts',rows:'[data-al-row]',ready:'#alTableBody tr',scroll:900,interval:30000,dates:['alDate']},
  {tab:'logchain',context:{},path:'/logchain/requests',root:'#tab-logchain',rows:'[data-lc-id]',ready:'[data-lc-id]',scroll:900,interval:30000,dates:['lcDate','lcToDate','lcFromTime','lcToTime','lcUserID']},
  {tab:'customer-health',context:{},path:'/customer-health/report',root:'#tab-customer-health',rows:'[data-ch-row]',ready:'[data-ch-row]',scroll:900,interval:60000,dates:[]},
 ];
 for(const s of specs.filter(s=>!args.includes('--modules')||opt('--modules').split(',').includes(s.tab)))await check(s.path+' keeps content, visible row, focus and dates through real cycles and injected failures',async()=>{
  const n=await completed(s.path);await navigate(s.tab,s.context);await completedAfter(s.path,n);await waitFor(`!!document.querySelector(${JSON.stringify(s.ready)})`,30000);
  if(s.path.endsWith('/frt'))await ev("document.querySelectorAll('.frt-channel').forEach(e=>e.open=true);document.querySelectorAll('.frt-group').forEach(e=>e.open=true)");
  if(s.path.endsWith('/report')&&s.tab==='model-statistics')await ev("document.querySelector('[data-ms-expand]').click()");
  await ev(`window.scrollTo(0,${s.scroll})`);
  await pause(120);
  // Focus a control in the row being checked, or leave focus on the body when
  // reading a non-interactive row. Focusing an unrelated visible header control
  // asks the product to preserve that control instead of this table row.
  const anchor=await ev(`(()=>{const e=[...document.querySelectorAll(${JSON.stringify(s.rows)})].find(e=>{const r=e.getBoundingClientRect();return r.height&&r.top>=0&&r.top<innerHeight});if(!e)return null;const a=[...e.attributes].find(a=>['data-frt-key','data-ms-expand','data-al-row','data-lc-id','data-ch-row'].includes(a.name));const f=e.matches('button,input,select,summary')?e:e.querySelector('button,input,select,summary');if(f)f.focus({preventScroll:true});else document.activeElement?.blur();return {key:a.name,value:a.value,top:e.getBoundingClientRect().top,scroll:scrollY,focus_id:f?.id||'',row_has_control:!!f};})()`);assert.ok(anchor,'need actual visible data row');
  const position=()=>ev(`(()=>{const e=[...document.querySelectorAll('['+${JSON.stringify(anchor.key)}+']')].find(e=>e.getAttribute(${JSON.stringify(anchor.key)})===${JSON.stringify(anchor.value)});if(!e)return null;const f=${JSON.stringify(anchor.focus_id)}?document.getElementById(${JSON.stringify(anchor.focus_id)}):e.matches('button,input,select,summary')?e:e.querySelector('button,input,select,summary');return {top:e.getBoundingClientRect().top,scroll:scrollY,focus:document.activeElement===(f||document.body)}})()`);
  const selected=()=>ev(`Object.fromEntries(${JSON.stringify(s.dates)}.map(id=>[id,document.getElementById(id).value]))`),initial=await selected();
  let count=await completed(s.path);await ev(`window.__review.delayPath=${JSON.stringify(s.path)};window.__review.delayMS=1400;window.dispatchEvent(new Event('focus'))`);await pause(250);assert.equal((await position())?.scroll,anchor.scroll,'slow fetch collapsed content');await completedAfter(s.path,count);await ev("window.__review.delayPath='';window.__review.delayMS=0");
  let p=await position();assert.ok(p&&Math.abs(p.top-anchor.top)<2,'slow response lost row anchor');assert.ok(p.focus,'slow response lost focus');
  await ev(`window.__review.failPath=${JSON.stringify(s.path)};window.dispatchEvent(new Event('focus'))`);await pause(250);p=await position();
  fs.writeFileSync(path.join(out,s.tab+'-error-position.json'),JSON.stringify({before:{top:anchor.top,scroll:anchor.scroll},after:p,row_identity_hash:sha(anchor.value)},null,2)+'\n');
  assert.ok(p&&Math.abs(p.top-anchor.top)<2,'error moved visible row');assert.deepEqual(await selected(),initial);
  count=await completed(s.path);await activate();await completedAfter(s.path,count);p=await position();assert.ok(p&&Math.abs(p.top-anchor.top)<2,'visibility refresh lost anchor');
  const watermarks=[];
  for(let cycle=0;cycle<2;cycle++){count=await completed(s.path);await completedAfter(s.path,count,s.interval+25000);const last=(await events(s.path)).filter(e=>e.delivered).at(-1);p=await position();assert.ok(p&&Math.abs(p.top-anchor.top)<2,'timer lost visible row');assert.deepEqual(await selected(),initial);watermarks.push({through:last.through,generated_at:last.generated_at,lag_seconds:last.lag_seconds,query:last.query})}
  if(anchor.scroll>0)assert.ok((await position()).scroll>0,'unexpected jump to top');return {initial_scroll:anchor.scroll,final_scroll:(await position()).scroll,scrollable_actual_list:anchor.scroll>0,selected_dates:initial,date_controls_applicable:s.dates.length>0,row_identity_hash:sha(anchor.value),row_top:anchor.top,watermarks,timer_cycles:2,normal_requests:'actual API',injections:['1400ms real body delay','client network rejection']};
 });
 if(!args.includes('--skip-extra')){
 await check('FRT old real response cannot overwrite a newer result',async()=>{
  const p='/model-statistics/frt';let n=await completed(p);await navigate('model-statistics',{view:'frt'});await completedAfter(p,n);
  await ev("window.__review.holdPath='/model-statistics/frt';window.dispatchEvent(new Event('focus'))");await waitFor('!!window.__review.release',30000);
  n=await completed(p);await ev("window.dispatchEvent(new Event('focus'))");await completedAfter(p,n);const current=sha(await ev('frtTree.innerHTML'));
  await ev('window.__review.release()');await pause(400);assert.equal(sha(await ev('frtTree.innerHTML')),current);assert.ok((await events(p)).some(e=>e.cancelled&&e.injected==='late real body'));return {late_response_ignored:true,new_result_sha256:current};
 });
 await check('Token and incident actual disabled/incomplete state, same-level tabs, fixed dates and narrow layouts',async()=>{
  const states=[];for(const view of ['token','incidents']){
   await navigate('anomalies',{view});const ready=view==='token'?"taList.getAttribute('aria-busy')==='false'":"imList.getAttribute('aria-busy')==='false'";await waitFor(ready,30000);
   const state=await ev(`document.getElementById(${JSON.stringify(view==='token'?'tokenAnomalyView':'incidentView')}).innerText`);
   assert.match(state,/未启用|未开启|覆盖不足|覆盖未完整|采集未完整|健康状态不可用|检测队列.*恢复|检测中|证据不足/);assert.doesNotMatch(state,/当前所选时间内没有 Token 异常情况|当前所选时间内没有已记录事故/);states.push({view,state_sha256:sha(state),actual_unavailable_notice:true});
   if(view==='token')assert.equal(await ev('taMore.open'),false);
   const prefix=view==='token'?'ta':'im';await ev(`${prefix}From.value='2026-10-21T00:00';${prefix}To.value='2026-10-22T00:00';${prefix}Filters.requestSubmit()`);await waitFor(ready,30000);await activate();await pause(250);assert.deepEqual(await ev(`[${prefix}From.value,${prefix}To.value]`),['2026-10-21T00:00','2026-10-22T00:00']);
   for(const theme of ['dark','light'])for(const width of [320,375]){await ev(`document.body.setAttribute('theme-mode',${JSON.stringify(theme)})`);await call('Emulation.setDeviceMetricsOverride',{width,height:950,deviceScaleFactor:1,mobile:false});await ev('window.scrollTo(0,0)');assert.ok(await ev('document.documentElement.scrollWidth<=innerWidth'));await screenshot(view+'-'+theme+'-'+width)}
   await call('Emulation.setDeviceMetricsOverride',{width:1280,height:950,deviceScaleFactor:1,mobile:false});
  }return {states,enabled_record_acceptance:'not tested: disabled capabilities remain disabled'};
 });
 await check('actual FRT three levels have distinct backgrounds in both themes at 320/375',async()=>{
  const n=await completed('/model-statistics/frt');await navigate('model-statistics',{view:'frt'});await completedAfter('/model-statistics/frt',n);await ev("document.querySelector('.frt-channel').open=true;document.querySelector('.frt-group').open=true");
  for(const theme of ['dark','light'])for(const width of [320,375]){await ev(`document.body.setAttribute('theme-mode',${JSON.stringify(theme)})`);await call('Emulation.setDeviceMetricsOverride',{width,height:950,deviceScaleFactor:1,mobile:false});assert.ok(await ev('document.documentElement.scrollWidth<=innerWidth'));const colors=await ev("['.frt-channel','.frt-group','.frt-model'].map(s=>getComputedStyle(document.querySelector(s)).backgroundColor)");assert.equal(new Set(colors).size,3);await ev("document.querySelector('.frt-channel').scrollIntoView({block:'start'})");await screenshot('frt-'+theme+'-'+width)}return {themes:2,widths:[320,375]};
 });
 }
 assert.equal(b.exceptions.length,0);assert.deepEqual(b.external,[]);
}catch(e){failure=e;console.error(e.message)}finally{
 try{fs.writeFileSync(path.join(out,'requests.json'),JSON.stringify(await ev('window.__review?.events||[]'),null,2)+'\n')}catch{}
 save();await b.close();
}
if(failure||checks.some(c=>c.status==='fail'))process.exitCode=1;
