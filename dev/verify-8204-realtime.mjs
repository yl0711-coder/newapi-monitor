// Read-only browser acceptance against the local 8204 image. Ordinary checks
// use real responses/timers. The race check delays one real response body in
// the browser (after transport completes) to exercise the generation guard.
// No collector settings, recompute jobs or production records are changed.
import assert from 'node:assert/strict';
import {mkdirSync, mkdtempSync, readFileSync, writeFileSync} from 'node:fs';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import path from 'node:path';

const args=process.argv.slice(2), option=n=>args[args.indexOf(n)+1];
assert.ok(args.includes('--browser')&&args.includes('--out'),'--browser and --out required');
const out=path.resolve(option('--out')), origin='http://127.0.0.3:8204';
const selectedPages=args.includes('--pages')?option('--pages').split(','):null;
const layoutOnly=args.includes('--layout-only'),workspaceStyles=args.includes('--workspace-styles');
const workspaceAlerts=args.includes('--workspace-alerts'),watchSeconds=args.includes('--watch-seconds')?Number(option('--watch-seconds')):0;
assert.ok(Number.isFinite(watchSeconds)&&watchSeconds>=0&&watchSeconds<=420,'--watch-seconds must be 0..420');
const localCSS=workspaceStyles?[...readFileSync(new URL('../monitor/page.html',import.meta.url),'utf8').matchAll(/<style>([\s\S]*?)<\/style>/g)].map(m=>m[1]).join('\n'):'';
const localAlerts=workspaceAlerts?readFileSync(new URL('../monitor/alerts.js',import.meta.url),'utf8'):'';
const injectedAssets=[...(workspaceStyles?[{path:'monitor/page.html inline styles',sha256:createHash('sha256').update(localCSS).digest('hex')}]:[]),
  ...(workspaceAlerts?[{path:'monitor/alerts.js',sha256:createHash('sha256').update(localAlerts).digest('hex')}]:[])];
mkdirSync(out,{recursive:true});
const profile=mkdtempSync(path.join(out,'profile-')), temp=path.join(out,'temp');
mkdirSync(temp,{recursive:true});
const browser=spawn(option('--browser'),['--headless=new','--remote-debugging-pipe','--no-first-run',
  '--no-default-browser-check','--disable-component-update','--disable-sync','--disable-default-apps',
  '--metrics-recording-only','--lang=zh-CN','--window-size=1400,1000',`--user-data-dir=${profile}`,'about:blank'],
  {stdio:['ignore','pipe','pipe','pipe','pipe'],windowsHide:true,env:{...process.env,TEMP:temp,TMP:temp}});
let seq=0,received='',sessionId,targetId,version,failure;
const pending=new Map(),checks=[],exceptions=[],consoleErrors=[];
browser.stderr.on('data',()=>{});
browser.stdio[4].on('data',chunk=>{
  received+=chunk.toString();
  for(let at;(at=received.indexOf('\0'))>=0;){
    const raw=received.slice(0,at);received=received.slice(at+1);if(!raw)continue;
    const msg=JSON.parse(raw),task=pending.get(msg.id);
    if(task){pending.delete(msg.id);clearTimeout(task.timer);msg.error?task.reject(Error(JSON.stringify(msg.error))):task.resolve(msg.result)}
    else if(msg.method==='Runtime.exceptionThrown')exceptions.push(msg.params.exceptionDetails.text);
    else if(msg.method==='Runtime.consoleAPICalled'&&msg.params.type==='error')consoleErrors.push('browser console error');
    else if(msg.method==='Fetch.requestPaused'){
      call('Fetch.fulfillRequest',{requestId:msg.params.requestId,responseCode:200,
        responseHeaders:[{name:'Content-Type',value:'application/javascript; charset=utf-8'},{name:'Cache-Control',value:'no-store'}],
        body:Buffer.from(localAlerts).toString('base64')},msg.sessionId).catch(e=>{failure=e;browser.kill()});
    }
  }
});
function call(method,params={},session=sessionId){return new Promise((resolve,reject)=>{
  const id=++seq,timer=setTimeout(()=>{pending.delete(id);reject(Error('CDP timeout '+method))},30000);
  pending.set(id,{resolve,reject,timer});browser.stdio[3].write(JSON.stringify({id,method,params,...(session?{sessionId:session}:{})})+'\0');
})}
async function evaluate(expression){const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw Error(r.exceptionDetails.text);return r.result.value}
const pause=ms=>new Promise(r=>setTimeout(r,ms));
async function waitFor(expression,ms=45000){const end=Date.now()+ms;do{if(await evaluate(expression))return;await pause(250)}while(Date.now()<end);throw Error('waitFor timeout: '+expression)}
async function click(selector){const p=await evaluate(`(()=>{const e=[...document.querySelectorAll(${JSON.stringify(selector)})].find(e=>e.getBoundingClientRect().width>0);if(!e)throw Error('missing visible control');e.scrollIntoView({block:'center'});const r=e.getBoundingClientRect();return{x:r.x+r.width/2,y:r.y+r.height/2}})()`);for(const type of ['mousePressed','mouseReleased'])await call('Input.dispatchMouseEvent',{type,...p,button:'left',clickCount:1})}
async function check(name,fn){try{checks.push({name,status:'pass',evidence:await fn()});console.log('PASS '+name)}catch(e){checks.push({name,status:'fail',error:e.message});throw e}}
async function shot(name){await evaluate(`(()=>{let s=document.getElementById('acceptance-privacy');if(!s){s=document.createElement('style');s.id='acceptance-privacy';s.textContent='#lcTableBody,#alTableBody,#chRows{filter:blur(10px)}';document.head.append(s)}})()`);const r=await call('Page.captureScreenshot',{format:'png'});writeFileSync(path.join(out,name+'.png'),Buffer.from(r.data,'base64'))}

const probe=`(()=>{
  const paths=new Set(['/model-statistics/report','/model-statistics/frt','/alerts/rejections','/logchain/requests','/customer-health/report']);
  const original=window.fetch.bind(window),p=window.__realtimeProbe={events:[],holdPath:'',held:null};
  window.fetch=async function(input,options={}){
    const url=new URL(typeof input==='string'?input:input.url,location.href);
    if(!paths.has(url.pathname))return original(input,options);
    const event={id:p.events.length+1,path:url.pathname,query:url.search,started:Date.now(),cache:options.cache,aborted:false};p.events.push(event);
    options.signal?.addEventListener('abort',()=>{event.aborted=true;event.abortedAt=Date.now()},{once:true});
    const hold=p.holdPath===url.pathname;if(hold)p.holdPath='';
    try{
      const r=await original(input,options),body=await r.clone().text();let j={};try{j=JSON.parse(body)}catch{}
      event.status=r.status;event.cacheControl=r.headers.get('cache-control');event.done=Date.now();
      event.meta={generated_at:j.generated_at,actual_through_ts:j.actual_through_ts??j.query_through_ts??j.through_ts??j.to_ts,
        lag_seconds:j.lag_seconds??j.collection?.lag_seconds,coverage_complete:j.coverage_complete??j.coverage?.complete??j.collection?.metrics_available,
        collection_failed:j.collection_failed,coverage_status:j.coverage_status??j.source?.coverage_status};
      if(hold){let release;const gate=new Promise(resolve=>release=resolve);p.held={id:event.id,release};
        r.text=async()=>{await gate;return body};r.json=async()=>{await gate;return JSON.parse(body)};
      }
      return r;
    }catch(e){event.error=e.name;throw e}
  };
})()`;
const pages=[
  {name:'model-statistics',tab:'model-statistics',sub:'#msRequestsTab',path:'/model-statistics/report',status:'#msStatus',coverage:'#msCoverage',previous:'#msDates [data-md-action="yesterday"]',latest:'#msDates [data-md-action="today"]',controls:['msDateStart','msDateEnd'],interval:30000},
  {name:'frt',tab:'model-statistics',sub:'#msFRTTab',path:'/model-statistics/frt',status:'#frtStatus',coverage:'#frtCoverage',previous:'#frtDates [data-md-action="yesterday"]',latest:'#frtDates [data-md-action="today"]',controls:['frtDateStart','frtDateEnd','frtMeanGt'],interval:30000},
  {name:'alerts',tab:'alerts',path:'/alerts/rejections',status:'#alStatus',coverage:'#alCoverageWarning',previous:'#alPrev',latest:'#alToday',controls:['alDate','alReason','alUser'],interval:30000},
  {name:'logchain',tab:'logchain',path:'/logchain/requests',status:'#lcStatus',coverage:'#lcNotes',previous:'#lcPrevDay',latest:'#lcToday',controls:['lcDate','lcToDate','lcFromTime','lcToTime','lcStream'],interval:30000},
  {name:'customer-health',tab:'customer-health',path:'/customer-health/report',status:'#chMeta',coverage:'#chMeta',controls:[],interval:60000},
];
if(selectedPages)assert.ok(selectedPages.length&&selectedPages.every(n=>pages.some(p=>p.name===n)),'unknown --pages value');
const events=p=>evaluate(`window.__realtimeProbe.events.filter(e=>e.path===${JSON.stringify(p.path)})`);
const status=p=>evaluate(`document.querySelector(${JSON.stringify(p.status)})?.innerText||''`);
const snapshot=p=>evaluate(`({status:document.querySelector(${JSON.stringify(p.status)})?.innerText,coverage:document.querySelector(${JSON.stringify(p.coverage)})?.innerText})`);
const controls=p=>evaluate(`Object.fromEntries(${JSON.stringify(p.controls)}.map(id=>[id,document.getElementById(id)?.value]))`);
async function settled(p){await waitFor(`/已更新|出报于/.test(document.querySelector(${JSON.stringify(p.status)})?.innerText||'')`)}
async function requestAfter(p,count,ms=45000){await waitFor(`window.__realtimeProbe.events.filter(e=>e.path===${JSON.stringify(p.path)}&&e.done&&e.status===200).length>${count}`,ms);await settled(p)}
const completed=async p=>(await events(p)).filter(e=>e.done&&e.status===200).length;
const watchdog=setTimeout(()=>browser.kill(),15*60*1000);
try{
  version=await call('Browser.getVersion',{},null);
  ({targetId}=await call('Target.createTarget',{url:'about:blank'},null));
  ({sessionId}=await call('Target.attachToTarget',{targetId,flatten:true},null));
  await call('Page.enable');await call('Runtime.enable');
  if(workspaceAlerts)await call('Fetch.enable',{patterns:[{urlPattern:origin+'/alerts.js*',requestStage:'Request'}]});
  await call('Page.addScriptToEvaluateOnNewDocument',{source:probe});
  await call('Emulation.setDeviceMetricsOverride',{width:1400,height:1000,deviceScaleFactor:1,mobile:false});
  await call('Page.navigate',{url:origin+'/login'});await pause(1000);
  assert.equal(await evaluate(`(async()=>{const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:'local',password:'local'})});return r.status})()`),200);
  await call('Page.navigate',{url:origin+'/'});await waitFor("typeof window.monitorNavigate==='function'");
  if(workspaceStyles)await evaluate(`(()=>{const s=document.createElement('style');s.id='acceptance-workspace-css';s.textContent=${JSON.stringify(localCSS)};document.head.append(s)})()`);
  for(const p of pages.filter(p=>!selectedPages||selectedPages.includes(p.name))){
    await click(`[data-tab="${p.tab}"]`);if(p.sub)await click(p.sub);await settled(p);
    if(!layoutOnly){
    await check(p.name+': freshness and no-store',async()=>{
      const e=(await events(p)).findLast(e=>e.done&&e.status===200);assert.ok(e);
      assert.equal(e.cache,'no-store');assert.match(e.cacheControl,/no-store/);
      assert.ok(e.meta.generated_at);assert.ok(e.meta.actual_through_ts>0);assert.ok(Number.isFinite(e.meta.lag_seconds));
      const text=(await status(p))+' '+await evaluate(`document.querySelector(${JSON.stringify(p.coverage)})?.innerText||''`);
      assert.match(text,/水位|截止|覆盖/);
      if(e.meta.coverage_complete===false||e.meta.lag_seconds>0||e.meta.collection_failed)assert.match(text,/延迟|不完整|未完整|未.*确认|同步中|采集|待定稿/);
      await shot(p.name+'-desktop');return {meta:e.meta,status:await status(p)};
    });
    await check(p.name+': real periodic refresh',async()=>{
      const before=await completed(p),selection=await controls(p),start=Date.now();
      await requestAfter(p,before,p.interval+20000);assert.deepEqual(await controls(p),selection);
      if(p.name==='alerts')assert.equal(new URL((await events(p)).at(-1).query,origin).searchParams.has('cutoff_ts'),false,'periodic refresh reused the old snapshot cutoff');
      return {interval_ms:p.interval,observed_wait_ms:Date.now()-start};
    });
    await check(p.name+': browser reactivation refresh',async()=>{
      const before=await completed(p),selection=await controls(p);
      const other=await call('Target.createTarget',{url:'about:blank'},null);
      await call('Target.activateTarget',{targetId:other.targetId},null);await pause(600);
      await call('Target.activateTarget',{targetId},null);await call('Page.bringToFront');
      await requestAfter(p,before,10000);await call('Target.closeTarget',{targetId:other.targetId},null);
      assert.deepEqual(await controls(p),selection);return {requests_after_activation:(await completed(p))-before};
    });
    if(p.previous)await check(p.name+': date switch preserved across refresh',async()=>{
      const prior=await controls(p);await click(p.previous);await settled(p);const selected=await controls(p);assert.notDeepEqual(selected,prior);
      const before=await completed(p);await evaluate("window.dispatchEvent(new Event('focus'))");await requestAfter(p,before);
      assert.deepEqual(await controls(p),selected);return selected;
    });
    if(p.name==='frt'||p.name==='alerts'||p.name==='logchain')await check(p.name+': filter preserved across refresh',async()=>{
      const before=await completed(p);
      if(p.name==='frt'){
        await evaluate("document.getElementById('frtMeanGt').value='3'");await click('#frtFilters button[type=submit]');
      }else{
        const id=p.name==='alerts'?'alReason':'lcStream';
        await evaluate(`(()=>{const e=document.getElementById(${JSON.stringify(id)});e.selectedIndex=1;e.dispatchEvent(new Event('change',{bubbles:true}))})()`);
      }
      await requestAfter(p,before);const selected=await controls(p),count=await completed(p);
      await evaluate("window.dispatchEvent(new Event('focus'))");await requestAfter(p,count);assert.deepEqual(await controls(p),selected);return selected;
    });
    await check(p.name+': abort and late body cannot overwrite',async()=>{
      await evaluate(`window.__realtimeProbe.holdPath=${JSON.stringify(p.path)};window.__realtimeProbe.held=null;window.dispatchEvent(new Event('focus'))`);
      await waitFor('window.__realtimeProbe.held!==null');
      const held=await evaluate('window.__realtimeProbe.held.id'),before=await completed(p);
      if(p.latest)await click(p.latest);else{await pause(1200);await evaluate("window.dispatchEvent(new Event('focus'))")}
      await requestAfter(p,before);
      assert.equal(await evaluate(`window.__realtimeProbe.events.find(e=>e.id===${held}).aborted`),true);
      const text=await snapshot(p),selection=await controls(p);
      const old=(await events(p)).find(e=>e.id===held),latest=(await events(p)).findLast(e=>e.done&&e.id!==held);
      if(p.latest)assert.notEqual(old.query,latest.query);else assert.notEqual(old.meta.generated_at,latest.meta.generated_at);
      await evaluate('window.__realtimeProbe.held.release();window.__realtimeProbe.held=null');await pause(500);
      assert.deepEqual(await snapshot(p),text);assert.deepEqual(await controls(p),selection);
      return {old_request_aborted:true,late_real_body_ignored:true};
    });
    await check(p.name+': leave and return refresh',async()=>{
      const selected=await controls(p),before=await completed(p);
      const other=p.tab==='alerts'?'customer-health':'alerts';
      await click(`[data-tab="${other}"]`);await pause(300);await click(`[data-tab="${p.tab}"]`);if(p.sub)await click(p.sub);
      await requestAfter(p,before);assert.deepEqual(await controls(p),selected);return selected;
    });
    }
    await check(p.name+': narrow screens',async()=>{
      const results=[];for(const width of [375,320]){
        await call('Emulation.setDeviceMetricsOverride',{width,height:900,deviceScaleFactor:1,mobile:false});await pause(200);
        const layout=await evaluate(`({width:innerWidth,scroll:document.documentElement.scrollWidth,status:!!document.querySelector(${JSON.stringify(p.status)})?.getBoundingClientRect().width})`);
        if(layout.scroll>width+2){
          const overflow=await evaluate(`[...document.querySelectorAll('#tab-${p.tab} *')].filter(e=>!e.closest('table')).map(e=>{const r=e.getBoundingClientRect();return {id:e.id,tag:e.tagName,class:e.className,left:r.left,right:r.right,width:r.width}}).filter(r=>r.width>0&&(r.right>innerWidth+2||r.left<0)).slice(0,25)`);
          writeFileSync(path.join(out,p.name+'-overflow-'+width+'.json'),JSON.stringify(overflow,null,2));await shot(p.name+'-overflow-'+width);
        }
        assert.ok(layout.scroll<=width+2,'horizontal page overflow '+JSON.stringify(layout));assert.ok(layout.status);results.push(layout);await shot(p.name+'-'+width);
      }
      await call('Emulation.setDeviceMetricsOverride',{width:1400,height:1000,deviceScaleFactor:1,mobile:false});return results;
    });
    if(watchSeconds&&['alerts','model-statistics','frt'].includes(p.name))await check(p.name+': real watermark advancement',async()=>{
      const before=(await events(p)).findLast(e=>e.done&&e.status===200),start=Date.now(),samples=[];
      do{
        await pause(Math.min(30000,watchSeconds*1000-(Date.now()-start)));
        const e=(await events(p)).findLast(e=>e.done&&e.status===200);samples.push(e);
        console.log('OBSERVE '+p.name+' through='+e.meta.actual_through_ts+' lag='+e.meta.lag_seconds);
      }while(Date.now()-start<watchSeconds*1000);
      const latest=samples.at(-1);assert.ok(latest.meta.actual_through_ts>before.meta.actual_through_ts,'collector or page watermark did not advance');
      assert.ok(samples.every(e=>e.meta.lag_seconds<=300),'watermark exceeded the configured 300-second collection cycle');
      return {seconds:watchSeconds,start:before.meta,end:latest.meta,samples:samples.map(e=>({query:e.query,done:e.done,...e.meta})),status:await snapshot(p)};
    });
  }
  assert.equal(exceptions.length,0);assert.equal(consoleErrors.length,0);
}catch(e){failure=e;console.error(e.message)}finally{
  let requests=[];try{requests=await evaluate('window.__realtimeProbe?.events||[]')}catch{}
  writeFileSync(path.join(out,'result.json'),JSON.stringify({generated_at:new Date().toISOString(),target:origin,browser:version?.product,scope:injectedAssets.length?'8204 real APIs with specified workspace assets substituted only in this browser; NOT deployed image acceptance':'8204 deployed image',injectedAssets,layoutOnly,checks,requests,exceptions,consoleErrors,failure:failure?.stack||null},null,2));
  clearTimeout(watchdog);browser.kill();
}
console.log('passed='+checks.filter(c=>c.status==='pass').length+' failed='+checks.filter(c=>c.status==='fail').length);
if(failure)process.exitCode=1;
