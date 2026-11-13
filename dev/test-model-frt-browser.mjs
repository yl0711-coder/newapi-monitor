// Real Chrome/CDP regression against the complete repository page and local synthetic APIs.
// No live Monitor, production data, external network or input.value assignments.
// Usage: node dev/test-model-frt-browser.mjs --browser <chrome> --out <directory>
import assert from 'node:assert/strict';
import {report as tokenReport} from './token-anomaly-fixture.mjs';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync} from 'node:fs';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {once} from 'node:events';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const args=process.argv.slice(2),option=name=>args[args.indexOf(name)+1];
assert.ok(args.includes('--browser')&&args.includes('--out'),'--browser and --out required (put artifacts on a disk with free space)');
const out=path.resolve(option('--out'));mkdirSync(out,{recursive:true});
const profile=mkdtempSync(path.join(out,'profile-'));
const browserTemp=path.join(out,'temp');mkdirSync(browserTemp,{recursive:true});
const html=readFileSync(path.join(root,'monitor/page.html'),'utf8');
const embedSource=readFileSync(path.join(root,'monitor/server.go'),'utf8');
const embedMap=new Map([...embedSource.matchAll(/\/\/go:embed ([^\r\n]+)\r?\nvar (\w+)/g)].map(m=>[m[2],m[1]]));
const assets=new Map([...embedSource.matchAll(/r\.GET\("([^"\n]+\.(?:js|css))",[\s\S]*?c\.Data\(http\.StatusOK, "[^"]+", (?:\[\]byte\()?([\w]+)\)?\)/g)].map(m=>[m[1],embedMap.get(m[2])]));

const requests=[],checks=[],exceptions=[],external=[],consoleErrors=[];
let scenario='complete';
const end=Date.UTC(2026,9,16,8,20)/1000;
const frt={mean_ms:760,mean_coverage_complete:true,mean_unavailable_reason:'',observed:100,p50_ms:500,p95_ms:3900,p99_ms:3980,max_ms:4000,over_3s:10,over_3s_pct:10,coverage_complete:true,unavailable_reason:''};
const timing={finalize_delay_seconds:60,alignment_seconds:60,collection_poll_seconds:60};
const legacy={requests:100,routed_requests:100,unavailable_channel_requests:0,frt_observed:100,frt_p50_ms:500,frt_p95_ms:3900,frt_p99_ms:3980,frt_max_ms:4000,frt_over_3s:10};
function rangeFor(url,mode){
 const q=url.searchParams,key=q.get('window')||'24h',seconds={'24h':86400,'3d':259200,'7d':604800}[key];
 const available=mode==='stale-source'?Date.UTC(2026,9,15,16)/1000-60:end;
 const requestedTo=q.has('to')?Date.parse(q.get('to'))/1000:available,requestedFrom=q.has('from')?Date.parse(q.get('from'))/1000:requestedTo-seconds;
 const calendar=q.get('range')==='calendar',to=calendar?Math.max(requestedFrom,Math.min(requestedTo,available)):requestedTo;
 return {key:q.get('range')||key,from:requestedFrom,to,available,range:{mode:q.get('range')||(q.has('from')?'exact':'rolling'),requested_from:new Date(requestedFrom*1000).toISOString(),requested_to:new Date(requestedTo*1000).toISOString(),available_through:new Date(available*1000).toISOString(),retained_from:'2026-10-06T16:00:00Z',max_days:7,has_window:to>requestedFrom,today_open:calendar&&requestedTo>end+180,note:to<=requestedFrom?'所选日期尚无可用区间；不能据此确认零请求。':calendar&&requestedTo>end+180?'今日尚未结束，仅展示实际可用截止前的数据。':'本地所选日期覆盖已核验。'}};
}
function report(url,mode){
 const r=rangeFor(url,mode),to=new Date(r.to*1000).toISOString();
 const metric=mode==='missing'?{observed:null,p50_ms:null,p95_ms:null,p99_ms:null,max_ms:null,coverage_complete:false,unavailable_reason:'historical_unsupported'}:mode==='missing-mean'?{...frt,mean_ms:null,mean_coverage_complete:false,mean_unavailable_reason:'historical_sum_unavailable'}:frt;
 const complete=['complete','missing-mean','collection-failed'].includes(mode);
 return {timing,filter:{channel_id:url.searchParams.has('channel_id')?Number(url.searchParams.get('channel_id')):null,group:url.searchParams.get('group'),model:url.searchParams.get('model')},range:r.range,window:r.key,from:new Date(r.from*1000).toISOString(),to,target:new Date((end+180)*1000).toISOString(),lag_seconds:mode==='collection-failed'?0:180,generated_at:to,collection_failed:mode==='collection-failed',
 coverage:{status:complete?'complete':'incomplete',complete:r.range.has_window&&complete,note:mode==='stale-source'?r.range.note:complete?'本地覆盖已核验':'本地覆盖存在缺口，不能确认全窗耗时'},channels:mode==='empty'||!r.range.has_window?[]:[{channel_id:7,name:'fixture-'+r.key,frt:metric,groups:[{group:'actual-group',frt:metric,models:[{model:'fixture-model',frt:metric}]}]}]};
}
function json(res,status,data){res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(data))}
const server=createServer(async(req,res)=>{
 const url=new URL(req.url,'http://127.0.0.1'),mode=scenario;requests.push({method:req.method,path:url.pathname,query:url.search,scenario:mode});
 if(req.method==='POST'&&url.pathname==='/anomalies/token/detection/query'){json(res,200,tokenReport());return}
 if(req.method!=='GET'){json(res,405,{error:'read only fixture'});return}
 if(url.pathname==='/'){res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});res.end(html);return}
 if(assets.has(url.pathname)){res.writeHead(200,{'Content-Type':url.pathname.endsWith('.css')?'text/css':'application/javascript'});res.end(readFileSync(path.join(root,'monitor',assets.get(url.pathname))));return}
 if(url.pathname==='/me'){json(res,200,{name:'Local browser fixture',is_root:false});return}
 if(url.pathname==='/api/brand'){json(res,200,{name:'本地合成验收'});return}
 if(url.pathname==='/data'){json(res,200,{enabled:false});return}
 if(['/model-statistics/report','/model-statistics/frt'].includes(url.pathname)){
  if(mode==='slow')await new Promise(r=>setTimeout(r,1300));
  if(mode==='error'){json(res,503,{error:'合成本地来源不可用'});return}
 }
 if(url.pathname==='/model-statistics/report'){
 const b=rangeFor(url,mode),complete=b.range.has_window&&mode!=='stale-source';
 json(res,200,{timing,range:b.range,window:b.key,generated_at:end,from_ts:b.from,to_ts:b.to,target_ts:Math.min(end+180,b.to),lag_seconds:Math.max(0,end-b.available),collection_failed:mode==='collection-failed',group_count:1,source:{facts_complete:complete,requests_complete:complete,frt_complete:complete,coverage_status:complete?'complete':'incomplete',note:b.range.note},models:!b.range.has_window?[]:[{...legacy,model:'fixture-model',groups:[{...legacy,group:'actual-group',channels:[{...legacy,channel_id:7,channel_name:'fixture-channel'}],customers:[{customer_id:31,customer_name:'fixture-customer',requests:7}]}]}]});return;
 }
 if(url.pathname==='/model-statistics/frt'){
 json(res,200,report(url,mode));return;
 }
 json(res,200,{});
});
server.listen(0,'127.0.0.1');await once(server,'listening');
const origin='http://127.0.0.1:'+server.address().port;
const browser=spawn(option('--browser'),['--headless=new','--remote-debugging-pipe','--no-first-run','--no-default-browser-check','--disable-background-networking','--disable-component-update','--disable-sync','--disable-default-apps','--disable-client-side-phishing-detection','--metrics-recording-only','--lang=zh-CN','--window-size=1280,950',`--user-data-dir=${profile}`,'about:blank'],{stdio:['ignore','pipe','pipe','pipe','pipe'],windowsHide:true,env:{...process.env,TEMP:browserTemp,TMP:browserTemp}});
let seq=0,received='',sessionId;
const pending=new Map();
browser.stderr.on('data',()=>{});
browser.stdio[4].on('data',chunk=>{
  received+=chunk.toString();
  for(let at;(at=received.indexOf('\0'))>=0;){
    const text=received.slice(0,at);received=received.slice(at+1);if(!text)continue;
    const msg=JSON.parse(text),task=pending.get(msg.id);
    if(task){pending.delete(msg.id);clearTimeout(task.timer);msg.error?task.reject(new Error(JSON.stringify(msg.error))):task.resolve(msg.result)}
    else if(msg.method==='Runtime.exceptionThrown')exceptions.push(msg.params.exceptionDetails);
    else if(msg.method==='Runtime.consoleAPICalled'&&msg.params.type==='error')consoleErrors.push(msg.params.args);
    else if(msg.method==='Fetch.requestPaused'){
      const allowed=msg.params.request.url.startsWith(origin+'/');
      if(!allowed)external.push(msg.params.request.url);
      call(allowed?'Fetch.continueRequest':'Fetch.failRequest',{requestId:msg.params.requestId,...(allowed?{}:{errorReason:'BlockedByClient'})}).catch(()=>{});
    }
  }
});
function call(method,params={},session=sessionId){return new Promise((resolve,reject)=>{
  const id=++seq,timer=setTimeout(()=>{pending.delete(id);reject(new Error('CDP timeout '+method))},10000);
  pending.set(id,{resolve,reject,timer});browser.stdio[3].write(JSON.stringify({id,method,params,...(session?{sessionId:session}:{})})+'\0');
})}
async function evaluate(expression){const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw new Error(JSON.stringify(r.exceptionDetails));return r.result.value}
const pause=ms=>new Promise(r=>setTimeout(r,ms));
async function waitFor(expression){const until=Date.now()+7000;do{if(await evaluate(expression))return;await pause(50)}while(Date.now()<until);throw new Error('waitFor '+expression)}
async function screenshot(name){const r=await call('Page.captureScreenshot',{format:'png'});writeFileSync(path.join(out,name+'.png'),Buffer.from(r.data,'base64'))}
async function click(selector){
  const p=await evaluate(`(()=>{const e=document.querySelector(${JSON.stringify(selector)});if(!e)throw Error('missing click target');e.scrollIntoView({block:'center',inline:'nearest'});const r=e.getBoundingClientRect(),x=r.x+r.width/2,y=r.y+r.height/2;const hit=document.elementFromPoint(x,y);return {x,y,width:r.width,height:r.height,hit:hit===e||e.contains(hit),disabled:e.matches(':disabled')};})()`);
  assert.ok(p.width&&p.height&&p.hit&&!p.disabled,'clickable '+selector+JSON.stringify(p));
  await call('Input.dispatchMouseEvent',{type:'mousePressed',x:p.x,y:p.y,button:'left',clickCount:1});
  await call('Input.dispatchMouseEvent',{type:'mouseReleased',x:p.x,y:p.y,button:'left',clickCount:1});
}
const codes={Tab:9,Enter:13,Escape:27,Home:36,End:35,ArrowUp:38,ArrowDown:40,ArrowLeft:37,ArrowRight:39,Backspace:8,a:65};
async function key(key,modifiers=0){const params={key,code:key,windowsVirtualKeyCode:codes[key]||key.toUpperCase().charCodeAt(0),modifiers};await call('Input.dispatchKeyEvent',{type:'keyDown',...params});await call('Input.dispatchKeyEvent',{type:'keyUp',...params})}
async function text(selector,value){await click(selector);await key('a',2);await call('Input.insertText',{text:value});if(selector.endsWith('.cur-year'))await key('Enter');await key('Tab')}
async function select(selector,index){await click(selector);await key('Home');for(let n=0;n<index;n++)await key('ArrowDown');await key('Enter');if(args.includes('--trace'))console.log(selector,await evaluate("({active:document.activeElement?.tagName+'#'+document.activeElement?.id,year:document.querySelector('#lcInvestigationCalendar .cur-year')?.value})"))}

async function check(name,fn){const start=Date.now();try{const evidence=await fn();checks.push({name,status:'pass',ms:Date.now()-start,evidence});console.log('PASS '+name)}catch(e){checks.push({name,status:'fail',error:e.stack});throw e}}
const frtCount=()=>requests.filter(r=>r.path==='/model-statistics/frt').length;
let version,failure;
const watchdog=setTimeout(()=>browser.kill(),180000);
try{
 version=await call('Browser.getVersion',{},null);
 const {targetId}=await call('Target.createTarget',{url:'about:blank'},null);({sessionId}=await call('Target.attachToTarget',{targetId,flatten:true},null));
 await call('Page.enable');await call('Runtime.enable');await call('Fetch.enable',{patterns:[{urlPattern:'http*'}]});
 await call('Emulation.setDeviceMetricsOverride',{width:1280,height:950,deviceScaleFactor:1,mobile:false});
 await call('Emulation.setTimezoneOverride',{timezoneId:'America/New_York'});
 await call('Page.addScriptToEvaluateOnNewDocument',{source:"{const NativeDate=Date;window.Date=class extends NativeDate{constructor(...a){super(...(a.length?a:[Date.UTC(2026,9,16,8,23)]))}static now(){return NativeDate.UTC(2026,9,16,8,23)}}}"});
 await call('Page.navigate',{url:origin+'/#tab=model-statistics'});
 await waitFor("document.getElementById('msModels')?.innerText.includes('fixture-model')");
 await check('old page stays default, opens original model and group',async()=>{
  assert.equal(await evaluate("document.getElementById('msRequestsView').hidden"),false);
  await click('[data-ms-expand]');await click('[data-ms-group-key]');
  const oldContent=await evaluate("document.getElementById('msModels').innerText");assert.doesNotMatch(oldContent,/FRT|3.9秒|P95/);assert.match(oldContent,/渠道请求统计/);assert.match(oldContent,/fixture-channel/);assert.match(oldContent,/fixture-customer/);await screenshot('01-original');
 });
 await check('FRT subpage loads independent local API, click channel and actual group, leaf has no expander',async()=>{
  await click('#msFRTTab');await waitFor("document.querySelector('.frt-channel')!==null");const count=frtCount();
  await click('.frt-channel>summary');await click('.frt-group>summary');
  assert.equal(frtCount(),count);assert.equal(await evaluate("document.querySelector('.frt-model details')"),null);
  const content=await evaluate("document.getElementById('msFRTView').innerText");assert.match(content,/fixture-model/);assert.match(content,/3.9 秒/);assert.match(content,/16:20:00/);assert.match(content,/平均 FRT/);assert.match(content,/0.76 秒/);assert.match(content,/FRT 样本/);assert.match(content,/最大 FRT/);assert.doesNotMatch(content,/无可用渠道|已进入渠道|渠道命中比例|并发满|3 秒|排队/);
  await screenshot('02-frt-expanded');return {count,content};
 });
 await check('comparison uses exact original UTC cutoff; refresh retains pinned range',async()=>{
  await click('#frtCompare');await waitFor("document.getElementById('frtCoverage').innerText.includes('已固定为原统计窗口')");
  const req=requests.filter(r=>r.path==='/model-statistics/frt').at(-1);assert.equal(new URLSearchParams(req.query).get('to'),new Date(end*1000).toISOString());
  await click('#frtRefresh');await waitFor("document.getElementById('frtStatus').innerText.startsWith('响应生成于')");
  assert.equal(requests.filter(r=>r.path==='/model-statistics/frt').at(-1).query,req.query);return req;
 });
 await check('refresh navigation restores FRT and fixed window; return to latest and keyboard tabs work',async()=>{
  await call('Page.reload');await waitFor("document.getElementById('msFRTView')?.hidden===false && document.querySelector('.frt-channel')!==null");
  assert.equal(await evaluate("document.getElementById('frtCompare').disabled"),true);
  await click('#frtLatest');await waitFor("document.getElementById('frtLatest').hidden");
  await click('#msFRTTab');await key('ArrowLeft');await waitFor("!document.getElementById('msRequestsView').hidden");
  await key('ArrowRight');await waitFor("!document.getElementById('msFRTView').hidden && document.querySelector('.frt-channel')!==null");
 });
 await check('window refresh race and switch-away cancellation keep latest page state',async()=>{
  scenario='slow';await click('[data-frt-window="3d"]');scenario='complete';await click('[data-frt-window="7d"]');
  await waitFor("document.getElementById('frtTree').innerText.includes('fixture-7d')");await pause(1500);
  assert.match(await evaluate("document.getElementById('frtTree').innerText"),/fixture-7d/);
  scenario='slow';await click('#frtRefresh');await click('#msRequestsTab');await pause(1500);
  assert.equal(await evaluate("document.getElementById('msRequestsView').hidden"),false);scenario='complete';await click('#msFRTTab');await waitFor("document.querySelector('.frt-channel')!==null");
 });
 await check('errors retry, no samples and history gaps remain explicit',async()=>{
  scenario='error';await click('#frtRefresh');await waitFor("!document.getElementById('frtError').hidden");assert.match(await evaluate("document.getElementById('frtError').innerText"),/不可用/);
  scenario='empty';await click('#frtRefresh');await waitFor("document.getElementById('frtTree').innerText.includes('不能据此判断')");await screenshot('03-gap-empty');
  scenario='missing';await click('#frtRefresh');await waitFor("document.getElementById('frtTree').innerText.includes('历史口径不支持')");assert.doesNotMatch(await evaluate("document.getElementById('frtTree').innerText"),/0 秒/);
  scenario='complete';await click('#frtRefresh');await waitFor("document.querySelector('.frt-channel')!==null");assert.equal(await evaluate("document.getElementById('frtError').hidden"),true);
 });
 await check('collection failure is visible even with complete history and zero lag',async()=>{
  scenario='collection-failed';await click('#frtRefresh');
  await waitFor("document.getElementById('frtCoverage').innerText.includes('最近一次采集失败')");
  const coverage=await evaluate("document.getElementById('frtCoverage').innerText");
  assert.match(coverage,/本地覆盖已核验/);
  assert.doesNotMatch(coverage,/数据延迟\/覆盖不完整/);
  assert.equal(await evaluate("document.getElementById('frtCoverage').classList.contains('gap')"),true);
  await click('#msRequestsTab');await waitFor("document.getElementById('msCoverage').innerText.includes('最近一次采集失败')");
  assert.equal(await evaluate("document.getElementById('msCoverage').classList.contains('incomplete')"),true);
  scenario='complete';await click('#msRefresh');await waitFor("!document.getElementById('msCoverage').innerText.includes('最近一次采集失败')");
  await click('#msFRTTab');await waitFor("document.querySelector('.frt-channel')!==null");
 });
 await check('historical missing sum keeps percentiles and shows uncomputable mean',async()=>{
  scenario='missing-mean';await click('#frtRefresh');await waitFor("document.getElementById('frtTree').innerText.includes('真实耗时总和')");
  if(!await evaluate("document.querySelector('.frt-channel').open"))await click('.frt-channel>summary');
  if(!await evaluate("document.querySelector('.frt-group').open"))await click('.frt-group>summary');
  const content=await evaluate("document.querySelector('.frt-model').innerText");assert.match(content,/不可计算/);assert.match(content,/3.9 秒/);assert.doesNotMatch(content,/0 秒|比例/);await screenshot('06-missing-mean');
  scenario='complete';await click('#frtRefresh');await waitFor("!document.getElementById('frtTree').innerText.includes('不可计算')");return content;
 });
 await check('exact filter controls submit local GETs, survive reload and reset',async()=>{
  await text('#frtChannel','7');await text('#frtGroup','actual-group');await text('#frtModel','fixture-model');await click('#frtFilters button[type="submit"]');
  await waitFor("document.getElementById('frtCoverage').innerText.includes('筛选：渠道 #7')");
  const r=requests.filter(r=>r.path==='/model-statistics/frt').at(-1),q=new URLSearchParams(r.query);assert.equal(q.get('channel_id'),'7');assert.equal(q.get('group'),'actual-group');assert.equal(q.get('model'),'fixture-model');
  await call('Page.reload');await waitFor("document.getElementById('frtGroup')?.value==='actual-group' && document.querySelector('.frt-channel')!==null");
  await click('#frtReset');await waitFor("document.getElementById('frtGroup').value==='' && document.getElementById('frtTree').getAttribute('aria-busy')==='false' && !document.getElementById('frtCoverage').innerText.includes('筛选：')");assert.ok(!requests.filter(r=>r.path==='/model-statistics/frt').at(-1).query.includes('group='));return r;
 });

 await check('date controls on both pages use Beijing natural days and real calendar clicks',async()=>{
  async function chooseDay(prefix,field,n){
   await click('#'+prefix+'Dates [data-md-action="'+field+'"]');await waitFor("document.getElementById('modelDateDialog').open");
   const index=await evaluate("(()=>{const a=[...document.querySelectorAll('#modelDateCalendar .dayContainer .flatpickr-day')];return a.findIndex(e=>e.textContent==='"+n+"'&&!e.classList.contains('prevMonthDay')&&!e.classList.contains('nextMonthDay')&&!e.classList.contains('flatpickr-disabled'));})()");
   assert.ok(index>=0,'calendar day reachable');
   await click('#modelDateCalendar .dayContainer .flatpickr-day:nth-child('+(index+1)+')');await click('#modelDateConfirm');
   assert.equal(await evaluate("document.getElementById('"+prefix+"Date"+(field==='start'?'Start':'End')+"').value"),'2026-10-'+String(n).padStart(2,'0'));
  }
  for(const prefix of ['ms','frt']){
   await click(prefix==='ms'?'#msRequestsTab':'#msFRTTab');await waitFor("document.getElementById('"+prefix+"Status').innerText.startsWith('响应生成于')");
   const endpoint=prefix==='ms'?'/model-statistics/report':'/model-statistics/frt';
   await click('#'+prefix+'Dates [data-md-action="today"]');await waitFor("document.getElementById('"+prefix+"Coverage').innerText.includes('今日尚未结束')");
   let q=new URLSearchParams(requests.filter(r=>r.path===endpoint).at(-1).query);assert.equal(q.get('from'),'2026-10-15T16:00:00.000Z');assert.equal(q.get('to'),'2026-10-16T16:00:00.000Z');assert.equal(q.get('window'),null);
   await click('#'+prefix+'Dates [data-md-action="yesterday"]');await waitFor("document.getElementById('"+prefix+"Coverage').innerText.includes('2026-10-15 至 2026-10-15')");
   await chooseDay(prefix,'start',14);await chooseDay(prefix,'end',14);
   await click('#'+prefix+'Dates [data-md-action="single"]');await waitFor("document.getElementById('"+prefix+"Coverage').innerText.includes('2026-10-14 至 2026-10-14')");
   await click('#'+prefix+'Dates [data-md-action="previous"]');await waitFor("document.getElementById('"+prefix+"DateStart').value==='2026-10-13'");
   await click('#'+prefix+'Dates [data-md-action="next"]');await waitFor("document.getElementById('"+prefix+"DateStart').value==='2026-10-14'");
   await text('#'+prefix+'DateStart','2026-10-13');await text('#'+prefix+'DateEnd','2026-10-14');await click('#'+prefix+'Dates button[type="submit"]');
   await waitFor("document.getElementById('"+prefix+"Coverage').innerText.includes('2026-10-13 至 2026-10-14')");
   q=new URLSearchParams(requests.filter(r=>r.path===endpoint).at(-1).query);assert.equal(q.get('from'),'2026-10-12T16:00:00.000Z');assert.equal(q.get('to'),'2026-10-14T16:00:00.000Z');
   const saved=q.toString();await click(prefix==='ms'?'#msRefresh':'#frtRefresh');await waitFor("document.getElementById('"+prefix+"Status').innerText.startsWith('响应生成于')");assert.equal(new URLSearchParams(requests.filter(r=>r.path===endpoint).at(-1).query).toString(),saved);
   await screenshot('08-'+prefix+'-calendar-range');
  }
 });
 await check('custom dates and filters survive subpage navigation, reload and exact comparison',async()=>{
  await text('#frtChannel','7');await text('#frtGroup','actual-group');await text('#frtModel','fixture-model');await click('#frtFilters button[type="submit"]');
  await waitFor("document.getElementById('frtCoverage').innerText.includes('筛选：渠道 #7')");
  const saved=requests.filter(r=>r.path==='/model-statistics/frt').at(-1).query;
  await click('#msRequestsTab');await waitFor("document.getElementById('msCoverage').innerText.includes('2026-10-13 至 2026-10-14')");
  await click('#msFRTTab');await waitFor("document.querySelector('.frt-channel')!==null");assert.equal(requests.filter(r=>r.path==='/model-statistics/frt').at(-1).query,saved);
  await click('#frtCompare');await waitFor("document.getElementById('frtCoverage').innerText.includes('已固定为原统计窗口')");
  const q=new URLSearchParams(requests.filter(r=>r.path==='/model-statistics/frt').at(-1).query);assert.equal(q.get('from'),'2026-10-12T16:00:00.000Z');assert.equal(q.get('to'),'2026-10-14T16:00:00.000Z');assert.equal(q.get('range'),'exact');assert.equal(q.get('window'),null);
  await call('Page.reload');await waitFor("document.getElementById('frtGroup')?.value==='actual-group' && document.querySelector('.frt-channel')!==null");assert.equal(new URLSearchParams(requests.filter(r=>r.path==='/model-statistics/frt').at(-1).query).get('from'),q.get('from'));
 });
 await check('date change races, source behind yesterday and failure leave editable inputs',async()=>{
  for(const prefix of ['ms','frt']){
   await click(prefix==='ms'?'#msRequestsTab':'#msFRTTab');await waitFor("document.getElementById('"+prefix+"Status').innerText.startsWith('响应生成于')");
   scenario='slow';await click('#'+prefix+'Dates [data-md-action="yesterday"]');scenario='complete';await click('#'+prefix+'Dates [data-md-action="today"]');
   await waitFor("document.getElementById('"+prefix+"Coverage').innerText.includes('今日尚未结束')");await pause(1500);assert.equal(await evaluate("document.getElementById('"+prefix+"DateStart').value"),'2026-10-16');
   scenario='stale-source';await click(prefix==='ms'?'#msRefresh':'#frtRefresh');await waitFor("document.getElementById('"+prefix+"Coverage').innerText.includes('尚无可用区间')");
   const content=await evaluate("document.getElementById('"+(prefix==='ms'?'msSummary':'frtTree')+"').innerText");assert.match(content,/不能据此/);await screenshot('09-'+prefix+'-unavailable-day');
   scenario='error';await click(prefix==='ms'?'#msRefresh':'#frtRefresh');await waitFor("!document.getElementById('"+prefix+"Error').hidden");
   assert.equal(await evaluate("document.getElementById('"+prefix+"DateStart').disabled||document.getElementById('"+prefix+"DateEnd').disabled"),false);
   scenario='complete';await click(prefix==='ms'?'#msRefresh':'#frtRefresh');await waitFor("document.getElementById('"+prefix+"Status').innerText.startsWith('响应生成于')");
  }
 });
 await check('anomaly monitor is a top-level page; Token local report calls only local report and health APIs',async()=>{
  const count=requests.length;await click('.monitor-nav [data-tab="anomalies"]');await waitFor("!document.getElementById('tab-anomalies').hidden");
  await waitFor("document.getElementById('tokenAnomalyView').innerText.includes('异常候选，待核查')");
  assert.equal(await evaluate("document.getElementById('monitorPageTitle').innerText"),'异常监控');assert.match(await evaluate("document.getElementById('tokenAnomalyView').innerText"),/异常候选，待核查/);
  assert.ok(requests.length>count);assert.ok(requests.slice(count).every(r=>r.path.startsWith('/anomalies/token/')));await screenshot('04-token-local');
 });
 await check('dark 320 and 375px: mobile navigation, subpages and every FRT metric stay reachable',async()=>{
  const sizes=[];
  for(const width of [320,375]){
   await call('Emulation.setDeviceMetricsOverride',{width,height:820,deviceScaleFactor:1,mobile:false});
   await click('.monitor-mobile-tabs [data-tab="model-statistics"]');await click('#msFRTTab');await waitFor("document.querySelector('.frt-channel')!==null");
   if(!await evaluate("document.querySelector('.frt-channel').open"))await click('.frt-channel>summary');
   if(!await evaluate("document.querySelector('.frt-group').open"))await click('.frt-group>summary');
   const layout=await evaluate("({width:innerWidth,scroll:document.documentElement.scrollWidth,metrics:[...document.querySelectorAll('.frt-model .frt-values dd')].map(e=>{const r=e.getBoundingClientRect();return {text:e.innerText,left:r.left,right:r.right}})})");
   assert.ok(layout.scroll<=width,JSON.stringify(layout));assert.ok(layout.metrics.length===7&&layout.metrics.every(m=>m.left>=0&&m.right<=width),JSON.stringify(layout));sizes.push(layout);await screenshot('05-narrow-'+width);
   await click('#msRequestsTab');await waitFor("!document.getElementById('msRequestsView').hidden && document.querySelector('[data-ms-expand]')!==null");
   if(await evaluate("document.querySelector('[data-ms-expand]').getAttribute('aria-expanded')!=='true'"))await click('[data-ms-expand]');
   if(await evaluate("document.querySelector('.ms-group-expand').getAttribute('aria-expanded')!=='true'"))await click('.ms-group-expand');
   assert.equal(await evaluate("document.querySelector('.ms-model-table-wrap>table>thead>tr').children.length"),5);
   assert.match(await evaluate("document.getElementById('msModels').innerText"),/fixture-customer/);
   const oldLayout=await evaluate("({width:innerWidth,scroll:document.documentElement.scrollWidth,overflow:getComputedStyle(document.querySelector('.ms-model-table-wrap')).overflowX})");
   assert.ok(oldLayout.scroll<=width && oldLayout.overflow==='auto',JSON.stringify(oldLayout));await screenshot('07-old-narrow-'+width);
   await click('#msDates [data-md-action="start"]');await waitFor("document.getElementById('modelDateDialog').open");
   const calendarLayout=await evaluate("(()=>{const r=document.getElementById('modelDateDialog').getBoundingClientRect();return {left:r.left,right:r.right,width:innerWidth}})()");assert.ok(calendarLayout.left>=0&&calendarLayout.right<=calendarLayout.width,JSON.stringify(calendarLayout));assert.equal(await evaluate("getComputedStyle(document.getElementById('modelDateCalendarInput')).display"),'none');await screenshot('10-date-calendar-'+width);await click('#modelDateCancel');
   await click('.monitor-mobile-tabs [data-tab="anomalies"]');await click('#tokenAnomalyTab');assert.equal(await evaluate("document.getElementById('tab-anomalies').hidden"),false);
  }return sizes;
 });
 assert.ok(requests.filter(r=>r.method!=='GET').every(r=>r.method==='POST'&&r.path==='/anomalies/token/detection/query'));assert.equal(external.length,0);assert.equal(exceptions.length,0,JSON.stringify(exceptions));
}catch(e){failure=e;try{await screenshot('99-failure')}catch{}console.error(e.stack);process.exitCode=1}
finally{
 clearTimeout(watchdog);
 writeFileSync(path.join(out,'browser-results.json'),JSON.stringify({scope:'Complete current HTML/JS/CSS, real Chrome mouse and keyboard; loopback synthetic APIs only, no 8204/production.',browser:version?.product,profile,checks,requests,exceptions,external,consoleErrors,failure:failure?.stack},null,2));
 try{await call('Browser.close',{},null)}catch{browser.kill()}
 for(const p of pending.values())clearTimeout(p.timer);server.closeAllConnections();server.close();
}
