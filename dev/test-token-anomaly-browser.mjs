// Real Chrome clicks and keyboard against repository assets and loopback-only fixtures.
// Usage: node dev/test-token-anomaly-browser.mjs --browser <chrome> --out <D-drive directory>
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,readdirSync,rmSync} from 'node:fs';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {once} from 'node:events';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {epoch,at,access,health,detectionHealth,notifyHealth,report,detail,receipt} from './token-anomaly-fixture.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const args=process.argv.slice(2),option=name=>args[args.indexOf(name)+1];
assert.ok(args.includes('--browser')&&args.includes('--out'));
const out=path.resolve(option('--out'));mkdirSync(out,{recursive:true});
const scratch=path.resolve(args.includes('--scratch')?option('--scratch'):out);mkdirSync(scratch,{recursive:true});
const profile=mkdtempSync(path.join(scratch,'profile-')),browserTemp=mkdtempSync(path.join(scratch,'temp-')),downloads=mkdtempSync(path.join(scratch,'downloads-'));
const html=readFileSync(path.join(root,'monitor/page.html'),'utf8'),embedSource=readFileSync(path.join(root,'monitor/server.go'),'utf8');
const embedMap=new Map([...embedSource.matchAll(/\/\/go:embed ([^\r\n]+)\r?\nvar (\w+)/g)].map(m=>[m[2],m[1]]));
const assets=new Map([...embedSource.matchAll(/r\.GET\("([^"\n]+\.(?:js|css))",[\s\S]*?c\.Data\(http\.StatusOK, "[^"]+", (?:\[\]byte\()?([\w]+)\)?\)/g)].map(m=>[m[1],embedMap.get(m[2])]));
const requests=[],checks=[],exceptions=[],external=[],consoleErrors=[];
let scenario='complete';
let autoGeneration=0;
const localAssessment={state:'local_metering_difference',evidence_level:'exact',local_difference_confirmed:true,upstream_overcharge_confirmed:false,upstream_reason:'upstream_billing_evidence_missing',missing_fields:[],request_length_bytes:'1000',request_length_meaning:'whole_http_request_not_body_or_token_count',input_tokens:'10000',output_tokens:'2',model:'fixture-model',request_path:'/v1/messages',normal_quota_from:'9007199254740992',normal_quota_to:'9007199254740992',actual_quota:'9007199254740993',formula_difference_quota:'1',reference_input_upper:null,reference_input_difference:null};
let diagnosisStatus='pending_delivery',diagnosisTask=0;
const json=(res,status,v)=>{res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(v))};
const server=createServer(async(req,res)=>{
 const url=new URL(req.url,'http://127.0.0.1'),mode=scenario;
 let raw='';for await(const chunk of req)raw+=chunk;let body={};try{if(raw)body=JSON.parse(raw)}catch{json(res,400,{});return}
 requests.push({method:req.method,path:url.pathname,query:url.search,body,mode});
 if(url.pathname==='/'){res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});res.end(html);return}
 if(assets.has(url.pathname)){res.writeHead(200,{'Content-Type':url.pathname.endsWith('.css')?'text/css':'application/javascript'});res.end(readFileSync(path.join(root,'monitor',assets.get(url.pathname))));return}
 if(url.pathname==='/me'){json(res,200,{name:'Local synthetic browser',is_root:true});return}
 if(url.pathname==='/api/brand'){json(res,200,{name:'Local synthetic review'});return}
 if(url.pathname==='/data'){json(res,200,{enabled:false});return}
 const p=url.pathname.replace('/anomalies/token/','');
 if(p==='detection/query'){
  if(mode==='slow')await new Promise(r=>setTimeout(r,700));
  if(mode==='error'){json(res,503,{error:'synthetic local query unavailable'});return}
  const r=report(body,mode==='slow'?'stale-response':'fixture-model');
  if(mode==='refresh'){
   if(!body.after_id)autoGeneration++;
   r.generated_at+=autoGeneration*60;r.source_complete_through+=autoGeneration*60;
  }
  if(mode==='assessment')for(const item of r.items){item.display.assessment=localAssessment;item.result.source_complete=true}
  if(mode==='slow')for(const item of r.items)item.display.customer_name='stale-response';
  if(mode==='empty'){r.items=[];r.has_more=false;r.summary.candidate_requests=0;r.summary.observed_results=0;r.analysis.status_counts.candidate=0}
  json(res,200,r);return
 }
 if(p==='health'){json(res,200,mode==='refresh'?{...health,state:{...health.state,scan_through:health.state.scan_through+autoGeneration*60}}:health);return}
 if(p==='detection/health'){json(res,200,detectionHealth);return}
 if(p==='notifications/health'){json(res,200,notifyHealth);return}
 if(p==='access'){json(res,200,mode==='disabled'?{...access,export_enabled:false}:access);return}
 if(p==='detection/detail'){const d=detail(body.revision);if(mode==='assessment'){d.assessment=localAssessment;d.evidence.result.source_complete=true;d.evidence.input.usage.fields.completion_tokens={state:'valid',value:'2'}}json(res,200,d);return}
 if(p==='exports'){
  assert.equal(body.source_epoch,epoch);
  assert.equal(req.headers['x-monitor-token-csrf'],access.csrf_token);
  if(mode==='slow-export')await new Promise(r=>setTimeout(r,600));
  json(res,200,receipt);return
 }
 if(p.endsWith('/download')){
  assert.equal(req.headers['x-monitor-token-csrf'],access.csrf_token);
  res.writeHead(200,{'Content-Type':'application/zip','Cache-Control':'no-store'});
  const b=Buffer.alloc(22);b.write('PK\x05\x06');res.end(b);return
 }
 if(url.pathname==='/model-statistics/report'){
  json(res,200,{window:'24h',from_ts:at-86400,to_ts:at,generated_at:at,source:{facts_complete:false,note:'synthetic history gap'},models:[]});return
 }
 if(url.pathname==='/logchain/filters'){json(res,200,{groups:[],models:[],channels:[]});return}
 if(url.pathname==='/logchain/requests'){
  json(res,200,{rows:[{id:10,created_at:at+10,type:5,request_id:'fixture-request',user_id:2,username:'fixture-customer',channel_id:3,channel_name:'fixture-channel',model_name:'fixture-model',content:'fixture rejection'}],has_more:false,generated_at:at+120,query_through_ts:at+60,coverage_complete:true,cloudwatch_enabled:true});return;
 }
 if(url.pathname==='/logchain/investigations'){diagnosisTask++;diagnosisStatus='pending_delivery';json(res,200,{investigation_id:'fixture-task-'+diagnosisTask,poll_after_ms:800});return}
 if(url.pathname.startsWith('/logchain/investigations/')){if(url.pathname.endsWith('/cancel'))diagnosisStatus='cancelled';json(res,200,{status:diagnosisStatus,source_status:[],summary:{conclusion:'fixture evidence retained'}});return}
 json(res,200,{});
});
server.listen(0,'127.0.0.1');await once(server,'listening');const origin='http://127.0.0.1:'+server.address().port;
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
async function waitFor(expression,timeout=7000){const until=Date.now()+timeout;do{if(await evaluate(expression))return;await pause(50)}while(Date.now()<until);throw new Error('waitFor '+expression)}
async function screenshot(name){const r=await call('Page.captureScreenshot',{format:'png'});writeFileSync(path.join(out,name+'.png'),Buffer.from(r.data,'base64'))}
async function click(selector){
  for(let i=0;i<6;i++){
    const closed=await evaluate(`(()=>{const e=document.querySelector(${JSON.stringify(selector)});let p=e?.tagName==='SUMMARY'?e.parentElement?.parentElement:e?.parentElement,a=[];while(p){if(p.tagName==='DETAILS'&&!p.open)a.push(p);p=p.parentElement}if(!a.length)return null;const s=a.at(-1).querySelector('summary');s.scrollIntoView({block:'center'});const r=s.getBoundingClientRect();return{x:r.x+r.width/2,y:r.y+r.height/2}})()`);
    if(!closed)break;
    for(const type of ['mousePressed','mouseReleased'])await call('Input.dispatchMouseEvent',{type,...closed,button:'left',clickCount:1});
  }
  const p=await evaluate(`(()=>{const e=document.querySelector(${JSON.stringify(selector)});if(!e)throw Error('missing click target');e.scrollIntoView({block:'center',inline:'nearest'});const r=e.getBoundingClientRect(),x=r.x+r.width/2,y=r.y+r.height/2;const hit=document.elementFromPoint(x,y);return {x,y,width:r.width,height:r.height,hit:hit===e||e.contains(hit),disabled:e.matches(':disabled')};})()`);
  assert.ok(p.width&&p.height&&p.hit&&!p.disabled,'clickable '+selector+JSON.stringify(p));
  await call('Input.dispatchMouseEvent',{type:'mousePressed',x:p.x,y:p.y,button:'left',clickCount:1});
  await call('Input.dispatchMouseEvent',{type:'mouseReleased',x:p.x,y:p.y,button:'left',clickCount:1});
}
const codes={Tab:9,Enter:13,Escape:27,Home:36,End:35,ArrowUp:38,ArrowDown:40,ArrowLeft:37,ArrowRight:39,Backspace:8,a:65};
async function key(key,modifiers=0){const params={key,code:key,windowsVirtualKeyCode:codes[key]||key.toUpperCase().charCodeAt(0),modifiers};await call('Input.dispatchKeyEvent',{type:'keyDown',...params});await call('Input.dispatchKeyEvent',{type:'keyUp',...params})}
async function text(selector,value){await click(selector);await key('a',2);await call('Input.insertText',{text:value});if(selector.endsWith('.cur-year'))await key('Enter');await key('Tab')}
async function select(selector,index){await click(selector);await key('Home');for(let n=0;n<index;n++)await key('ArrowDown');await key('Enter');if(args.includes('--trace'))console.log(selector,await evaluate("({active:document.activeElement?.tagName+'#'+document.activeElement?.id,year:document.querySelector('#lcInvestigationCalendar .cur-year')?.value})"))}

async function check(name,fn){if(args.includes('--tasks-only')&&!/diagnosis|prefill|pending_delivery|active task/.test(name)||args.includes('--token-only')&&/pending_delivery|active task/.test(name)||args.includes('--evidence-only')&&!/local formula confirmation/.test(name)){checks.push({name,status:'not_tested',reason:'selected scope'});return}const start=Date.now();try{const evidence=await fn();checks.push({name,status:'pass',ms:Date.now()-start,evidence});console.log('PASS '+name)}catch(e){checks.push({name,status:'fail',error:e.stack});throw e}}
let version,failure;
const watchdog=setTimeout(()=>browser.kill(),420000);
try {
 version=await call('Browser.getVersion',{},null);
 await call('Browser.setDownloadBehavior',{behavior:'allow',downloadPath:downloads},null);
 const {targetId}=await call('Target.createTarget',{url:'about:blank'},null);({sessionId}=await call('Target.attachToTarget',{targetId,flatten:true},null));
 await call('Page.enable');await call('Runtime.enable');await call('Fetch.enable',{patterns:[{urlPattern:'http*'}]});
 await call('Emulation.setDeviceMetricsOverride',{width:1280,height:950,deviceScaleFactor:1,mobile:false});
 await call('Emulation.setTimezoneOverride',{timezoneId:'America/New_York'});
 await call('Page.addScriptToEvaluateOnNewDocument',{source:"{const NativeDate=Date;window.Date=class extends NativeDate{constructor(...a){super(...(a.length?a:[1792138980000]))}static now(){return 1792138980000}}}"});
 await call('Page.navigate',{url:origin+'/#tab=anomalies'});
 await waitFor("document.querySelector('[data-ta-detail]')!==null");
 await check('independent Token page, disabled sources, candidates and precision',async()=>{
  const content=await evaluate("document.getElementById('tokenAnomalyView').innerText");
  assert.match(content,/异常候选，待核查/);assert.match(content,/采集未启用/);assert.match(content,/客户/);assert.match(content,/异常数量/);
  assert.match(content,/Request ID: fixture-request-10/);assert.match(content,/source log: 10/);assert.match(content,/upstream_request_id: source_missing/);assert.match(content,/\/v1\/messages/);assert.match(content,/1,000 字节/);
  assert.equal(await evaluate("document.querySelector('#taList').closest('details')===null"),true);
  assert.equal(await evaluate("[...document.querySelectorAll('details.ta-panel')].every(e=>!e.open)"),true);
  await screenshot('01-token');
 });
 await check('visible calendar, actual date/hour/minute clicks, keyboard and UTC submission',async()=>{
  const before=await evaluate("document.getElementById('taFrom').value");
  await click('[data-ta-picker="taFrom"]');await waitFor("document.getElementById('taTimeDialog').open");
  const day=await evaluate("(()=>{const a=[...document.querySelectorAll('#taCalendar .flatpickr-day')];return '#taCalendar .flatpickr-day:nth-child('+(a.findIndex(e=>e.textContent==='14'&&!e.classList.contains('prevMonthDay')&&!e.classList.contains('nextMonthDay'))+1)+')'})()");
  await click(day);await select('#taHour',15);await select('#taMinute',20);await screenshot('00-time-picker');await click('#taTimeApply');
  const after=await evaluate("document.getElementById('taFrom').value");assert.equal(after,'2026-10-14T15:20');assert.notEqual(after,before);
  await click('[data-ta-picker="taTo"]');await waitFor("document.getElementById('taTimeDialog').open");await select('#taHour',17);await select('#taMinute',30);await click('#taTimeApply');
  await text('#taModel','fixture-model');await click('#taFilters button[type="submit"]');
  await waitFor("document.getElementById('taUpdated').innerText.includes('响应生成于')");
  const q=requests.filter(r=>r.path.endsWith('detection/query')).at(-1).body;
  const values=await evaluate("({from:document.getElementById('taFrom').value,to:document.getElementById('taTo').value})");
  assert.equal(q.from,Date.parse(values.from+':00+08:00')/1000);assert.equal(q.to,Date.parse(values.to+':00+08:00')/1000);return {before,after,submitted:q};
 });
 await check('pagination, amount and dimension drilldown stay local',async()=>{
  await click('#taNext');await waitFor("document.querySelectorAll('[data-ta-detail]').length===2");
  await click('[data-ta-amount]');await waitFor("document.getElementById('taScope').innerText.includes('customer_wallet')");
  const money=requests.filter(r=>r.path.endsWith('detection/query')).at(-1).body;assert.equal(money.method,'historical_formula_v1');assert.equal(money.unit,'USD');
  await select('#taDimension',3);await click('[data-ta-dimension]');await waitFor("document.getElementById('taScope').innerText.includes('#2')");
  assert.equal(requests.filter(r=>r.path.endsWith('detection/query')).at(-1).body.user_id,2);
  await screenshot('02-drilldown');
 });
 await check('frozen detail and first revision, no source query on expand',async()=>{
  await click('[data-ta-detail]');await waitFor("document.getElementById('taDetailBody').innerText.includes('为什么命中')");
  await click('#taOriginal');await waitFor("document.getElementById('taDetailBody').innerText.includes(' / 1')");
  await click('#taDetailBody details:nth-of-type(2)>summary');
  const c=await evaluate("document.getElementById('taDetailBody').innerText");assert.match(c,/不是纯正文/);assert.match(c,/null/);assert.match(c,/缺失/);assert.match(c,/不可计算/);
  assert.match(c,/fixture-request-10/);assert.match(c,/upstream_request_id:source_missing/);assert.match(c,/fixture-epoch:10/);assert.doesNotMatch(c,/unique \/ pending/);
  await screenshot('03-frozen-detail');await click('#taDetailClose');
 });
 await check('real 30 second Token refresh retains pages, focus, range and frozen detail',async()=>{
  scenario='refresh';await click('#taRefresh');await waitFor("document.getElementById('taList').getAttribute('aria-busy')==='false'");
  if(await evaluate("!document.getElementById('taNext').disabled")){await click('#taNext');await waitFor("document.querySelectorAll('[data-ta-detail]').length===2")}
  await click('[data-ta-detail]');await waitFor("document.getElementById('taDetail').open&&document.getElementById('taDetailBody').innerText.includes('为什么命中')");
  await evaluate("document.getElementById('taOriginal').focus({preventScroll:true})");
  const snapshot="({range:[document.getElementById('taFrom').value,document.getElementById('taTo').value,document.getElementById('taModel').value],rows:document.querySelectorAll('[data-ta-detail]').length,focus:document.activeElement.id,scroll:scrollY,open:document.getElementById('taDetail').open,detail:document.getElementById('taDetailBody').innerHTML})";
  const before=await evaluate(snapshot),updated=await evaluate("document.getElementById('taUpdated').textContent"),start=requests.length;
  const generations=autoGeneration;await waitFor("document.getElementById('taUpdated').textContent!=="+JSON.stringify(updated)+"&&document.getElementById('taList').getAttribute('aria-busy')==='false'",35000);
  await pause(100);const after=await evaluate(snapshot);assert.deepEqual(after,before);assert.ok(autoGeneration>generations);
  const refreshed=requests.slice(start);for(const p of ['health','detection/health','detection/query'])assert.ok(refreshed.some(r=>r.path==='/anomalies/token/'+p));
  // The host dashboard also polls its existing local overview. Token refresh
  // must not add any production log/query endpoint to that baseline.
  assert.ok(refreshed.every(r=>r.path.startsWith('/anomalies/token/')||['/data','/trend/long'].includes(r.path)));assert.ok(refreshed.some(r=>r.body.after_id===10));
  await screenshot('08-auto-refresh');await click('#taDetailClose');scenario='complete';return {before,after,refreshed:refreshed.map(r=>r.path),autoGeneration};
 });
 await check('hidden browser tab stops Token polling and resumes on return',async()=>{
  const other=await call('Target.createTarget',{url:origin+'/visibility-fixture'},null);
  try{
   await call('Target.activateTarget',{targetId:other.targetId},null);await waitFor('document.hidden');
   await pause(200);const before=requests.filter(r=>r.path.startsWith('/anomalies/token/')).length;
   await pause(31500);assert.equal(requests.filter(r=>r.path.startsWith('/anomalies/token/')).length,before);
   await call('Target.activateTarget',{targetId},null);await waitFor('!document.hidden');
   const until=Date.now()+5000;while(Date.now()<until&&requests.filter(r=>r.path.startsWith('/anomalies/token/')).length===before)await pause(50);
   assert.ok(requests.filter(r=>r.path.startsWith('/anomalies/token/')).length>before);return {hiddenRequests:0};
  }finally{await call('Target.closeTarget',{targetId:other.targetId},null);await call('Target.activateTarget',{targetId},null)}
 });
 await check('local formula confirmation is distinct from upstream billing truth',async()=>{
  scenario='assessment';await click('#taRefresh');await waitFor("document.getElementById('taList').innerText.includes('本地计量差异已核实')");
  await click('[data-ta-detail]');await waitFor("document.querySelector('.ta-assessment')!==null");
  const content=await evaluate("document.querySelector('.ta-assessment').innerText");
  for(const expected of ['上游真实 Token 与扣账未确认','不是正文大小或输入 Token','9007199254740993','9007199254740992','具体证据缺口'])assert.ok(content.includes(expected));
  assert.doesNotMatch(await evaluate("document.querySelector('#taDetailBody>.ta-warning').innerText"),/异常候选，待核查/);
  await screenshot('09-evidence-level');await click('#taDetailClose');scenario='complete';
 });
 await check('controlled selection, frozen export and actual local download',async()=>{
  await click('[data-ta-select]');await select('#taTarget',1);await click('#taExportConfirm');await click('#taCreateExport');
  await waitFor("!document.getElementById('taDownloadExport').disabled");
  const req=requests.filter(r=>r.path==='/anomalies/token/exports').at(-1).body;assert.equal(req.target_id,'supplier-fixture');assert.equal(req.selection.length,1);assert.equal(req.selection[0].revision,2);assert.equal(req.include_customer_pricing,false);
  await click('#taDownloadExport');await waitFor("document.getElementById('taExportResult').innerText.includes('已下载')");await pause(250);
  assert.ok(readdirSync(downloads).some(n=>n.endsWith('.zip')));await screenshot('04-export');return req;
 });
 await check('export target changed in flight cannot expose obsolete download',async()=>{
  scenario='slow-export';await click('#taCreateExport');await select('#taTarget',2);await pause(850);assert.equal(await evaluate("document.getElementById('taDownloadExport').disabled"),true);scenario='complete';
 });
 await check('query errors, empty and rapid refresh keep controls usable',async()=>{
  scenario='error';await click('#taRefresh');await waitFor("!document.getElementById('taError').hidden");assert.equal(await evaluate("document.getElementById('taFrom').disabled"),false);
  scenario='empty';await click('#taRefresh');await waitFor("document.getElementById('taList').innerText.includes('采集未启用')");
  scenario='slow';await click('#taRefresh');await pause(40);scenario='complete';await click('#taRefresh');await pause(900);
  assert.doesNotMatch(await evaluate("document.getElementById('taList').innerText"),/stale-response/);
  assert.match(await evaluate("document.getElementById('taList').innerText"),/测试客户/);
 });
 await check('old model page switch and return keep Token filters and lifecycle',async()=>{
  await click('.monitor-sidebar [data-tab="model-statistics"]');await waitFor("!document.getElementById('tab-model-statistics').hidden");
  await click('.monitor-sidebar [data-tab="anomalies"]');await waitFor("document.querySelector('[data-ta-detail]')!==null");
  assert.equal(await evaluate("document.getElementById('taModel').value"),'fixture-model');
 });
 await check('dark and narrow, scrollable tables and clickable details',async()=>{
  const sizes=[];
  for(const width of [390,320]){
   await call('Emulation.setDeviceMetricsOverride',{width,height:850,deviceScaleFactor:1,mobile:false});
   await evaluate("document.body.setAttribute('theme-mode','dark')");
   const layout=await evaluate("({width:innerWidth,scroll:document.documentElement.scrollWidth,color:getComputedStyle(document.getElementById('taFrom')).color,background:getComputedStyle(document.getElementById('taFrom')).backgroundColor,scrollX,scrolls:[...document.querySelectorAll('body *')].filter(e=>e.clientWidth&&e.scrollWidth>e.clientWidth+3&&!e.closest('.ta-table')).map(e=>({id:e.id,tag:e.tagName,class:e.className,w:e.clientWidth,sw:e.scrollWidth,overflow:getComputedStyle(e).overflowX})),offenders:[...document.querySelectorAll('body *')].filter(e=>!e.closest('.ta-table')&&e.getBoundingClientRect().right+scrollX>innerWidth).map(e=>({id:e.id,tag:e.tagName,class:e.className,width:e.getBoundingClientRect().width,text:e.textContent.slice(0,30)}))})");
   assert.ok(layout.scroll<=width,JSON.stringify(layout));assert.notEqual(layout.color,layout.background);
   await click('[data-ta-picker="taFrom"]');await waitFor("document.getElementById('taTimeDialog').open");await click('#taTimeCancel');await click('[data-ta-picker="taTo"]');await click('#taTimeCancel');
   await click('[data-ta-detail]');await waitFor("document.getElementById('taDetail').open");
   await screenshot('05-detail-'+width);await click('#taDetailClose');await screenshot('06-page-'+width);sizes.push(layout);
  }return sizes;
 });
 await check('customer diagnosis is explicit local prefill without a source scan',async()=>{
  await click('[data-ta-detail]');await waitFor("document.getElementById('taDetail').open");await click('#taTroubleshoot');
  await waitFor("!document.getElementById('tab-logchain').hidden");
  assert.equal(requests.filter(r=>r.path==='/logchain/requests').length,0);
  assert.match(await evaluate("document.getElementById('lcStatus').innerText"),/未查询生产日志/);return {productionLogQueries:0};
 });
 await check('Token prefill remains zero-query across real timers, focus and page reactivation',async()=>{
  await call('Emulation.setDeviceMetricsOverride',{width:1280,height:950,deviceScaleFactor:1,mobile:false});
  await evaluate("window.dispatchEvent(new Event('focus'));document.dispatchEvent(new Event('visibilitychange'))");
  await click('.monitor-sidebar [data-tab="model-statistics"]');await click('.monitor-sidebar [data-tab="logchain"]');
  await pause(32000);
  assert.equal(requests.filter(r=>r.path==='/logchain/requests').length,0);
  await click('#lcApply');await waitFor("document.querySelector('tr[data-lc-id]')!==null");
  return {submittedQueries:requests.filter(r=>r.path==='/logchain/requests').length};
 });
 await check('pending_delivery survives multiple real refresh cycles and completes with evidence',async()=>{
  await click('tr[data-lc-id="10"]');await click('[data-lc-cw="10"]');
  await waitFor("document.getElementById('lcTableBody').innerText.includes('等待 CloudFront 日志投递')");
  const before=requests.filter(r=>r.path==='/logchain/requests').length;
  await pause(70000);
  assert.equal(requests.filter(r=>r.path==='/logchain/requests').length,before);
  assert.equal(await evaluate("document.querySelector('[data-lc-cw-cancel=\"10\"]')!==null"),true);
  assert.ok(requests.filter(r=>r.path==='/logchain/investigations/fixture-task-1').length>=2);
  diagnosisStatus='complete';await waitFor("document.getElementById('lcTableBody').innerText.includes('已完成')",65000);
  await click('tr[data-lc-id="10"]');await evaluate("window.dispatchEvent(new Event('focus'))");await pause(500);
  await click('tr[data-lc-id="10"]');assert.match(await evaluate("document.getElementById('lcTableBody').innerText"),/已完成/);
  await screenshot('07-task-complete');
 });
 await check('active task blocks filter replacement, stays cancellable and releases after cancel',async()=>{
  await click('[data-lc-cw="10"]');await waitFor("document.querySelector('[data-lc-cw-cancel=\"10\"]')!==null");
  const before=requests.filter(r=>r.path==='/logchain/requests').length;
  await select('#lcStream',1);await pause(200);
  assert.equal(requests.filter(r=>r.path==='/logchain/requests').length,before);
  assert.match(await evaluate("document.getElementById('lcError').innerText"),/完成或取消/);
  await click('[data-lc-cw-cancel="10"]');await waitFor("document.getElementById('lcTableBody').innerText.includes('已取消')");
  await select('#lcStream',1);await pause(200);assert.ok(requests.filter(r=>r.path==='/logchain/requests').length>before);
 });
 assert.equal(external.length,0);assert.equal(exceptions.length,0,JSON.stringify(exceptions));
 assert.ok(requests.filter(r=>r.method==='POST').every(r=>r.path.startsWith('/anomalies/token/')||r.path.startsWith('/logchain/investigations')));
}catch(e){failure=e;try{await screenshot('99-failure')}catch{}console.error(e.stack);process.exitCode=1}
finally{
 clearTimeout(watchdog);
 writeFileSync(path.join(out,'browser-results.json'),JSON.stringify({scope:'Repository page/JS/CSS; real Chrome mouse and keyboard, synthetic loopback API. No 8204, production, real SMTP or source scans.',browser:version?.product,profile,checks,requests,exceptions,external,consoleErrors,failure:failure?.stack},null,2));
 try{await call('Browser.close',{},null)}catch{browser.kill()}
 for(const p of pending.values())clearTimeout(p.timer);server.closeAllConnections();server.close();
 for(let i=0;i<50&&browser.exitCode===null;i++)await pause(100);
 if(browser.exitCode===null)browser.kill();
 for(const dir of [profile,browserTemp,downloads]){if(!path.resolve(dir).startsWith(scratch+path.sep))throw Error('Cleanup scope mismatch');rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:250})}
}
