// Browser regression against the actual embedded page, with synthetic local APIs.
// Timers use their real 30/60 second intervals; --quick is development-only.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {createHash} from 'node:crypto';
import {browserSession} from './browser-session.mjs';
import * as token from './token-anomaly-fixture.mjs';
const args=process.argv.slice(2),opt=k=>args[args.indexOf(k)+1];
assert.ok(args.includes('--browser')&&args.includes('--out')&&args.includes('--scratch'));
const out=path.resolve(opt('--out'));fs.mkdirSync(out,{recursive:true});const root=process.cwd();
const driverSHA256=createHash('sha256').update(fs.readFileSync(new URL(import.meta.url))).digest('hex');
const embed=fs.readFileSync('monitor/server.go','utf8'),map=new Map([...embed.matchAll(/\/\/go:embed ([^\r\n]+)\r?\nvar (\w+)/g)].map(m=>[m[2],m[1]]));
const assets=new Map([...embed.matchAll(/r\.GET\("([^"\n]+\.(?:js|css))",[\s\S]*?c\.Data\(http\.StatusOK, "[^"]+", (?:\[\]byte\()?([\w]+)\)?\)/g)].map(m=>[m[1],map.get(m[2])]));
const requests=[],checks=[],dataSteps=new Map();let mode='normal',delay=0,serial=0,tokenMode='candidate',incidentMode='normal',detailMode='normal';
let companyName='fixture-company-0',modelRotation=0;
let freshnessState='normal';
const base=Math.floor(Date.now()/60000)*60-3600;
const metric={mean_ms:750,observed:100,p50_ms:500,p95_ms:2200,p99_ms:3100,max_ms:4000,coverage_complete:true,mean_coverage_complete:true};
const counts={requests:100,routed_requests:95,unavailable_channel_requests:5};
const incident=i=>({id:'incident-'+String(i).padStart(3,'0'),revision:1,row_snapshot:'row-'+i,status:i%2?'resolved':'open',primary_fault_class:'unknown',severity:'',scenario:'',scenario_pending:true,pending_reason:'no_fallback_config_fact',channel_scope:'assigned',channel_scope_label:'已分配渠道',channel_id:3,model:'fixture-model',user_group:'default',first_seen_ts:base-600,last_seen_ts:base-i*60,evidence_level:'inferred',responsibility:'unknown'});
function json(res,data,status=200){
 if(args.includes('--freshness')&&(data.models||data.channels||typeof data.statistics_complete==='boolean')){
  data.timing={finalize_delay_seconds:120,alignment_seconds:60,collection_poll_seconds:60};data.lag_seconds=freshnessState==='backlog'?180:freshnessState==='normal'?60:0;data.collection_failed=freshnessState==='failed';
  if(data.models){data.source.facts_complete=freshnessState!=='gap';data.source.requests_complete=freshnessState!=='gap';data.source.coverage_status=freshnessState==='gap'?'incomplete':'complete'}
  if(data.channels)data.coverage={complete:freshnessState!=='gap',note:freshnessState==='gap'?'历史区间存在缺口。':'统计窗口覆盖完整。'};
  if(typeof data.statistics_complete==='boolean'){data.statistics_complete=freshnessState!=='gap';data.coverage_complete=freshnessState==='history';data.tail_syncing=freshnessState!=='history'}
 }
 res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(data))
}
const server=createServer(async(req,res)=>{
 const url=new URL(req.url,'http://localhost'),pathname=url.pathname,q=Object.fromEntries(url.searchParams),n=++serial,record={pathname,query:q,at:Date.now(),mode,serial:n};requests.push(record);res.on('close',()=>{if(!res.writableFinished)record.cancelled=true});
 if(pathname==='/'){const bytes=fs.readFileSync('monitor/page.html');record.source_sha256=createHash('sha256').update(bytes).digest('hex');res.writeHead(200,{'Content-Type':'text/html;charset=utf-8'});res.end(bytes);return}
 if(assets.has(pathname)){const bytes=fs.readFileSync(path.join(root,'monitor',assets.get(pathname)));record.source_sha256=createHash('sha256').update(bytes).digest('hex');res.writeHead(200,{'Content-Type':pathname.endsWith('.css')?'text/css':'application/javascript'});res.end(bytes);return}
 if(pathname==='/me')return json(res,{name:'Synthetic reviewer',is_root:true});
 if(pathname==='/api/brand')return json(res,{name:'本地合成验收'});
 if(pathname==='/customer-health/groups/update'){let raw='';for await(const part of req)raw+=part;companyName=JSON.parse(raw).name;return json(res,{ok:true})}
 if(pathname.startsWith('/anomalies/token/')){
  let body='';for await(const part of req)body+=part;const query=body?JSON.parse(body):{};
  if(pathname.endsWith('/detection/query')){if(tokenMode==='error')return json(res,{error:'合成查询失败'},503);const r=token.report(query);if(tokenMode!=='candidate')r.items=[];r.has_more=false;if(tokenMode==='complete')Object.assign(r.summary,{source_complete:true,unprocessed_requests:0,insufficient_requests:0,gaps:[],candidate_requests:0});if(tokenMode==='detecting')Object.assign(r.summary,{source_complete:true,unprocessed_requests:1,insufficient_requests:0,gaps:[]});return json(res,r)}
  if(pathname.endsWith('/detection/detail'))return json(res,token.detail(query.revision));
  if(pathname.endsWith('/detection/health'))return json(res,{...token.detectionHealth,enabled:tokenMode!=='disabled',policy_configured:true});
  if(pathname.endsWith('/notifications/health'))return json(res,token.notifyHealth);
  if(pathname.endsWith('/health'))return json(res,{...token.health,enabled:tokenMode!=='disabled',source_available:true});
  if(pathname.endsWith('/access'))return json(res,{...token.access,export_enabled:false});
 }
 if(pathname==='/incidents/health')return json(res,{enabled:incidentMode!=='disabled',ingest_running:false});
 if(pathname==='/incidents'){
  if(incidentMode==='forbidden')return json(res,{error:'没有事故监控查看权限'},403);
  if(incidentMode==='disabled')return json(res,{enabled:false,note:'事故核心开关未开启'});
  if(q.cursor&&incidentMode==='changed')return json(res,{error:'事故快照已变化，请重新查询',data_status:'snapshot_changed'},409);
  const empty=incidentMode==='empty'||incidentMode==='incomplete';
  return json(res,{enabled:true,incidents:empty?[]:q.cursor?[incident(1)]:[incident(0)],has_more:!empty&&!q.cursor,next_cursor:q.cursor?'':'900:incident-000',snapshot:'fixture-snapshot',generated_at:base+n,from_ts:+q.from,to_ts:+q.to,coverage:{complete:incidentMode!=='incomplete',status:incidentMode==='incomplete'?'incomplete':'complete',actual_through_ts:base,scan_through_ts:base+300,lag_seconds:0,finalize_lag_seconds:1500}});
 }
 if(pathname==='/incidents/detail'){
  if(detailMode==='changed')return json(res,{error:'证据快照已变化，请重新打开详情',data_status:'snapshot_changed'},409);
  const r={enabled:true,incident:incident(Number(q.id.split('-')[1])),evidence_snapshot:'detail-snapshot',segments:[{SeqNo:q.segments_cursor?2:1,StartTs:base-600,EndTs:0,ConsecutiveWindows:3}],observations:[{ID:q.observations_cursor?'obs2':'obs1',WindowFromTs:base-600,WindowToTs:base-300,DataPresent:true,Anomalous:true,SourceIDs:'capacity_user_minute_samples',SampleTotal:100,DistinctRequests:null,VisibleAttempts:null,LogRows:null,EvidenceLevel:'inferred',Note:q.observations_cursor?'观测第二页':'合成冻结证据'}],impacts:[{Dimension:'channel',DimValue:q.impacts_cursor?'second-channel':'3',WindowFromTs:base-600,WindowToTs:base,SampleTotal:100,CoverageComplete:false}],impact_dimension_gaps:['user（无事实）'],revisions:[{id:q.revisions_cursor?'revision-second':'revision-first'}],impacts_archived:[{id:q.impacts_archived_cursor?'archive-second':'archive-first'}],anomaly_rate_caveat:'日志口径异常率，不代表客户端最终失败率'};
  for(const key of ['observations','segments','impacts','impacts_archived','revisions']){r[key+'_has_more']=!q[key+'_cursor'];r[key+'_next_cursor']='next-'+key}
  r.observations_next_cursor_key=String(base-300)+':obs1';return json(res,r);
 }
 const refreshable=['/model-statistics/report','/model-statistics/frt','/alerts/rejections','/logchain/requests','/customer-health/report'].includes(pathname);
 const capturedMode=mode,capturedDelay=delay;
 if(refreshable&&capturedDelay)await new Promise(r=>setTimeout(r,capturedDelay));
 if(refreshable&&capturedMode==='error')return json(res,{error:'合成刷新失败'},503);
 const step=(dataSteps.get(pathname)||0)+1;dataSteps.set(pathname,step);
 const through=base+step*60,from=q.from&&Number.isFinite(Date.parse(q.from))?Date.parse(q.from)/1000:through-86400,to=q.to&&Number.isFinite(Date.parse(q.to))?Date.parse(q.to)/1000:through;
 if(refreshable)record.data_through_ts=pathname.startsWith('/model-statistics/')?Math.min(through,to):through;
 if(pathname==='/model-statistics/frt')return json(res,{window:'24h',generated_at:new Date((through+180)*1000).toISOString(),from:new Date(from*1000).toISOString(),to:new Date(to*1000).toISOString(),actual_through_ts:record.data_through_ts,lag_seconds:0,coverage:{complete:true,note:'合成本地完整覆盖'},channels:Array.from({length:30},(_,i)=>({channel_id:i+1,name:'channel-'+i,frt:{...metric,observed:100+n},groups:[{group:'group-'+i,frt:metric,models:[{model:'model-'+i,frt:metric}]}]}))});
 if(pathname==='/model-statistics/report')return json(res,{generated_at:through+180,from_ts:from,to_ts:to,actual_through_ts:record.data_through_ts,target_ts:to,lag_seconds:0,source:{facts_complete:true,requests_complete:true,coverage_status:'complete'},models:Array.from({length:60},(_,index)=>{const i=(index+modelRotation)%60;return {...counts,model:'model-'+i,groups:[{...counts,group:'group-'+i,channels:[{...counts,channel_id:i+1,channel_name:'channel-'+i}],customers:[]}]}})});
 if(pathname==='/alerts/rejections')return json(res,{enabled:true,generated_at:through+180,through_ts:through,actual_through_ts:through,target_ts:through+60,lag_seconds:60,timing:{finalize_delay_seconds:120,alignment_seconds:60,collection_poll_seconds:60},cutoff_ts:through,from_ts:through-86400,to_ts:through,coverage_complete:false,statistics_complete:true,statistics_available:true,tail_syncing:true,total:100+n,row_total:100,has_more:!q.cursor,next_cursor:'next',reason_options:['model_not_found'],rows:Array.from({length:q.cursor?5:70},(_,i)=>({ts:base-i*60,user_id:i+1,reason:'model_not_found',grp:'default',model:'fixture',count:1}))});
 if(pathname==='/logchain/filter-options')return json(res,{models:[],groups:[],channels:[]});
 if(pathname==='/logchain/requests')return json(res,{rows:Array.from({length:65},(_,i)=>({id:1000-i,created_at:base-i*60,type:5,user_id:17,channel_id:3,model_name:'fixture',username:'fixture',content:'合成错误',request_id:'request-'+i,fault_class:'unknown',fault_confidence:'none'})),has_more:false,generated_at:through+180,through_ts:through,query_through_ts:through,coverage_complete:true,cloudwatch_enabled:false,scope:{from_ts:base-86400,to_ts:through}});
 if(pathname==='/customer-health/report')return json(res,{day:'2026-10-23',time_zone:'Asia/Shanghai',generated_at:through+180,to_ts:through,rows:Array.from({length:35},(_,i)=>({group_id:i+1,company:i?'fixture-company-'+i:companyName,total:100+n,success:100,anomaly:0,failed:0,stability_pct:100,members:[{user_id:i+1,username:'fixture-member'}],primary_models:[],channel_scope:'unknown'}))});
 return json(res,{enabled:false});
});
server.listen(0,'127.0.0.1');await once(server,'listening');const origin='http://127.0.0.1:'+server.address().port;
const b=await browserSession({browser:opt('--browser'),scratch:opt('--scratch'),origin});let failure;
const {evaluate:ev,click,waitFor,call,pause}=b;
const count=p=>requests.filter(r=>r.pathname===p).length;
async function check(name,fn){try{const evidence=await fn(),status=evidence?.not_tested?'not_tested':'pass';checks.push({name,status,evidence});console.log(status.toUpperCase()+' '+name)}catch(e){checks.push({name,status:'fail',error:e.stack});throw e}}
async function navigate(tab,context={}){await ev(`window.monitorNavigate(${JSON.stringify(tab)},${JSON.stringify(context)})`)}
async function awayAndBack(){
 await call('Emulation.setFocusEmulationEnabled',{enabled:false});
 const other=await call('Target.createTarget',{url:'about:blank'},null);
 try{
  await call('Target.activateTarget',{targetId:other.targetId},null);await waitFor('document.hidden');
  await call('Target.activateTarget',{targetId:b.targetId},null);await waitFor('!document.hidden&&document.hasFocus()');
 }finally{await call('Target.closeTarget',{targetId:other.targetId},null)}
}
async function assertVisibleWatermark(endpoint,selector){
 const latest=requests.filter(r=>r.pathname===endpoint&&!r.cancelled&&r.data_through_ts).at(-1),through=latest.data_through_ts;
 const format=endpoint==='/alerts/rejections'?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false}):null;
 let expected;
 if(format){const parts=format.formatToParts(new Date(through*1000)),part=k=>parts.find(p=>p.type===k).value;expected=part('month')+'-'+part('day')+' '+part('hour')+':'+part('minute')}
 else expected=await ev(`new Date(${through}*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'})`);
 const shown=await ev(`document.querySelector(${JSON.stringify(selector)}).innerText`);
 if(endpoint==='/logchain/requests'){const stamp=new Date((through+28800)*1000).toISOString().slice(0,19).replace('T',' ');assert.ok(shown.includes(stamp)||shown.includes(expected),shown)}else assert.ok(shown.includes(expected),shown);
 return {data_through:through,displayed:shown};
}
try{
 if(args.includes('--freshness')){
  await call('Page.navigate',{url:origin+'/#tab=model-statistics'});await waitFor("typeof window.monitorNavigate==='function'&&document.readyState==='complete'");
  for(const spec of [
   {tab:'model-statistics',context:{view:'frt'},endpoint:'/model-statistics/frt',id:'frtCoverage',warning:'gap',dateRoot:'frtDatePicker'},
   {tab:'model-statistics',context:{view:'requests'},endpoint:'/model-statistics/report',id:'msCoverage',warning:'incomplete',dateRoot:'msDatePicker'},
   {tab:'alerts',context:{},endpoint:'/alerts/rejections',id:'alStatus',warningID:'alCoverageWarning'}
  ])await check(spec.endpoint+' distinguishes normal wait, backlog, failure and gaps during real refresh',async()=>{
   freshnessState='normal';await navigate(spec.tab,spec.context);await waitFor(`document.getElementById('${spec.id}').textContent.includes('正常等待')`);
   const warns=()=>ev(spec.warningID?`!document.getElementById('${spec.warningID}').hidden`:`document.getElementById('${spec.id}').classList.contains('${spec.warning}')`);
   assert.equal(await warns(),false);
   for(const [state,text]of [['backlog','采集积压'],['failed','采集失败'],['gap',spec.warningID?'数据不完整':spec.id==='msCoverage'?'历史覆盖尚未全部确认':'历史区间存在缺口']]){
    freshnessState=state;await ev("window.dispatchEvent(new Event('focus'))");await waitFor(`document.getElementById('${spec.warningID||spec.id}').textContent.includes('${text}')`);assert.equal(await warns(),true);
   }
   freshnessState='normal';const before=count(spec.endpoint);await ev("window.dispatchEvent(new Event('focus'))");await waitFor(`document.getElementById('${spec.id}').textContent.includes('正常等待')`);assert.equal(await warns(),false);
   const afterFocus=count(spec.endpoint);const deadline=Date.now()+35000;while(count(spec.endpoint)===afterFocus&&Date.now()<deadline)await pause(250);
   assert.ok(count(spec.endpoint)>afterFocus,'real interval did not refresh');await pause(150);assert.equal(await warns(),false);
   freshnessState='history';
   if(spec.tab==='alerts')await ev("document.getElementById('alDate').value='2026-10-21';document.getElementById('alDate').dispatchEvent(new Event('change'))");
   else await navigate(spec.tab,{...spec.context,range:'calendar',from:'2026-10-20T16:00:00Z',to:'2026-10-21T16:00:00Z'});
   await waitFor(`document.getElementById('${spec.id}').textContent.includes('已追平封口目标')`);assert.equal(await warns(),false);
   const fixed=requests.filter(r=>r.pathname===spec.endpoint).at(-1).query;await ev("window.dispatchEvent(new Event('focus'))");await pause(200);
   const refreshed=requests.filter(r=>r.pathname===spec.endpoint).at(-1).query;assert.equal(refreshed.from,fixed.from);assert.equal(refreshed.to,fixed.to);
   await b.screenshot(path.join(out,spec.id+'-complete.png'));return {states:4,real_timer:true,requests:count(spec.endpoint)-before};
  });
 }else{
 await call('Page.navigate',{url:origin+'/#tab=model-statistics&view=frt'});await waitFor("document.querySelector('.frt-channel')");
 await check('FRT three levels, keyboard focus, scroll retained through slow and failed focus refresh',async()=>{
  await click('.frt-channel>summary');await click('.frt-group>summary');
  await ev("document.querySelectorAll('.frt-channel').forEach(e=>e.open=true);window.scrollTo(0,2600)");await pause(150);
  const before=await ev('scrollY');assert.ok(before>2000);
  delay=1200;const prior=count('/model-statistics/frt');await call('Emulation.setFocusEmulationEnabled',{enabled:true});await ev("document.querySelectorAll('.frt-channel>summary')[4].focus({preventScroll:true});window.dispatchEvent(new Event('focus'))");
  await pause(350);assert.equal(await ev('scrollY'),before);await waitFor("document.getElementById('frtTree').getAttribute('aria-busy')==='false'");assert.equal(await ev('scrollY'),before);assert.equal(await ev("document.activeElement.matches('.frt-channel>summary')"),true);assert.ok(count('/model-statistics/frt')>prior);
  const anchorTop=await ev("document.querySelectorAll('.frt-channel>summary')[4].getBoundingClientRect().top");mode='error';await ev("window.dispatchEvent(new Event('focus'))");await waitFor("!document.getElementById('frtError').hidden");assert.ok(Math.abs(await ev("document.querySelectorAll('.frt-channel>summary')[4].getBoundingClientRect().top")-anchorTop)<1);assert.ok(await ev('scrollY>2000'));assert.equal(await ev("document.querySelectorAll('.frt-channel[open]').length"),30);mode='normal';delay=0;
  return {before,after:await ev('scrollY'),syntheticFocusEvent:true};
 });
 await check('actual browser tab focus/visibility, cancellation and late responses keep FRT state',async()=>{
  await ev("window.__refreshEvents=[];window.addEventListener('focus',e=>__refreshEvents.push({type:'focus',trusted:e.isTrusted}));document.addEventListener('visibilitychange',e=>__refreshEvents.push({type:'visibility',hidden:document.hidden,trusted:e.isTrusted}))");
  const old=count('/model-statistics/frt');await awayAndBack();await waitFor("document.getElementById('frtTree').getAttribute('aria-busy')==='false'");assert.ok(count('/model-statistics/frt')>old);
  const events=await ev('window.__refreshEvents');assert.ok(events.some(e=>e.type==='visibility'&&e.hidden&&e.trusted));assert.ok(events.some(e=>e.type==='focus'&&e.trusted));
  delay=1100;const before=count('/model-statistics/frt');await ev("window.dispatchEvent(new Event('focus'))");await pause(150);assert.ok(count('/model-statistics/frt')>before);delay=0;await ev("window.dispatchEvent(new Event('focus'))");await waitFor("document.getElementById('frtTree').getAttribute('aria-busy')==='false'");const content=await ev("document.getElementById('frtTree').innerText");await pause(1200);assert.equal(await ev("document.getElementById('frtTree').innerText"),content);assert.ok(requests.some(r=>r.pathname==='/model-statistics/frt'&&r.cancelled));
  return {events,cancelled:requests.filter(r=>r.pathname==='/model-statistics/frt'&&r.cancelled).length};
 });
 await check('real FRT timer advances response and data without disturbing scroll over two cycles',async()=>{
  if(args.includes('--quick'))return {not_tested:'quick development run'};
  const start=count('/model-statistics/frt'),scroll=await ev('scrollY'),anchor=await ev("document.querySelectorAll('.frt-channel>summary')[4].getBoundingClientRect().top"),watermarks=[await assertVisibleWatermark('/model-statistics/frt','#frtCoverage')];
  for(let i=0;i<2;i++){const deadline=Date.now()+35000;while(count('/model-statistics/frt')<start+i+1&&Date.now()<deadline)await pause(300);await waitFor("document.getElementById('frtTree').getAttribute('aria-busy')==='false'");assert.ok(count('/model-statistics/frt')>=start+i+1);assert.equal(await ev("document.querySelectorAll('.frt-channel>summary')[4].getBoundingClientRect().top"),anchor);assert.ok(await ev('scrollY>2000'));watermarks.push(await assertVisibleWatermark('/model-statistics/frt','#frtCoverage'));assert.ok(watermarks.at(-1).data_through>watermarks.at(-2).data_through)}return {cycles:2,interval_ms:30000,scroll,watermarks};
 });
 await check('FRT dark/light 320/375 hierarchy backgrounds differ and do not overflow viewport',async()=>{
  await ev("window.scrollTo(0,0)");for(const theme of ['dark','light'])for(const width of [320,375]){await ev(`document.body.setAttribute('theme-mode',${JSON.stringify(theme)})`);await call('Emulation.setDeviceMetricsOverride',{width,height:950,deviceScaleFactor:1,mobile:false});await pause(100);const colors=await ev("['.frt-channel','.frt-group','.frt-model'].map(s=>getComputedStyle(document.querySelector(s)).backgroundColor)");assert.equal(new Set(colors).size,3);assert.ok(await ev('document.documentElement.scrollWidth<=innerWidth'));await ev("document.querySelector('.frt-channel').scrollIntoView({block:'start'})");await b.screenshot(path.join(out,`frt-${theme}-${width}.png`))}await ev("document.body.setAttribute('theme-mode','dark')");await call('Emulation.setDeviceMetricsOverride',{width:1280,height:950,deviceScaleFactor:1,mobile:false});return {themes:2,widths:[320,375]};
 });
 for(const spec of [{tab:'model-statistics',path:'/model-statistics/report',rows:'.ms-model-row',ready:"document.querySelector('[data-ms-expand]')",scroll:861},{tab:'alerts',path:'/alerts/rejections',rows:'#alTableBody tr',ready:"document.querySelectorAll('#alTableBody tr').length>50",scroll:900},{tab:'logchain',path:'/logchain/requests',rows:'#lcTableBody tr',ready:"document.querySelectorAll('[data-lc-id]').length>50",scroll:1000},{tab:'customer-health',path:'/customer-health/report',rows:'#chRows tr',ready:"document.querySelectorAll('[data-ch-row]').length>30",scroll:900}]){
  await check(spec.tab+' keeps scroll, focus, data on slow/failing refresh and real timer',async()=>{
   await navigate(spec.tab);await waitFor(spec.ready);await ev(`window.scrollTo(0,${spec.scroll})`);const before=await ev('scrollY'),idx=await ev(`Array.from(document.querySelectorAll(${JSON.stringify(spec.rows)})).findIndex(e=>e.getBoundingClientRect().top>=0)`),anchorExpr=`document.querySelectorAll(${JSON.stringify(spec.rows)})[${idx}].getBoundingClientRect().top`,top=await ev(anchorExpr);delay=700;await ev("window.dispatchEvent(new Event('focus'))");await pause(200);assert.equal(await ev('scrollY'),before);await pause(800);assert.equal(await ev('scrollY'),before);mode='error';await ev("window.dispatchEvent(new Event('focus'))");await pause(900);
   const errorPosition={before,top,after:await ev('scrollY'),after_top:await ev(anchorExpr),focus:await ev('({tag:document.activeElement.tagName,id:document.activeElement.id})')};
   fs.writeFileSync(path.join(out,spec.tab+'-error-position.json'),JSON.stringify(errorPosition,null,2)+'\n');
   assert.ok(Math.abs(errorPosition.after_top-top)<1,JSON.stringify(errorPosition));assert.ok(await ev('scrollY>500'));assert.ok(await ev(spec.ready));mode='normal';delay=0;
   const status={'model-statistics':'#msStatus',alerts:'#alStatus',logchain:'#lcStatus','customer-health':'#chMeta'}[spec.tab];
   const beforeFocus=count(spec.path);await awayAndBack();await pause(250);assert.ok(count(spec.path)>beforeFocus);assert.ok(Math.abs(await ev(anchorExpr)-top)<1);
   const watermarks=[await assertVisibleWatermark(spec.path,status)];
   if(!args.includes('--quick'))for(let i=0;i<2;i++){const old=count(spec.path),deadline=Date.now()+(spec.tab==='customer-health'?65000:35000);while(count(spec.path)<=old&&Date.now()<deadline)await pause(300);assert.ok(count(spec.path)>old);await pause(200);assert.ok(Math.abs(await ev(anchorExpr)-top)<1);watermarks.push(await assertVisibleWatermark(spec.path,status));assert.ok(watermarks.at(-1).data_through>watermarks.at(-2).data_through)}return {before,after:await ev('scrollY'),anchor_top:top,real_timer:!args.includes('--quick'),watermarks,real_focus_visibility:true};
  });
 }
 await check('customer maintenance editing survives focus and refresh without losing draft',async()=>{
  await click('[data-ch-row="1"]');await click('[data-ch-edit="1"]');await b.input('#chEditName-1','未保存的公司名');const old=count('/customer-health/report');await ev("window.dispatchEvent(new Event('focus'))");await pause(250);assert.equal(count('/customer-health/report'),old);assert.equal(await ev("document.getElementById('chEditName-1').value"),'未保存的公司名');await click('[data-ch-cancel="1"]');
  await click('[data-ch-edit="1"]');await b.input('#chEditName-1','已保存的公司名');await click('[data-ch-save="1"]');await waitFor("document.querySelector('[data-ch-row=\"1\"]').innerText.includes('已保存的公司名')");assert.ok(count('/customer-health/report')>old);assert.equal(await ev("document.getElementById('chEditName-1')===null"),true);
 });
 await check('historical dates, unsubmitted input and expand state survive returning to model tabs',async()=>{
  const day=new Date(Date.now()-86400000).toISOString().slice(0,10);
  await navigate('model-statistics',{from:day+'T00:00:00+08:00',to:day+'T23:59:59+08:00',range:'exact'});await waitFor("document.querySelector('[data-ms-expand]')");await click('[data-ms-expand]');
  const dates=await ev('[msDateStart.value,msDateEnd.value]');await navigate('alerts');await navigate('model-statistics');await pause(200);assert.deepEqual(await ev('[msDateStart.value,msDateEnd.value]'),dates);
  await ev("msDateStart.value='2026-10-01';msDateStart.focus()");await awayAndBack();await pause(200);assert.equal(await ev('msDateStart.value'),'2026-10-01');
  return {dates,preserved_draft:true};
 });
 await check('model ranking changes preserve visible model, focus, expanded sections and date drafts',async()=>{
  await navigate('model-statistics',{view:'requests'});await waitFor("!!document.querySelector('[data-ms-expand]')");await pause(300);
  // Keep enough rows on both sides: moving an early row to the very bottom
  // hits the browser's maximum scroll and cannot retain its old screen top.
  const target=await ev("(()=>{const e=document.querySelector('[data-ms-expand=\"model-20\"]');e.scrollIntoView({block:'center'});e.focus({preventScroll:true});return {model:e.dataset.msExpand,top:e.getBoundingClientRect().top,scroll:scrollY,dates:[msDateStart.value,msDateEnd.value],open:[...document.querySelectorAll('[data-ms-expand][aria-expanded=\"true\"]')].map(e=>e.dataset.msExpand).sort()}})()");
  modelRotation=5;await ev("window.dispatchEvent(new Event('focus'))");await waitFor("document.querySelector('[data-ms-expand]')?.dataset.msExpand==='model-5'");
  const result=await ev(`(()=>{const e=[...document.querySelectorAll('[data-ms-expand]')].find(e=>e.dataset.msExpand===${JSON.stringify(target.model)});return {top:e.getBoundingClientRect().top,scroll:scrollY,focused:document.activeElement===e,dates:[msDateStart.value,msDateEnd.value],open:[...document.querySelectorAll('[data-ms-expand][aria-expanded="true"]')].map(e=>e.dataset.msExpand).sort()}})()`);
  fs.writeFileSync(path.join(out,'model-reorder-position.json'),JSON.stringify({before:target,after:result},null,2)+'\n');
  assert.equal(result.focused,true);assert.ok(Math.abs(result.top-target.top)<1);assert.deepEqual(result.open,target.open);assert.deepEqual(result.dates,target.dates);modelRotation=0;return {before:target,after:result};
 });
 await check('Token default is filters, one status and records; evidence remains available',async()=>{
  await navigate('anomalies');await waitFor("document.querySelector('[data-ta-detail]')");
  const fixtureFrom=new Date((token.at-86400+28800)*1000).toISOString().slice(0,16),fixtureTo=new Date((token.at+3600+28800)*1000).toISOString().slice(0,16);
  await ev(`taFrom.value=${JSON.stringify(fixtureFrom)};taTo.value=${JSON.stringify(fixtureTo)};taFilters.requestSubmit()`);await waitFor("taList.getAttribute('aria-busy')==='false'");
  assert.equal(await ev("document.querySelectorAll('#tokenAnomalyView>h2').length"),0);assert.equal(await ev("document.querySelector('#taMore').open"),false);await click('[data-ta-detail]');await waitFor("document.getElementById('taDetailBody').innerText.includes('原判定时间')");await click('#taDetailClose');await b.screenshot(path.join(out,'token-default.png'));return {from:token.at-86400,to:token.at+3600,request_at:token.at+10};
 });
 await check('Token empty states never confuse disabled, detection, incomplete or failed with healthy empty',async()=>{
  const results=[];for(const [value,pattern]of [['complete','当前所选时间内没有 Token 异常情况'],['disabled','采集未启用'],['detecting','检测中'],['incomplete','覆盖未完整'],['error','查询失败']]){tokenMode=value;await ev("document.getElementById('taFilters').requestSubmit()");await waitFor("document.getElementById('taList').getAttribute('aria-busy')==='false'");const text=await ev("document.getElementById('tokenAnomalyView').innerText");assert.ok(text.includes(pattern),value+': '+text.slice(0,300));if(value!=='complete')assert.ok(!text.includes('当前所选时间内没有 Token 异常情况'));results.push(value)}tokenMode='candidate';return results;
 });
 await check('Incident subtab, list/detail, unknown counts, evidence and stable pagination',async()=>{
  await click('#incidentTab');await waitFor("document.querySelector('[data-incident-id]')");const range=await ev("[imFrom.value,imTo.value]");await click('button[data-incident-id]');await waitFor("document.getElementById('imDetailBody').innerText.includes('合成冻结证据')");const text=await ev("document.getElementById('imDetailBody').innerText");assert.match(text,/未知/);assert.match(text,/尚未定级/);await b.screenshot(path.join(out,'incident-evidence.png'));await click('#imDetailClose');await click('#imNext');await waitFor("document.querySelectorAll('#imList tbody tr').length===2");assert.ok(requests.filter(r=>r.pathname==='/incidents').at(-1).query.snapshot);await click('#tokenAnomalyTab');await click('#incidentTab');assert.deepEqual(await ev('[imFrom.value,imTo.value]'),range);assert.equal(await ev("document.querySelectorAll('#imList tbody tr').length"),2);await b.screenshot(path.join(out,'incident-list.png'));
 });
 await check('Incident disabled, incomplete, healthy empty and permissions are distinct',async()=>{
  for(const [value,pattern]of [['disabled','事故核心开关未开启'],['incomplete','覆盖不足'],['empty','当前所选时间内没有已记录事故'],['forbidden','没有事故监控查看权限']]){incidentMode=value;await ev("document.getElementById('imFilters').requestSubmit()");await waitFor("document.getElementById('imList').getAttribute('aria-busy')==='false'");assert.ok((await ev("document.getElementById('incidentView').innerText")).includes(pattern))}
 });
 await check('Incident evidence pages retain other sections and reject changed child snapshots',async()=>{
  incidentMode='normal';await ev('imFilters.requestSubmit()');await waitFor("document.querySelector('[data-incident-id]')&&imList.getAttribute('aria-busy')==='false'");
  await click('button[data-incident-id]');await waitFor("document.getElementById('imDetailBody').innerText.includes('合成冻结证据')");
  assert.equal(requests.filter(r=>r.pathname==='/incidents/detail').at(-1).query.row_snapshot,'row-0');
  await click('#imDetailBody details summary');
  const specs=[['observations','imObsMore','观测第二页'],['segments','imSegMore',null],['impacts','imImpactMore','second-channel'],['impacts_archived','imArchiveMore','archive-second'],['revisions','imRevisionMore','revision-second']];
  const completed=[];
  for(const [key,id,needle]of specs){
   if(key==='revisions'){
    const original=await ev('imDetailBody.innerHTML');detailMode='changed';await click('#'+id);await waitFor("!imDetailError.hidden&&imDetailError.innerText.includes('快照')");assert.equal(await ev('imDetailBody.innerHTML'),original);detailMode='normal';
   }
   await click('#'+id);await waitFor(`document.getElementById(${JSON.stringify(id)}).hidden===true`);
   const query=requests.filter(r=>r.pathname==='/incidents/detail').at(-1).query;
   assert.equal(query.evidence_snapshot,'detail-snapshot');assert.equal(query.row_snapshot,'row-0');assert.ok(query[key+'_cursor']);
   assert.equal(await ev("document.querySelector('#imDetailBody details').open"),true);
   const body=await ev('imDetailBody.innerText');assert.ok(body.includes('合成冻结证据'));for(const text of completed)assert.ok(body.includes(text));if(needle){assert.ok(body.includes(needle));completed.push(needle)}
   if(key==='segments')assert.equal(await ev("document.querySelectorAll('#imDetailBody table')[1].querySelectorAll('tbody tr').length"),2);
  }
  assert.ok(!(await ev('imDetailBody.innerText')).includes('当前仅展示部分证据'));await click('#imDetailClose');return {pages:5,row_snapshot_checked:true,child_snapshot_conflict_preserves_evidence:true};
 });
 await check('Incident live range advances, manual dates and drafts stay fixed, changed snapshots reject pagination',async()=>{
  incidentMode='normal';await click('#imReset');await waitFor("document.querySelector('[data-incident-id]')&&imList.getAttribute('aria-busy')==='false'");
  const live=requests.filter(r=>r.pathname==='/incidents').at(-1).query;
  await ev("window.__originalNow=Date.now;Date.now=()=>window.__originalNow()+120000");await awayAndBack();await pause(200);
  const advanced=requests.filter(r=>r.pathname==='/incidents').at(-1).query;assert.equal(+advanced.to-+live.to,120);assert.equal(+advanced.to-+advanced.from,86400);
  const day=new Date(Date.now()-2*86400000).toISOString().slice(0,10);
  await ev(`imFrom.value=${JSON.stringify(day+'T00:00')};imTo.value=${JSON.stringify(day+'T23:59')};imFilters.requestSubmit()`);await pause(200);
  const fixed=requests.filter(r=>r.pathname==='/incidents').at(-1).query;
  await ev("imFrom.value='2026-10-01T01:02';imFrom.focus();Date.now=()=>window.__originalNow()+240000");await awayAndBack();await pause(200);
  assert.equal(await ev('imFrom.value'),'2026-10-01T01:02');const after=requests.filter(r=>r.pathname==='/incidents').at(-1).query;assert.equal(after.from,fixed.from);assert.equal(after.to,fixed.to);
  incidentMode='changed';await click('#imNext');await waitFor("!imError.hidden&&imError.innerText.includes('快照')");assert.equal(await ev("document.querySelectorAll('#imList tbody tr').length"),1);
  await ev('Date.now=window.__originalNow;delete window.__originalNow');return {live:{from:live.from,to:live.to},advanced:{from:advanced.from,to:advanced.to},fixed:{from:fixed.from,to:fixed.to},snapshot_conflict:true};
 });
 await check('Token, incident, logchain and maintenance fit 320/375px',async()=>{
  incidentMode='normal';tokenMode='candidate';for(const [tab,context,id]of [['anomalies',{view:'token'},'token'],['anomalies',{view:'incidents'},'incidents'],['logchain',{},'logchain'],['customer-health',{},'customer']]){
   await navigate(tab,context);await pause(200);for(const width of [320,375]){await call('Emulation.setDeviceMetricsOverride',{width,height:950,deviceScaleFactor:1,mobile:false});await ev('window.scrollTo(0,0)');assert.ok(await ev('document.documentElement.scrollWidth<=innerWidth'),id+' overflow '+width);await b.screenshot(path.join(out,id+'-'+width+'.png'))}
  }return {widths:[320,375],modules:4};
 });
 }
 assert.deepEqual(b.external,[]);assert.deepEqual(b.exceptions,[]);
}catch(e){failure=e;console.error(e.stack)}finally{
 fs.writeFileSync(path.join(out,'result.json'),JSON.stringify({at:new Date().toISOString(),kind:'real Chrome with synthetic local APIs, not final image',driver_sha256:driverSHA256,quick:args.includes('--quick'),version:b.version,checks,requests,exceptions:b.exceptions,external:b.external,failure:failure?.stack},null,2)+'\n');
 await b.close();server.closeAllConnections();await new Promise(r=>server.close(r));
}
if(failure)process.exitCode=1;
