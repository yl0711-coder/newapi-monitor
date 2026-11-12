// Browser verification for the channel-level contract. Local fixtures/replays
// are explicitly identified; --origin validates an already deployed version.
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {browserSession} from './browser-session.mjs';
const args=process.argv.slice(2),opt=(name,fallback)=>args.includes(name)?args[args.indexOf(name)+1]:fallback;
const out=path.resolve(opt('--out','.local-test-kit/group-governance-channel-evidence/browser.json'));
const input=opt('--input'),retained=input?JSON.parse(fs.readFileSync(input,'utf8')):null;
const source=name=>fs.readFileSync(new URL('../monitor/'+name,import.meta.url),'utf8');
const through=1794355200,baseGroup='演示分组';let serial=0,failNext=false,server,origin=opt('--origin','');
const requests=[];
function fixture(){
 const channels=Array.from({length:9},(_,i)=>{
  const id=i+1,group=id<7?baseGroup:'另一分组',status=id===1?(serial?'unchanged':'pause'):id===2?'increase':id===3?'attention':id===6?'no_requests':'unchanged',key=JSON.stringify({Group:group,Model:'',Channel:id});
  const end=through+serial*60,attempts=id===6?0:123+serial,actions=status==='pause'?['pause:1']:status==='increase'?['increase:demo']:[];
  return {key,group,channel_id:id,channel_name:'渠道 '+id,channel_status:1,models:['文本模型','推理模型'],status,from_ts:end-900,to_ts:end,window_minutes:15,requests:attempts,rpm:attempts/15,peak_rpm:attempts?35+serial:0,attributed_errors:id===3?17:id===2?3:0,pending_errors:0,other_errors:0,fault_rate_pct:attempts?(id===3?17:id===2?3:0)/attempts*100:null,requests_complete:true,errors_complete:true,frt_source_complete:true,config_current:true,config_at:through,requests_through_ts:end,errors_through_ts:end,frt_through_ts:end,frt:{mean_ms:id===3?null:id===1&&!serial?10000:1344,observed:id===3?0:attempts,p50_ms:id===1&&!serial?10000:1000,p95_ms:id===1&&!serial?10000:5000,coverage_complete:true,mean_unavailable_reason:id===3?'no_valid_samples':''},baseline:{rpm:6,from_ts:end-2700,to_ts:end-900,minutes:30},compared_samples:attempts,actions,evidence:[],decision:{label:({pause:'建议暂停',increase:'建议增加渠道',attention:'继续观察',no_requests:'近期无请求',unchanged:'不用调整'})[status],reason:status==='pause'?'暂停整个渠道 #1：持续显著偏慢；跨组承接检查已通过':status==='increase'?'为“演示分组”增加渠道：已确认上游限流，补充容量':status==='attention'?'已确认上游 HTTP/2 流错误，先处理具体故障':status==='no_requests'?'最近15分钟无请求，保留当前配置':'速度和错误情况正常，维持现状',frt:id===3?'无有效首字耗时样本，暂不能比较速度':id===1&&!serial?'按当前模型构成对齐参照 4.00 秒，相比慢 150.0%；同模型判慢规则已持续满足':'按当前模型构成对齐参照 1.20 秒，相比慢 12.0%；未达到持续判慢条件',errors:id===3?'主要原因：上游 HTTP/2 流错误（上游侧）；待判 0 次':id===2?'主要原因：上游容量限制（上游侧）；待判 0 次':'未发现已确认的渠道或上游错误；待判 0 次',traffic:attempts?`比此前正常水平增加 ${(attempts/15/6*100-100).toFixed(1)}%（每分钟 6.00 次）`:'近期没有请求，保留已有正常基准',gaps:id===3?['无有效首字耗时样本，暂不能比较速度']:[]},error_reasons:id===2||id===3?[{reason:id===3?'上游 HTTP/2 流错误':'上游容量限制',responsibility:'上游侧',count:id===3?17:3,samples:[{request_id:'retained-request-'+id,created_at:end-20,model:'推理模型',log_id:id}]}]:[],trend:Array.from({length:15},(_,n)=>({at:end-(15-n)*60,requests:attempts?(n===0?35+serial:n<=4?7:6):0,observed:id===3?0:attempts?(n===0?35+serial:n<=4?7:6):0,mean_ms:id===3||!attempts?null:id===1&&!serial?10000:1344,errors:n===0?(id===3?17:id===2?3:0):0,pending:0}))};
 });
 const actions=[{id:'increase:demo',kind:'increase',group:baseGroup,models:['文本模型','推理模型'],subtypes:['capacity','backup'],scope:[baseGroup+' / 文本模型',baseGroup+' / 推理模型'],reason:'已确认上游限流，补充容量及备用',triggers:[]}];
 if(!serial)actions.push({id:'pause:1',kind:'pause',channel_id:1,subtype:'channel',scope:[baseGroup+' / 文本模型'],reason:'持续显著偏慢；承接检查通过',triggers:[]});
 return {enabled:true,state:{last_success_at:through,current_group_count:3},groups:[baseGroup,'另一分组','无请求分组'].map(group=>({group,current:true,channels:channels.filter(c=>c.group===group).map(c=>({id:c.channel_id,name:c.channel_name,status:1}))})),realtime:{status:'ready',advice_ready:true,config_ready:true,requests_complete:true,metrics_through_ts:through+serial*60,generated_at:through+30+serial*60,config_snapshot_at:through,finalize_delay_seconds:120,rule_version:'explicit-channel-browser-fixture',adjustment_count:actions.length,pause_count:serial?0:1,increase_count:1,channels,actions}};
}
if(!origin){
 server=http.createServer((req,res)=>{
  requests.push(req.url);
  if(req.url==='/group-governance/report'){res.setHeader('content-type','application/json');if(failNext){failNext=false;res.statusCode=503;return res.end(JSON.stringify({error:'本地读取暂时失败，保留原结果。'.repeat(8)}))}return res.end(JSON.stringify(retained||fixture()))}
  const asset={'/group-governance.js':'group_governance.js','/group-governance.css':'group_governance.css','/view-refresh.js':'view_refresh.js'}[req.url];if(asset){res.setHeader('content-type',asset.endsWith('.js')?'text/javascript':'text/css');return res.end(source(asset))}
  if(req.url!=='/'){res.writeHead(404);return res.end()}
  const page=source('page.html'),fragment=page.slice(page.indexOf('<div class="tabpane group-governance-page"'),page.indexOf('</div><!-- /tab-group-governance -->')+'</div><!-- /tab-group-governance -->'.length).replace('id="tab-group-governance" hidden','id="tab-group-governance"');
  res.setHeader('content-type','text/html; charset=utf-8');res.end(`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><style>body{margin:0;padding:20px;background:#11141a;color:#e5e7eb;font:14px system-ui;--text:#e5e7eb;--text-muted:#9ca3af;--card:#1d2029;--border:#343846}*{box-sizing:border-box}</style><link rel="stylesheet" href="/group-governance.css"><body>${fragment}<script>window.__navigations=[];window.monitorNavigate=(...args)=>window.__navigations.push(args)</script><script src="/view-refresh.js"></script><script src="/group-governance.js"></script><script>groupGovernanceActivate()</script></body></html>`);
 });await new Promise(r=>server.listen(0,'127.0.0.1',r));origin='http://127.0.0.1:'+server.address().port;
}
fs.mkdirSync(path.dirname(out),{recursive:true});
const b=await browserSession({browser:opt('--browser','C:/Program Files/Google/Chrome/Application/chrome.exe'),scratch:path.resolve(opt('--scratch','.local-test-kit/group-governance-channel-work/browser')),origin});
const result={mode:retained?'retained-local-data-replay':server?'explicit-fixture':'running-monitor',checks:[],input_sha256:input?createHash('sha256').update(fs.readFileSync(input)).digest('hex'):null};
const selector=parts=>'#gg-'+Buffer.from(JSON.stringify(parts)).toString('hex');
const open=async parts=>{const id=selector(parts);await b.waitFor(`!!document.querySelector(${JSON.stringify(id+' > summary')})`);if(!await b.evaluate(`document.querySelector(${JSON.stringify(id)}).open`))await b.click(id+' > summary');await b.waitFor(`document.querySelector(${JSON.stringify(id)}).open&&document.querySelector(${JSON.stringify(id)}).children.length>1`);return id};
try{
 if(!server)await b.authenticate();
 await b.call('Page.addScriptToEvaluateOnNewDocument',{source:`(()=>{const f=window.fetch.bind(window);window.__ggReads=[];window.__logchainQueries=0;window.fetch=async(...args)=>{const url=new URL(typeof args[0]==='string'?args[0]:args[0].url,location.href);if(url.pathname==='/logchain/requests')window.__logchainQueries++;const response=await f(...args);if(url.pathname==='/group-governance/report'&&response.ok){window.__ggLast=await response.clone().json();window.__ggReads.push({at:Date.now(),through:window.__ggLast.realtime.metrics_through_ts})}return response}})()`});
 await b.call('Page.navigate',{url:origin+(server?'/':'/#tab=group-governance')});await b.waitFor('window.__ggLast?.realtime?.channels?.length>0&&ggRows.querySelector("details")',15000);
 const report=await b.evaluate('window.__ggLast'),rt=report.realtime;
 if(opt('--image'))assert.equal((await b.evaluate("fetch('/ready').then(r=>r.json())")).image_version,opt('--image'));
 if(opt('--manifest')){const manifest=JSON.parse(fs.readFileSync(opt('--manifest'),'utf8'));for(const file of ['group_governance.js','group_governance.css','view_refresh.js']){const body=await b.evaluate(`fetch(${JSON.stringify('/'+file.replaceAll('_','-'))}).then(r=>r.text())`);assert.equal(createHash('sha256').update(body).digest('hex'),manifest.find(f=>f.path==='monitor/'+file)?.sha256)}}
 const uniq=new Set(rt.channels.map(c=>JSON.stringify([c.group,c.channel_id])));assert.equal(uniq.size,rt.channels.length);assert.equal(new Set(rt.actions.map(a=>a.id)).size,rt.adjustment_count);
 for(const a of rt.actions){for(const t of a.triggers||[]){
  const c=rt.channels.find(c=>c.group===t.group&&c.channel_id===t.channel_id);assert.ok(c);
  assert.ok(a.scope.includes(t.group+' / '+t.model));
  if(t.from_ts===c.from_ts&&t.to_ts===c.to_ts){
   assert.ok(c.actions.includes(a.id));const e=c.evidence.find(e=>e.key===t.metric_key);assert.ok(e);
   if(t.rule.id!=='primary_exited')assert.deepEqual(t.rule,e.rules.find(r=>r.id===t.rule.id));
  }else{
   // Same-window peer support can differ from the peer's displayed window.
   // It must support a selected trigger in this exact model/window, never
   // inherit an operation merely because another model shares the group.
   assert.equal(a.kind,'increase');assert.ok(t.rule.active&&t.rule.eligible);
   assert.ok([15,30].includes((t.to_ts-t.from_ts)/60));
   assert.ok(a.triggers.some(x=>x.group===t.group&&x.model===t.model&&x.from_ts===t.from_ts&&x.to_ts===t.to_ts&&rt.channels.some(y=>y.group===x.group&&y.channel_id===x.channel_id&&y.from_ts===x.from_ts&&y.to_ts===x.to_ts&&y.actions.includes(a.id))));
  }
 }}
 result.channels=rt.channels.length;result.actions=rt.actions.length;result.checks.push('one channel row per group; action IDs and exact trigger evidence agree');
 const selected=[...new Set([...['pause','increase','attention','pending','unchanged','no_requests','stale'].map(s=>rt.channels.find(c=>c.status===s&&(c.requests>0||s==='no_requests'))),rt.channels.find(c=>c.requests_complete&&c.frt_source_complete&&c.frt.mean_ms==null&&c.attributed_errors>0),rt.channels.find(c=>c.window_minutes===30&&c.requests>0&&c.window_reason)].filter(Boolean))];assert.ok(selected.length>0);result.examples=[];
 for(const c of selected){
  await b.input('#ggSearch',c.group);await open(['group',c.group]);const id=await open(['channel',c.group,c.channel_id]);
  assert.equal(await b.evaluate(`document.querySelector(${JSON.stringify(id)}).querySelectorAll('.gg-core').length`),3);assert.equal(await b.evaluate(`document.querySelector(${JSON.stringify(id)}).querySelectorAll('.gg-model-row').length`),0);
  const text=await b.evaluate(`document.querySelector(${JSON.stringify(id)}).innerText`);for(const sentence of [c.decision.reason,c.decision.frt,c.decision.errors,c.decision.traffic])assert.ok(text.includes(sentence),c.key+' missing backend explanation');
  assert.ok(text.includes('最近'+c.window_minutes+'分钟'));if(c.window_reason)assert.ok(text.includes(c.window_reason));if(c.frt.observed!=null)assert.ok(text.includes(c.frt.observed.toLocaleString('zh-CN')+' 个有效首字样本'));
  const format=t=>new Date(t*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'});assert.ok(text.includes(format(c.from_ts)));assert.ok(text.includes(format(c.to_ts)));if(c.frt.mean_ms!=null)assert.ok(text.includes((c.frt.mean_ms/1000).toLocaleString('zh-CN',{maximumFractionDigits:3})+' 秒'));assert.ok(text.includes(c.requests.toLocaleString('zh-CN')+' 次'));
  assert.ok(!await b.evaluate(`document.querySelector(${JSON.stringify(selector(['basis',c.key]))}).open`));
  if(c.requests_complete&&c.frt_source_complete&&c.frt.mean_ms==null&&c.attributed_errors>0){if(!c.stale)assert.ok(!text.includes('采集落后'));assert.ok(text.includes('无有效首字耗时样本，暂不能比较速度'));assert.ok(text.includes(c.decision.errors));if(c.stale){assert.ok(text.includes('采集落后'));assert.equal(await b.evaluate('ggAdjust.innerText'),'暂不可判断');result.checks.push('genuinely stale collection remains explicit while missing-FRT and known errors stay visible')}}
  result.examples.push({group:c.group,channel_id:c.channel_id,requests:c.requests,confirmed_errors:c.attributed_errors,mean_ms:c.frt.mean_ms,reason:c.decision.reason,frt:c.decision.frt,from:c.from_ts,to:c.to_ts});
 }
 result.checks.push('all available statuses show three channel metrics and backend advice; times and values agree; detailed evidence initially collapsed');
 const selectedChannel=rt.channels.find(c=>c.requests>0&&c.status==='pause')||rt.channels.find(c=>c.requests>0);
 await b.input('#ggSearch',selectedChannel.group);await open(['group',selectedChannel.group]);const id=await open(['channel',selectedChannel.group,selectedChannel.channel_id]);
 await b.evaluate(`document.querySelector(${JSON.stringify(id)}).scrollIntoView({block:'start'})`);await b.screenshot(path.join(path.dirname(out),'channel-wide.png'));
 await open(['basis',selectedChannel.key]);const details=await b.evaluate(`document.querySelector(${JSON.stringify(id)}).innerText`);for(const s of ['调整事项与影响范围','同模型比较与连续评估','渠道汇总与正常基准','分钟趋势','数据状态与来源','不是渠道平均耗时的分位线'])assert.ok(details.includes(s));
 const summary=id+' > summary',position=()=>b.evaluate(`({top:document.querySelector(${JSON.stringify(summary)}).getBoundingClientRect().top,open:[...ggRows.querySelectorAll('details[open]')].map(x=>x.id).sort(),filter:ggSearch.value,nested:document.querySelector(${JSON.stringify(id+' .gg-scroll')}).scrollTop,order:[...document.querySelector(${JSON.stringify(selector(['group',selectedChannel.group]))}).querySelectorAll('.gg-channel-row')].map(x=>JSON.parse(x.dataset.ggKey)[2])})`);
 await b.evaluate(`document.querySelector(${JSON.stringify(summary)}).scrollIntoView({block:'start'});document.querySelector(${JSON.stringify(summary)}).focus({preventScroll:true});document.querySelector(${JSON.stringify(id+' .gg-scroll')}).scrollTop=30;`);await b.pause(120);
 const before=await position();
 const refresh=async()=>{const n=await b.evaluate('__ggReads.length');await b.evaluate('groupGovernanceActivate()');await b.waitFor(`__ggReads.length>${n}&&ggRows.getAttribute('aria-busy')==='false'`,15000)};
 if(server){serial++;await refresh()}else{result.cycles=[];for(let i=0;i<Number(opt('--cycles','3'));i++){const n=await b.evaluate('__ggReads.length');await b.waitFor(`__ggReads.length>${n}&&ggRows.getAttribute('aria-busy')==='false'`,45000);const cycle=await position();assert.deepEqual(cycle.open,before.open);assert.equal(cycle.filter,before.filter);assert.ok(Math.abs(cycle.top-before.top)<3);result.cycles.push(await b.evaluate('__ggReads.at(-1)'))}if(result.cycles.length>=3)assert.ok(new Set(result.cycles.map(r=>r.through)).size>1,'no newly completed minute arrived')}
 const after=await position();assert.deepEqual(after.open,before.open);assert.equal(after.filter,before.filter);assert.equal(after.nested,before.nested);assert.ok(Math.abs(after.top-before.top)<3,'sorting moved the reading position');
 if(server&&!retained){assert.equal(before.order[0],1);assert.equal(before.order[1],2);assert.equal(after.order[0],2);assert.ok(after.order.indexOf(1)>after.order.indexOf(3),'recovered channel did not move down');result.checks.push('pause before increase; recovered channel moves down; same-priority order stable')}
 result.position={before,after};result.checks.push('refresh preserves channel identity, details, filter, nested scroll and reading position after sorting');
 if(server){
  failNext=true;await b.evaluate('groupGovernanceActivate()');await b.waitFor("ggAlert.classList.contains('bad')&&ggRows.getAttribute('aria-busy')==='false'");assert.ok(Math.abs((await position()).top-after.top)<3);assert.equal(await b.evaluate('ggAdjust.innerText'),'暂不可判断');await refresh();assert.ok(Math.abs((await position()).top-after.top)<3);result.checks.push('failed refresh retains evidence; recovery and header transitions preserve reading position');
 }
 const count=await b.evaluate('__ggReads.length');await b.evaluate("window.dispatchEvent(new Event('focus'))");await b.waitFor(`__ggReads.length>${count}&&ggRows.getAttribute('aria-busy')==='false'`,15000);assert.deepEqual((await position()).open,before.open);
 await b.call('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:false});await b.evaluate(`document.querySelector(${JSON.stringify(summary)}).scrollIntoView({block:'start'})`);assert.ok(await b.evaluate('document.documentElement.scrollWidth<=innerWidth+1'),'horizontal page overflow');await b.screenshot(path.join(path.dirname(out),'channel-narrow.png'));
 result.checks.push('390px layout and focus refresh');
 const sampleChannel=rt.channels.find(c=>c.error_reasons?.some(r=>r.samples?.some(s=>s.request_id)));
 if(sampleChannel){
  await b.input('#ggSearch',sampleChannel.group);await open(['group',sampleChannel.group]);const sampleID=await open(['channel',sampleChannel.group,sampleChannel.channel_id]);await open(['basis',sampleChannel.key]);
  const sample=sampleChannel.error_reasons.flatMap(r=>r.samples||[]).find(s=>s.request_id),count=await b.evaluate('__logchainQueries');await b.click(sampleID+' [data-gg-request]');if(server){const nav=await b.evaluate('__navigations.at(-1)');assert.equal(nav[1].preset,'group_governance');assert.equal(nav[1].request_id,sample.request_id);assert.equal(nav[1].model_name||nav[1].model,sample.model)}else{await b.waitFor("!document.getElementById('tab-logchain').hidden&&/仅预填/.test(lcStatus.innerText)");await b.pause(1000)}assert.equal(await b.evaluate('__logchainQueries'),count);result.checks.push('exact request/model navigation only prefills troubleshooting');
 }
 assert.equal(b.external.length,0);assert.equal(b.exceptions.length,0);if(server)assert.ok(requests.every(url=>['/','/favicon.ico','/group-governance/report','/group-governance.js','/group-governance.css','/view-refresh.js'].includes(url)));
 result.browser=b.version.product;result.pass=true;
}catch(e){result.error=e.message;throw e}finally{fs.writeFileSync(out,JSON.stringify(result,null,2));await b.close();if(server)await new Promise(r=>server.close(r));console.log(JSON.stringify(result))}
