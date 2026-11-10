// Real Chrome/CDP regression against the complete repository page and local synthetic APIs.
// No live Monitor, production data, external network or input.value assignments.
// Usage: node dev/test-logchain-time-picker-browser.mjs --browser <chrome> --out <directory>
import assert from 'node:assert/strict';
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
const html=readFileSync(path.join(root,'monitor/page.html'),'utf8');
const embedSource=readFileSync(path.join(root,'monitor/server.go'),'utf8');
const embedMap=new Map([...embedSource.matchAll(/\/\/go:embed ([^\r\n]+)\r?\nvar (\w+)/g)].map(m=>[m[2],m[1]]));
const assets=new Map([...embedSource.matchAll(/r\.GET\("([^"\n]+\.(?:js|css))",[\s\S]*?c\.Data\(http\.StatusOK, "[^"]+", (?:\[\]byte\()?([\w]+)\)?\)/g)].map(m=>[m[1],embedMap.get(m[2])]));
const requests=[],checks=[],exceptions=[],external=[],consoleErrors=[];
let scenario='complete',taskSeq=0;
const tasks=new Map();
function json(res,status,data){res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(data))}
function result(task,status){return {investigation_id:task.id,status,source_status:status==='partial'
  ?[{source:'cloudfront',status:'disabled'},{source:'nginx',status:'unavailable'},{source:'newapi_database',status:'empty'}]:[],
  summary:{classification:'inconclusive',evidence_level:'unavailable',conclusion:'本地合成证据不足，不能确定责任方'},
  observability:{schema_version:'observability.v1',from:task.body.from,to:task.body.to,events:[],source_quality:[],summary:{evidence_level:'unavailable',candidate:false,conclusion:'本地合成证据不足，不能确定责任方'}}}}
const server=createServer(async(req,res)=>{
  const url=new URL(req.url,'http://127.0.0.1');
  const chunks=[];for await(const c of req)chunks.push(c);
  const text=Buffer.concat(chunks).toString();
  requests.push({method:req.method,path:url.pathname,body:text?JSON.parse(text):null,scenario});
  if(url.pathname==='/'){res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});res.end(html);return}
  if(assets.has(url.pathname)){
    const file=assets.get(url.pathname);assert.ok(file,'static asset mapping '+url.pathname);
    res.writeHead(200,{'Content-Type':url.pathname.endsWith('.css')?'text/css':'application/javascript'});
    res.end(readFileSync(path.join(root,'monitor',file)));return;
  }
  if(url.pathname==='/me'){json(res,200,{name:'Local browser fixture',is_root:false});return}
  if(url.pathname==='/api/brand'){json(res,200,{name:'离线交互验证'});return}
  if(url.pathname==='/data'){json(res,200,{enabled:false});return}
  if(url.pathname==='/logchain/requests'){json(res,200,{rows:[],has_more:false,cloudwatch_enabled:false,scope:{}});return}
  if(url.pathname==='/logchain/filters'){json(res,200,{groups:[],channels:[],domains:[]});return}
  if(url.pathname==='/logchain/investigations'&&req.method==='POST'){
    if(scenario==='create-network'){req.socket.destroy();return}
    if(scenario==='create-http'){json(res,503,{error:'本地模拟：来源不可用'});return}
    const id='fixture-'+(++taskSeq),task={id,body:JSON.parse(text),scenario,polls:0,cancelled:false};tasks.set(id,task);
    if(scenario==='slow-create')await new Promise(r=>setTimeout(r,1200));
    json(res,202,{investigation_id:id,poll_after_ms:200});return;
  }
  const match=url.pathname.match(/^\/logchain\/investigations\/(fixture-\d+)(\/cancel)?$/);
  if(match){
    const task=tasks.get(match[1]);assert.ok(task,'known fixture task');
    if(match[2]){
      if(task.scenario==='cancel-race'){await new Promise(r=>setTimeout(r,1200));json(res,503,{error:'本地模拟：迟到的取消失败'});return}
      if(task.scenario==='cancel-error'&&!task.cancelAttempted){task.cancelAttempted=true;json(res,503,{error:'本地模拟：取消暂不可用'});return}
      task.cancelled=true;json(res,200,result(task,'cancelled'));return;
    }
    task.polls++;
    if(task.scenario==='poll-network'){req.socket.destroy();return}
    if(task.scenario==='poll-http'){json(res,503,{error:'本地模拟：读取任务失败'});return}
    const pending=['cancel','cancel-error','close-running'].includes(task.scenario);
    if(task.scenario==='cancel'&&task.polls===2)await new Promise(r=>setTimeout(r,1100));
    json(res,200,result(task,task.cancelled?'cancelled':task.polls===1||pending?'running':task.scenario==='cancel-race'?'complete':task.scenario));return;
  }
  // Unrelated dashboard reads are inert synthetic empty payloads; writes fail closed.
  if(req.method!=='GET'){json(res,405,{error:'fixture has no write route'});return}
  json(res,200,{});
});
server.listen(0,'127.0.0.1');await once(server,'listening');
const origin='http://127.0.0.1:'+server.address().port;
const browser=spawn(option('--browser'),['--headless=new','--remote-debugging-pipe','--no-first-run','--no-default-browser-check','--disable-background-networking','--disable-component-update','--disable-sync','--disable-default-apps','--disable-client-side-phishing-detection','--metrics-recording-only','--lang=zh-CN','--window-size=1280,950',`--user-data-dir=${profile}`,'about:blank'],{stdio:['ignore','pipe','pipe','pipe','pipe'],windowsHide:true});
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
async function state(){return evaluate(`({inputs:['From','To'].map(s=>{const e=document.getElementById('lcInvestigation'+s),b=document.getElementById('lcInvestigation'+s+'Pick'),r=b.getBoundingClientRect();return {value:e.value,disabled:e.matches(':disabled'),readOnly:e.readOnly,buttonDisabled:b.matches(':disabled'),buttonWidth:r.width,buttonHeight:r.height}}),panelHidden:document.getElementById('lcInvestigationPanel').hidden,dialogOpen:document.getElementById('lcInvestigationTimeDialog').open,submitDisabled:document.getElementById('lcInvestigationSubmit').disabled,result:document.getElementById('lcInvestigationResult').innerText})`)}
async function unlocked(){const s=await state();assert.ok(s.inputs.every(i=>!i.disabled&&!i.readOnly&&!i.buttonDisabled)&&!s.submitDisabled,JSON.stringify(s));return s}
async function choose(field,date,hour,minute,shot){
  await click('#lcInvestigation'+field+'Pick');await waitFor("document.getElementById('lcInvestigationTimeDialog').open");
  const [y,m,d]=date.split('-').map(Number);
  await text('#lcInvestigationCalendar .cur-year',String(y));
  if(args.includes('--trace'))console.log('year filled',await evaluate("({active:document.activeElement?.outerHTML,year:document.querySelector('#lcInvestigationCalendar .cur-year')?.value})"));
  await select('#lcInvestigationCalendar .flatpickr-monthDropdown-months',m-1);
  const selector=await evaluate(`(()=>{const a=[...document.querySelectorAll('#lcInvestigationCalendar .flatpickr-day')];const i=a.findIndex(e=>e.textContent===${JSON.stringify(String(d))}&&!e.classList.contains('prevMonthDay')&&!e.classList.contains('nextMonthDay'));if(i<0)throw Error('date missing');return '#lcInvestigationCalendar .flatpickr-day:nth-child('+(i+1)+')'})()`);
  await click(selector);
  await select('#lcInvestigationHour',hour);await select('#lcInvestigationMinute',minute);
  if(shot)await screenshot(shot);
  await click('#lcInvestigationTimeApply');
  const expected=date+'T'+String(hour).padStart(2,'0')+':'+String(minute).padStart(2,'0');
  assert.equal(await evaluate(`document.getElementById('lcInvestigation${field}').value`),expected);
  return expected;
}
async function open(){await click('#lcInvestigationOpen');await unlocked()}
async function terminal(mode){scenario=mode;await click('#lcInvestigationSubmit');await waitFor("!document.getElementById('lcInvestigationSubmit').disabled");return unlocked()}
async function check(name,fn){const start=Date.now();try{const evidence=await fn();checks.push({name,status:'pass',ms:Date.now()-start,evidence});console.log('PASS '+name)}catch(e){checks.push({name,status:'fail',error:e.stack});throw e}}
let version,failure;
const watchdog=setTimeout(()=>browser.kill(),180000);
try{
  version=await call('Browser.getVersion',{},null);
  const {targetId}=await call('Target.createTarget',{url:'about:blank'},null);({sessionId}=await call('Target.attachToTarget',{targetId,flatten:true},null));
  await call('Page.enable');await call('Runtime.enable');await call('Fetch.enable',{patterns:[{urlPattern:'http*'}]});
  await call('Emulation.setDeviceMetricsOverride',{width:1280,height:950,deviceScaleFactor:1,mobile:false});
  await call('Emulation.setTimezoneOverride',{timezoneId:'Asia/Shanghai'});
  await call('Page.navigate',{url:origin+'/#tab=logchain'});
  await waitFor("!document.getElementById('tab-logchain')?.hidden && !!document.getElementById('lcInvestigationOpen')");
  await check('first open, visible time buttons, no enabled source required',async()=>{await open();await screenshot('10-first-open');const s=await state();assert.ok(s.inputs.every(i=>!i.value&&i.buttonHeight>=40));return s});
  await check('mouse selects and modifies complete start/end dates, hours and minutes',async()=>{
    await choose('From','2025-12-31',23,30);
    await choose('From','2026-10-08',23,30,'11-start-calendar');
    await choose('To','2026-10-09',1,30,'12-end-calendar');
    await choose('From','2026-10-08',23,29);await choose('From','2026-10-08',23,30);
    await text('#lcInvestigationRequestID','fixture-request-only');await screenshot('13-selected-range');return state();
  });
  await check('UTC POST, exact two-hour cross-day range, running poll and complete result',async()=>{
    scenario='complete';await click('#lcInvestigationSubmit');
    await waitFor("document.getElementById('lcInvestigationResult').innerText.includes('任务状态：查询中')");
    const loading=await state();assert.ok(loading.inputs.every(i=>i.disabled&&i.buttonDisabled));
    await waitFor("document.getElementById('lcInvestigationResult').innerText.includes('任务状态：已完成')");
    const s=await unlocked(),task=[...tasks.values()].at(-1);
    assert.equal(task.body.from,'2026-10-08T15:30:00.000Z');assert.equal(task.body.to,'2026-10-08T17:30:00.000Z');assert.equal(task.body.timezone,'Asia/Shanghai');assert.equal(task.body.schema_version,'observability.v1');assert.ok(task.polls>=2);
    assert.match(s.result,/北京时间，左闭右开/);await screenshot('14-complete');return {body:task.body,polls:task.polls,state:s};
  });
  await check('calendar cancel and Escape preserve typed values',async()=>{
    const before=(await state()).inputs[0].value;await click('#lcInvestigationFromPick');await select('#lcInvestigationHour',9);await click('#lcInvestigationTimeCancel');assert.equal((await state()).inputs[0].value,before);
    await click('#lcInvestigationFromPick');await key('Escape');assert.equal((await state()).dialogOpen,false);assert.equal((await state()).inputs[0].value,before);
  });
  await check('partial result exposes disabled and unavailable sources before details',async()=>{const s=await terminal('partial');assert.match(s.result,/cloudfront：该来源未启用/);assert.match(s.result,/nginx：数据源不可用/);assert.match(s.result,/缺少的证据不能用于排除故障/);await screenshot('15-source-unavailable');return s});
  for(const mode of ['failed','create-http','create-network','poll-http','poll-network'])await check(mode+' unlocks for another selection/query',async()=>{const s=await terminal(mode);assert.match(s.result,/失败|不可用/);await click('#lcInvestigationToPick');assert.equal((await state()).dialogOpen,true);await key('Escape');return s});
  await check('cancel request, late poll and retry after failed cancel',async()=>{
    for(const mode of ['cancel','cancel-error']){
      scenario=mode;await click('#lcInvestigationSubmit');await waitFor("document.getElementById('lcInvestigationResult').innerText.includes('任务状态：查询中')");
      if(mode==='cancel')await pause(850);
      await click('#lcInvestigationResult [data-lc-cw-cancel]');
      if(mode==='cancel-error'){
        await waitFor("document.getElementById('lcInvestigationResult').innerText.includes('取消失败')");assert.ok((await state()).submitDisabled);await click('#lcInvestigationResult [data-lc-cw-cancel]');
      }
      await waitFor("document.getElementById('lcInvestigationResult').innerText.includes('任务状态：已取消')");await unlocked();await pause(1300);assert.match((await state()).result,/任务状态：已取消/);
    }
    await screenshot('16-cancelled');return state();
  });
  await check('terminal poll followed by late cancel error keeps completed inputs editable',async()=>{
    scenario='cancel-race';await click('#lcInvestigationSubmit');
    await waitFor("document.getElementById('lcInvestigationResult').innerText.includes('任务状态：查询中')");
    await click('#lcInvestigationResult [data-lc-cw-cancel]');
    await waitFor("document.getElementById('lcInvestigationResult').innerText.includes('任务状态：已完成')");await unlocked();
    await waitFor("document.getElementById('lcInvestigationResult').innerText.includes('迟到的取消失败')");
    const s=await unlocked();assert.match(s.result,/任务状态：已完成/);await screenshot('19-cancel-after-complete');return s;
  });
  await check('close during running poll clears input and ignores late results',async()=>{
    scenario='close-running';await click('#lcInvestigationSubmit');
    await waitFor("document.getElementById('lcInvestigationResult').innerText.includes('任务状态：查询中')");
    await click('#lcInvestigationClose');await open();await pause(1000);
    const s=await unlocked();assert.equal(s.result,'');assert.ok(s.inputs.every(i=>!i.value));
    await choose('From','2026-10-08',8,0);await choose('To','2026-10-08',9,0);await text('#lcInvestigationRequestID','fixture-reopened');return s;
  });
  await check('close while creating, reopen, no stale disable or late result',async()=>{
    scenario='slow-create';await click('#lcInvestigationSubmit');await click('#lcInvestigationClose');await open();await pause(1400);
    const s=await unlocked();assert.ok(s.inputs.every(i=>!i.value));assert.equal(s.result,'');return s;
  });
  await check('keyboard edits all native date/time segments after reopening',async()=>{
    for(const field of ['From','To']){
      const p=await evaluate(`(()=>{const e=document.getElementById('lcInvestigation${field}');e.scrollIntoView({block:'center'});const r=e.getBoundingClientRect();return {x:r.x+22,y:r.y+r.height/2}})()`);
      await call('Input.dispatchMouseEvent',{type:'mousePressed',...p,button:'left',clickCount:1});await call('Input.dispatchMouseEvent',{type:'mouseReleased',...p,button:'left',clickCount:1});
      for(let n=0;n<5;n++){await key('ArrowUp');await key('Tab')}
    }
    const s=await state();assert.ok(s.inputs.every(i=>/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(i.value)));await screenshot('17-keyboard');return s;
  });
  await check('320px and 375px dark viewport: both buttons, calendar, clock and apply stay clickable',async()=>{
    const sizes=[];
    for(const width of [320,375]){
      await call('Emulation.setDeviceMetricsOverride',{width,height:740,deviceScaleFactor:1,mobile:false});
      await choose('From','2026-10-08',8,15,'18-narrow-'+width);
      await choose('To','2026-10-08',9,45);
      await click('#lcInvestigationToPick');
      const box=await evaluate(`(()=>{const e=document.getElementById('lcInvestigationTimeDialog'),r=e.getBoundingClientRect();const b=document.getElementById('lcInvestigationToPick').getBoundingClientRect();return {left:r.left,right:r.right,top:r.top,bottom:r.bottom,width:innerWidth,scrollWidth:e.scrollWidth,clientWidth:e.clientWidth,buttonLeft:b.left,buttonRight:b.right}})()`);
      assert.ok(box.left>=0&&box.right<=width&&box.scrollWidth<=box.clientWidth,JSON.stringify(box));assert.ok(box.buttonLeft>=0&&box.buttonRight<=width,JSON.stringify(box));sizes.push(box);await key('Escape');
    }
    return sizes;
  });
  await check('foreign browser timezone and DST gap preserve Beijing selection and UTC conversion',async()=>{
    await call('Emulation.setDeviceMetricsOverride',{width:1280,height:950,deviceScaleFactor:1,mobile:false});
    await call('Emulation.setTimezoneOverride',{timezoneId:'America/New_York'});
    await choose('From','2026-03-08',2,30);await choose('To','2026-03-08',3,30);await text('#lcInvestigationPath','/v1/responses');
    await terminal('complete');const task=[...tasks.values()].at(-1);assert.equal(task.body.from,'2026-03-07T18:30:00.000Z');assert.equal(task.body.to,'2026-03-07T19:30:00.000Z');return task.body;
  });
  await check('equal, reversed, >2h windows reject without POST; exactly 2h remains allowed',async()=>{
    await choose('From','2026-10-08',8,0);const count=()=>requests.filter(r=>r.method==='POST'&&r.path==='/logchain/investigations').length;
    const start=count();
    for(const [h,m] of [[8,0],[7,59],[10,1]]){await choose('To','2026-10-08',h,m);await click('#lcInvestigationSubmit');assert.match((await unlocked()).result,/时间范围必须大于 0 且不超过 2 小时/);assert.equal(count(),start)}
    await choose('To','2026-10-08',10,0);await terminal('complete');assert.equal(count(),start+1);
    await click('#lcInvestigationClose');await open();await click('#lcInvestigationSubmit');assert.match((await unlocked()).result,/填写完整/);assert.equal(count(),start+1);
  });
  assert.equal(external.length,0,'no external network attempted');assert.equal(exceptions.length,0,JSON.stringify(exceptions));
}catch(e){failure=e;try{await screenshot('99-failure')}catch{}console.error(e.stack);process.exitCode=1}
finally{
  clearTimeout(watchdog);
  writeFileSync(path.join(out,'browser-results.json'),JSON.stringify({scope:'Complete repository HTML and production JS/CSS, real headless Chrome mouse/keyboard events; all APIs synthetic on loopback; no production/8204 or live data.',browser:version?.product,profile,checks,requests,exceptions,external,consoleErrors,failure:failure?.stack},null,2));
  try{await call('Browser.close',{},null)}catch{browser.kill()}
  for(const p of pending.values())clearTimeout(p.timer);
  server.closeAllConnections();server.close();
}
