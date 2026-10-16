import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
function install(ctx,name,async=false){
  const source=page.match(new RegExp(`^${async?'async ':''}function ${name}\\([^]*?^}`,'m'));
  assert.ok(source,name);vm.runInContext(source[0],ctx);
}
const id='a'.repeat(32),hash='b'.repeat(64);
const task=(state='queued',rows=0)=>({id,state,rows,total:2,bytes:100,sha256:hash});
function client(request,wait=async()=>{}){
  const events=[],ctx=vm.createContext({});install(ctx,'createBillExportClient');
  const c=ctx.createBillExportClient(request,wait,v=>events.push(v));
  return {c,events};
}
test('one create, bounded progress, ready only after verification',async()=>{
  const calls=[],queue=[task(),task('running',1),task('running',2),task('ready',2)];
  const {c,events}=client(async(path,method,body)=>{calls.push({path,method,body});return {task:queue.shift()};});
  await c.start('ticket');
  assert.equal(calls.filter(v=>v.method==='POST').length,1);
  assert.deepEqual(events.map(v=>v.state),['creating','queued','running','running','ready']);
  assert.equal(c.pending(),false);
});
test('cancel during create waits for ID and confirms server cancellation',async()=>{
  let release;const calls=[];
  const {c,events}=client(async(path,method)=>{
    calls.push(path);
    if(path.endsWith('/cancel'))return {ok:true};
    return new Promise(resolve=>{release=()=>resolve({task:task()});});
  });
  const running=c.start('ticket');c.cancel();
  assert.equal(events.at(-1).state,'creating');
  release();await running;
  assert.equal(calls.at(-1),'/api/logs/bill/tasks/'+id+'/cancel');
  assert.equal(events.at(-1).state,'canceled');assert.equal(c.pending(),false);
});
test('status interruption resumes same ID without a second create',async()=>{
  let n=0;const calls=[];
  const {c,events}=client(async(path,method)=>{
    calls.push({path,method});n++;
    if(n===1)return {task:task()};
    if(n===2)throw new Error('offline');
    return {task:task('ready',2)};
  });
  await c.start('ticket');assert.equal(events.at(-1).state,'paused');assert.equal(c.pending(),true);
  await c.resume();assert.equal(events.at(-1).state,'ready');
  assert.equal(calls.filter(v=>v.method==='POST').length,1);
});
test('failed cancel is not reported as canceled and can retry',async()=>{
  let cancelAttempts=0,release;
  const {c,events}=client(async path=>{
    if(path.endsWith('/cancel')){if(++cancelAttempts===1)throw new Error('offline');return {ok:true};}
    return new Promise(resolve=>{release=()=>resolve({task:task()});});
  });
  const p=c.start('ticket');c.cancel();release();await p;
  assert.equal(events.at(-1).state,'paused');assert.equal(c.pending(),true);
  await c.cancel();assert.equal(cancelAttempts,2);assert.equal(events.at(-1).state,'canceled');
});
test('revocation or expired task is terminal; no fallback/new generation',async()=>{
  for(const status of [401,403,404]){
    let calls=0;
    const {c,events}=client(async()=>{if(++calls===1)return {task:task()};const e=new Error('unavailable');e.status=status;throw e;});
    await c.start('ticket');assert.equal(events.at(-1).state,'failed');assert.equal(c.pending(),false);assert.equal(calls,2);
  }
});
test('malformed metadata cannot become a downloadable file',async()=>{
  for(const change of [{id:'../../bad'},{state:'done'},{rows:3},{total:50001},{rows:1,state:'ready'},{state:'ready',rows:2,sha256:''},{state:'ready',rows:2,bytes:0}]){
    const {c,events}=client(async()=>({task:{...task(),...change}}));
    await c.start('ticket');assert.equal(events.at(-1).state,'failed');assert.ok(!events.some(v=>v.state==='ready'));
  }
});
test('changed ID or count after create is refused',async()=>{
  for(const change of [{id:'c'.repeat(32)},{total:3}]){
    let calls=0;const {c,events}=client(async()=>({task:++calls===1?task():{...task('ready',2),...change}}));
    await c.start('ticket');assert.equal(events.at(-1).state,'paused');assert.ok(!events.some(v=>v.state==='ready'));
  }
});
test('polling is bounded and terminal failure does not expose raw text',async()=>{
  let calls=0;const {c,events}=client(async()=>{calls++;return {task:task()};});
  await c.start('ticket');assert.equal(calls,61);assert.equal(events.at(-1).state,'paused');
  const failed=client(async()=>({task:{...task('failed'),failure_code:'secret raw error'}}));
  await failed.c.start('ticket');assert.doesNotMatch(failed.events.at(-1).message,/secret/);
});
test('double start while busy does not duplicate jobs',async()=>{
  let release,calls=0;
  const {c}=client(async()=>{calls++;return new Promise(resolve=>{release=()=>resolve({task:task('ready',2)});});});
  const p=c.start('ticket');await c.start('ticket2');release();await p;assert.equal(calls,1);
});

test('lost creation reply is explicitly unconfirmed, never falsely canceled or downloaded',async()=>{
  const {c,events}=client(async()=>{throw new Error('offline');});
  await c.start('ticket');assert.equal(events.at(-1).state,'unconfirmed');assert.equal(c.pending(),false);
  assert.ok(!events.some(v=>['ready','canceled'].includes(v.state)));
});

test('last bounded poll can complete without an extra resume',async()=>{
  let n=0;const {c,events}=client(async()=>({task:++n===61?task('ready',2):task()}));
  await c.start('ticket');assert.equal(events.at(-1).state,'ready');assert.equal(n,61);
});

test('transport sends only signed ticket and hides arbitrary server error text',async()=>{
  const calls=[];
  const ctx=vm.createContext({AbortController,setTimeout,clearTimeout,fetch:async(path,options)=>{
    calls.push({path,options});return {ok:false,status:503,json:async()=>({error:'secret SQL and credential'})};
  }});
  install(ctx,'billTaskRequest',true);
  await assert.rejects(ctx.billTaskRequest('/api/logs/bill/tasks','POST',{ticket:'signed'}),e=>e.status===503&&!e.message.includes('secret'));
  const call=calls[0];assert.equal(call.options.body,'{"ticket":"signed"}');
  assert.equal(call.options.credentials,'same-origin');assert.equal(call.options.cache,'no-store');
  assert.equal(call.options.headers['X-Usage-Bill-Request'],'1');
});

test('complete production inline script parses and export dialog is width-bounded',()=>{
  for(const match of page.matchAll(/<script(?:\s[^>]*)?>([^]*?)<\/script>/g))new vm.Script(match[1]);
  assert.match(page,/#expMask \.modal\{max-width:calc\(100vw - 32px\)\}/);
});
test('page shows download action only for ready and never buffers CSV',()=>{
  const els=new Map(),links=[];
  const ctx=vm.createContext({$:id=>{if(!els.has(id))els.set(id,{style:{}});return els.get(id);},
    setExpBar(){},expStatus(){},closeExp(){},resumeBillExport(){},rememberBillTask(){},billExportClient:{cancel(){}},exporting:false,
    document:{body:{appendChild(){}},createElement(){return {click(){links.push(this.href);},remove(){}};}}});
  install(ctx,'renderBillTask');
  for(const state of ['creating','queued','running','canceling','failed','canceled']){
    ctx.renderBillTask({state,task:task(state),message:''});assert.equal(els.get('expAction').hidden,true);
  }
  ctx.renderBillTask({state:'ready',task:task('ready',2)});
  assert.equal(links.length,0);assert.equal(els.get('expAction').textContent,'下载账单');
  els.get('expAction').onclick();assert.deepEqual(links,['/api/logs/bill/tasks/'+id+'/download']);
  const block=page.slice(page.indexOf('function createBillExportClient'),page.indexOf('function setExpBar'));
  assert.doesNotMatch(block,/new Blob|\.arrayBuffer\(|\.blob\(/);
});

test('fresh client restores server task with GET only, never trusts cached state',async()=>{
  for(const state of ['queued','running','ready','failed','canceled']){
    const calls=[];
    const {c,events}=client(async(path,method)=>{
      calls.push({path,method});return {task:task(calls.length===1?state:'ready',state==='ready'||calls.length>1?2:0)};
    });
    await c.restore(id);
    assert.ok(calls.every(v=>v.method==='GET'&&v.path==='/api/logs/bill/tasks/'+id));
    assert.equal(events[0].state,'restoring');
    assert.equal(events.at(-1).state,['failed','canceled'].includes(state)?state:'ready');
  }
});
test('restore transient error can resume same ID; revoked or expired is terminal',async()=>{
  for(const status of [503,429,401,403,404]){
    let n=0;const calls=[];
    const {c,events}=client(async(path,method)=>{
      calls.push({path,method});if(++n===1){const e=new Error('unavailable');e.status=status;throw e;}
      return {task:task('ready',2)};
    });
    await c.restore(id);
    const transient=[503,429].includes(status);
    assert.equal(events.at(-1).state,transient?'paused':'failed');
    assert.equal(c.pending(),transient);
    if(transient){await c.resume();assert.equal(events.at(-1).state,'ready');}
    assert.ok(calls.every(v=>v.method==='GET'&&v.path.endsWith(id)));
  }
});
test('restore rejects unsafe ID and mismatched or malformed ready response',async()=>{
  let calls=0;
  const bad=client(async()=>{calls++;return {task:task('ready',2)};});
  await bad.c.restore('../bad');assert.equal(calls,0);assert.equal(bad.events.at(-1).state,'failed');
  for(const change of [{id:'c'.repeat(32)},{rows:1},{sha256:''},{bytes:0}]){
    const {c,events}=client(async()=>({task:{...task('ready',2),...change}}));
    await c.restore(id);assert.ok(!events.some(v=>v.state==='ready'));
  }
});
test('tab storage contains only bounded task ID and timestamp; blocked storage is harmless',()=>{
  const data=new Map();const storage={getItem:k=>data.get(k)||null,setItem:(k,v)=>data.set(k,v),removeItem:k=>data.delete(k)};
  let now=1000000;const ctx=vm.createContext({});install(ctx,'createBillTaskMemory');
  const memory=ctx.createBillTaskMemory(()=>storage,()=>now);
  assert.equal(memory.read(),'');memory.save(id);assert.equal(memory.read(),id);
  assert.deepEqual(JSON.parse(data.values().next().value),{id,savedAt:now});
  now+=600000;assert.equal(memory.read(),'');assert.equal(data.size,0);
  for(const v of [{id:'../bad',savedAt:now},{id,savedAt:now+1},{id,savedAt:'1'},null]){
    data.set('usage-bill-task-v1',JSON.stringify(v));assert.equal(memory.read(),'');
  }
  const blocked=ctx.createBillTaskMemory(()=>{throw new Error('denied');});
  blocked.save(id);assert.equal(blocked.read(),'');blocked.clear();
});
test('restoration is a separate explicit action, not silently applied to current filters',()=>{
  assert.match(page,/id="billResumeBtn" hidden title="恢复本标签页上次的账单任务/);
  assert.match(page,/\.exp-btn\[hidden\]\{display:none\}/);
  assert.match(page,/这是上次导出范围的账单，不应用当前筛选/);
  const start=page.match(/^async function startExport\([^]*?^}/m)[0];
  assert.doesNotMatch(start,/billTaskMemory\.read|restoreBillExport/);
});

test('reload drops all task state; memory restores only after fresh server authorization',async()=>{
  const data=new Map(),storage={getItem:k=>data.get(k)||null,setItem:(k,v)=>data.set(k,v),removeItem:k=>data.delete(k)};
  function tab(request){
    const els=new Map(),events=[];
    const ctx=vm.createContext({window:{sessionStorage:storage},exporting:false,
      $:key=>{if(!els.has(key))els.set(key,{});return els.get(key);}});
    for(const fn of ['createBillTaskMemory','updateBillResumeButton','rememberBillTask','createBillExportClient'])install(ctx,fn);
    vm.runInContext('const billTaskMemory=createBillTaskMemory(()=>window.sessionStorage);updateBillResumeButton();',ctx);
    const c=ctx.createBillExportClient(request,async()=>{},v=>{ctx.rememberBillTask(v);events.push(v);});
    return {c,events,els,id:()=>vm.runInContext('billTaskMemory.read()',ctx)};
  }
  const first=tab(async()=>({task:task('ready',2)}));
  assert.equal(first.els.get('billResumeBtn').hidden,true);
  await first.c.start('signed-secret-ticket');assert.equal(first.els.get('billResumeBtn').hidden,false);
  assert.doesNotMatch([...data.values()].join(''),/signed-secret-ticket|sha256|total|bytes/);
  let calls=0;
  const reload=tab(async(path,method)=>{calls++;assert.equal(method,'GET');assert.ok(path.endsWith(id));return {task:task('ready',2)};});
  assert.equal(calls,0);assert.equal(reload.events.length,0);assert.equal(reload.els.get('billResumeBtn').hidden,false);
  await reload.c.restore(reload.id());assert.equal(calls,1);assert.equal(reload.events.at(-1).state,'ready');
  const switchedUser=tab(async()=>{const e=new Error('not found');e.status=404;throw e;});
  await switchedUser.c.restore(switchedUser.id());
  assert.equal(switchedUser.events.at(-1).state,'failed');assert.equal(switchedUser.els.get('billResumeBtn').hidden,true);
  assert.equal(data.size,0);
});
