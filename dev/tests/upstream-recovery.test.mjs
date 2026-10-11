import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8');
const start=source.indexOf('function renderUpstreamRecoveryOptions(');
const end=source.indexOf('function upstreamBalanceSummary(',start);
assert.ok(start>=0&&end>start);
const response=(data,status=200)=>({status,ok:status===200,json:async()=>data});
const snapshot=(state='pending',success=100)=>({domain:'fixture.example',account_epoch:'epoch-1',tasks:[{task:'usage',label:'消费账单',state,can_recover:state==='failed',last_success_at:success}]});

function setup(fetcher=async()=>response(snapshot())){
  const elements=new Map(),messages=[],requests=[],timers=new Map();let timerID=0;
  const $=id=>{
    if(!elements.has(id))elements.set(id,{disabled:false,hidden:false,value:'',innerHTML:'',textContent:''});
    return elements.get(id);
  };
  const context=vm.createContext({$,AbortController,location:{href:''},
    setTimeout:(fn,ms)=>{timers.set(++timerID,{fn,ms});return timerID},clearTimeout:id=>timers.delete(id),
    cm:{loaded:true,upstreamDomain:{domain:'fixture.example'},upstreamConfig:{provider:'newapi'}},
    dateTime:ts=>`time:${ts}`,esc:s=>String(s??'').replaceAll('<','&lt;').replaceAll('"','&quot;'),
    showUpstreamMessage:(message,warning)=>messages.push({message,warning:!!warning}),
    fetch:async(url,options)=>{requests.push({url,options});return fetcher(url,options);},initial:snapshot('failed'),
  });
  vm.runInContext(`let upstreamRecoverySeq=0,upstreamRecoveryAbort=null;
    let upstreamRecoveryStatusSeq=0,upstreamRecoveryStatusAbort=null,upstreamRecoveryTimer=null,upstreamRecoveryData=initial,upstreamRecoveryWatch=null;
    ${source.slice(start,end)}`,context);
  return {context,$,messages,requests,timers,run:()=>vm.runInContext("recoverUpstreamTask('usage')",context),
    refresh:()=>vm.runInContext('loadUpstreamRecoveryStatus()',context),
    tick:async()=>{const entry=[...timers].find(([,t])=>t.ms===5000);assert.ok(entry,'poll expected');timers.delete(entry[0]);await entry[1].fn();}};
}

for(const status of ['queued','waiting','blocked','not_needed','unknown']){
  test(`recovery feedback: ${status}`,async()=>{
    const f=setup(async url=>response(url==='/channels/upstream/recover'?{status,message:'fixture message',queued_at:200,retry_at:status==='waiting'?123:2**62}:snapshot()));
    f.$('cmUpstreamBaseURL').value='unsaved input';await f.run();
    assert.equal(f.requests.length,status==='unknown'?1:2);
    assert.deepEqual(JSON.parse(f.requests[0].options.body),{domain:'fixture.example',task:'usage'});
    assert.equal(f.messages.at(-1).warning,status==='blocked'||status==='unknown');
    assert.doesNotMatch(f.messages.at(-1).message,/账单已更新|余额已更新|数据已补齐|Invalid Date/);
    if(status==='waiting')assert.match(f.messages.at(-1).message,/time:123/);
    else assert.doesNotMatch(f.messages.at(-1).message,/下次可检测/);
    assert.equal(f.context.cm.loaded,status!=='queued');
    assert.equal(f.$('cmUpstreamBaseURL').value,'unsaved input');
    if(status!=='unknown'){
      assert.equal(f.requests[1].url,'/channels/upstream/recovery-status?domain=fixture.example');
      assert.equal(f.requests[1].options.cache,'no-store');
    }
    assert.equal([...f.timers.values()].filter(t=>t.ms===5000).length,status==='queued'?1:0);
  });
}

test('duplicate clicks suppressed and stale domain POST ignored',async()=>{
  let resolve;const f=setup(()=>new Promise(r=>{resolve=r}));
  const first=f.run();await f.run();assert.equal(f.requests.length,1);
  f.context.cm.upstreamDomain={domain:'other.example'};
  resolve(response({status:'queued',message:'stale result',queued_at:200}));await first;
  assert.ok(!f.messages.some(m=>m.message==='stale result'));assert.equal(f.context.cm.loaded,true);
});

test('failed local refresh preserves queued result, never permits stale recovery action',async()=>{
  for(const reply of [response({},500),response({tasks:null}),response({tasks:[]})]){
    const f=setup(async url=>url==='/channels/upstream/recover'?response({status:'queued',message:'已恢复排期',queued_at:200}):reply);
    await f.run();assert.equal(f.context.cm.loaded,false);
    assert.equal(f.requests.length,2);assert.match(f.messages.at(-1).message,/已恢复排期/);
    assert.match(f.$('cmUpstreamRecoveryRows').textContent,/状态读取失败/);
    await f.run();assert.equal(f.requests.length,2);assert.equal(f.timers.size,0);
  }
});

test('late status response cannot update another or closed account dialog',async()=>{
  for(const close of [false,true]){
    let resolve;const f=setup(()=>new Promise(r=>{resolve=r}));
    const first=f.refresh();f.context.cm.upstreamDomain=close?null:{domain:'other.example'};
    resolve(response(snapshot('synced',300)));await first;
    assert.equal(f.$('cmUpstreamRecoveryRows').innerHTML,'');assert.equal(f.messages.length,0);
  }
});

test('expired session redirects without claiming recovery success',async()=>{
  const f=setup(async()=>response({},401));await f.run();
  assert.equal(f.context.location.href,'/login');assert.equal(f.context.cm.loaded,true);assert.equal(f.timers.size,0);
});

test('backend capabilities control actions; escaped content and no guessed providers',async()=>{
  const data=snapshot('disabled');data.tasks[0].label='<script>';data.tasks.push({task:'funds',label:'资金流水',state:'cooling',retry_at:250});
  const f=setup(async()=>response(data));await f.refresh();
  const html=f.$('cmUpstreamRecoveryRows').innerHTML;
  assert.doesNotMatch(html,/<script>|data-recovery-task="usage"/);
  assert.match(html,/&lt;script>/);assert.match(html,/data-recovery-task="funds" disabled/);
  await f.run();assert.equal(f.requests.length,1);
});

test('only newer published success after receipt confirms a completed batch',async()=>{
  let data=snapshot();const f=setup(async url=>response(url==='/channels/upstream/recover'?{status:'queued',message:'排期',queued_at:200}:data));
  await f.run();data=snapshot('synced',100);await f.tick();assert.doesNotMatch(f.messages.at(-1).message,/已确认/);
  data=snapshot('synced',199);await f.tick();assert.doesNotMatch(f.messages.at(-1).message,/已确认/);
  data=snapshot('synced',201);await f.tick();assert.match(f.messages.at(-1).message,/已确认后台完成一批实际同步.*历史覆盖/);
  assert.equal(f.timers.size,0);assert.equal(f.requests.filter(r=>r.options.method==='POST').length,1);
});

test('tracking bounded to twelve reads; changing identity or new failure stops tracking',async()=>{
  for(const next of ['pending','failed','cooling','disabled','changed']){
    let data=snapshot();const f=setup(async url=>response(url==='/channels/upstream/recover'?{status:'queued',message:'排期',queued_at:200}:data));
    await f.run();
    if(next==='pending'){for(let i=0;i<11;i++)await f.tick();assert.equal(f.requests.length,13);}
    else{data=snapshot(next);if(next==='changed')data.account_epoch='epoch-2';await f.tick();}
    assert.equal(f.timers.size,0);assert.doesNotMatch(f.messages.at(-1).message,/已确认后台/);
    assert.equal(f.requests.filter(r=>r.options.method==='POST').length,1);
  }
});

test('closing or diagnostic reset cancels polling and in-flight status reads',async()=>{
  const f=setup(async url=>response(url==='/channels/upstream/recover'?{status:'queued',message:'排期',queued_at:200}:snapshot()));
  const resetStart=source.indexOf('function resetUpstreamDiagnostic('),resetEnd=source.indexOf('function renderUpstreamDiagnostic(',resetStart);
  vm.runInContext('let upstreamDiagnosticSeq=0,upstreamDiagnosticAbort=null;\n'+source.slice(resetStart,resetEnd),f.context);
  await f.run();assert.equal(f.timers.size,1);f.$('cmUpstreamRecovery').hidden=false;
  vm.runInContext('resetUpstreamDiagnostic(false)',f.context);
  assert.equal(f.$('cmUpstreamRecovery').hidden,false);assert.equal(f.timers.size,0);
  vm.runInContext('resetUpstreamDiagnostic()',f.context);assert.equal(f.$('cmUpstreamRecovery').hidden,true);
});
