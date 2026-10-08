import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8');
const start=source.indexOf('function renderUpstreamRecoveryOptions(');
const end=source.indexOf('function upstreamBalanceSummary(',start);
assert.ok(start>=0&&end>start);
function setup(fetcher){
  const elements=new Map(),messages=[],requests=[],rendered=[];
  const $=id=>{
    if(!elements.has(id))elements.set(id,{disabled:false,hidden:false,value:'usage',options:['balance','usage','usage_history','funds','pricing','error_logs'].map(value=>({value,disabled:false}))});
    return elements.get(id);
  };
  const context=vm.createContext({$,AbortController,setTimeout,clearTimeout,location:{href:''},
    cm:{loaded:true,upstreamDomain:{domain:'fixture.example'},upstreamConfig:{provider:'newapi'}},dateTime:ts=>`time:${ts}`,
    renderUpstreamStatus:account=>rendered.push(account),
    showUpstreamMessage:(message,warning)=>messages.push({message,warning}),
    fetch:async(url,options)=>{requests.push({url,options});return fetcher(url,options);},
  });
  vm.runInContext('let upstreamRecoverySeq=0,upstreamRecoveryAbort=null;\n'+source.slice(start,end),context);
  return {context,$,messages,requests,rendered,run:()=>vm.runInContext('recoverUpstreamTask()',context)};
}
function response(data,status=200){return {status,ok:status===200,json:async()=>data};}
for(const status of ['queued','waiting','blocked','not_needed','unknown']){
  test(`recovery feedback: ${status}`,async()=>{
    const fixture=setup(async url=>response(url==='/channels/upstream/recover'?{status,message:'fixture message',retry_at:status==='waiting'?123:2**62}:{account:{configured:true,usage_status:'pending'}}));
    fixture.$('cmUpstreamBaseURL').value='unsaved input';
    await fixture.run();
    const {messages,requests,$,context}=fixture;
    assert.equal(requests.length,status==='queued'?2:1);assert.equal(requests[0].url,'/channels/upstream/recover');
    assert.deepEqual(JSON.parse(requests[0].options.body),{domain:'fixture.example',task:'usage'});
    assert.equal(messages.at(-1).warning,status==='blocked'||status==='unknown');
    assert.doesNotMatch(messages.at(-1).message,/账单已更新|余额已更新|数据已补齐|Invalid Date/);
    if(status==='waiting')assert.match(messages.at(-1).message,/time:123/);
    else assert.doesNotMatch(messages.at(-1).message,/下次可检测/);
    assert.equal(context.cm.loaded,status!=='queued');
    assert.equal(fixture.rendered.length,status==='queued'?1:0);
    if(status==='queued'){
      assert.equal(requests[1].url,'/channels/upstream?domain=fixture.example');
      assert.equal(requests[1].options.cache,'no-store');
      assert.equal(context.cm.upstreamConfig.account.usage_status,'pending');
      assert.equal(context.cm.upstreamConfig.provider,'newapi');
    }
    assert.equal(fixture.$('cmUpstreamBaseURL').value,'unsaved input');
    assert.equal($('cmUpstreamRecover').disabled,false);
  });
}
test('recovery suppresses duplicate clicks and ignores stale domain response',async()=>{
  let resolve;
  const fixture=setup(()=>new Promise(r=>{resolve=r}));
  const first=fixture.run();await fixture.run();
  assert.equal(fixture.requests.length,1);
  fixture.context.cm.upstreamDomain={domain:'other.example'};
  resolve(response({status:'queued',message:'stale result'}));await first;
  assert.ok(!fixture.messages.some(m=>m.message==='stale result'));
  assert.equal(fixture.context.cm.loaded,true);
});
test('local status refresh failure must not claim the recovery itself failed',async()=>{
  for(const reply of [response({},500),response({account:null}),response({account:[]})]){
    const fixture=setup(async url=>url==='/channels/upstream/recover'?response({status:'queued',message:'已恢复排期'}):reply);
    await fixture.run();
    assert.equal(fixture.context.cm.loaded,false);
    assert.equal(fixture.rendered.length,0);
    assert.equal(fixture.requests.length,2);
    assert.match(fixture.messages.at(-1).message,/已恢复排期.*状态刷新失败/);
    assert.equal(fixture.messages.at(-1).warning,true);
  }
});
test('late local status refresh cannot update another or closed account dialog',async()=>{
  for(const close of [false,true]){
    let resolve,started;
    const refreshing=new Promise(r=>{started=r});
    const fixture=setup(async url=>{
      if(url==='/channels/upstream/recover')return response({status:'queued',message:'已恢复排期'});
      started();return new Promise(r=>{resolve=r});
    });
    const first=fixture.run();await refreshing;await fixture.run();
    assert.equal(fixture.requests.length,2);
    fixture.context.cm.upstreamDomain=close?null:{domain:'other.example'};
    resolve(response({account:{configured:true,usage_status:'pending'}}));await first;
    assert.equal(fixture.rendered.length,0);
    assert.equal(fixture.context.cm.upstreamConfig.account,undefined);
  }
});
test('expired session redirects without claiming recovery success',async()=>{
  const fixture=setup(async()=>response({},401));await fixture.run();
  assert.equal(fixture.context.location.href,'/login');
  assert.equal(fixture.context.cm.loaded,true);
  assert.equal(fixture.$('cmUpstreamRecover').disabled,false);
});
test('unsupported or disabled task options are not offered',()=>{
  const fixture=setup();
  fixture.context.account={configured:true,enabled:true,usage_sync_enabled:true,provider:'tokenforce'};
  vm.runInContext('renderUpstreamRecoveryOptions(account)',fixture.context);
  for(const option of fixture.$('cmUpstreamRecoveryTask').options)assert.equal(option.disabled,['funds','pricing','error_logs'].includes(option.value));
  fixture.context.account.enabled=false;vm.runInContext('renderUpstreamRecoveryOptions(account)',fixture.context);
  assert.equal(fixture.$('cmUpstreamRecovery').hidden,true);
});

test('diagnostic refresh preserves recovery action but closing clears it',()=>{
  const fixture=setup();
  const resetStart=source.indexOf('function resetUpstreamDiagnostic(');
  const resetEnd=source.indexOf('function renderUpstreamDiagnostic(',resetStart);
  vm.runInContext('let upstreamDiagnosticSeq=0,upstreamDiagnosticAbort=null;\n'+source.slice(resetStart,resetEnd),fixture.context);
  fixture.$('cmUpstreamRecovery').hidden=false;
  vm.runInContext('resetUpstreamDiagnostic(false)',fixture.context);
  assert.equal(fixture.$('cmUpstreamRecovery').hidden,false);
  vm.runInContext('resetUpstreamDiagnostic()',fixture.context);
  assert.equal(fixture.$('cmUpstreamRecovery').hidden,true);
});
