import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function fixture() {
  const elements=new Map();
  const element=id=>{
    if(!elements.has(id))elements.set(id,{hidden:false,value:'',textContent:'',innerHTML:'',addEventListener(){}});
    return elements.get(id);
  };
  const timers=[];
  const context=vm.createContext({document:{getElementById:element},window:{},AbortController,URLSearchParams,
    setTimeout(fn,delay){timers.push({fn,delay});return timers.length;},clearTimeout(){},
    // Failed background refresh must keep the existing rendered report.
    fetch:async()=>{throw new Error('offline');}});
  const source=readFileSync(new URL('../../monitor/finance.js',import.meta.url),'utf8');
  const end=source.lastIndexOf('}());');
  assert.ok(end>0,'finance module closure not found');
  vm.runInContext(source.slice(0,end)+'\nglobalThis.fixture={state,scheduleStaleRefresh,load};\n'+source.slice(end),context);
  return {api:context.fixture,element,timers};
}

test('finance polling stops honestly after bounded attempts, preserving report',()=>{
  const {api,element,timers}=fixture();
  element('finPeriodRows').innerHTML='previous verified data';
  api.state.stale=true;
  for(let n=0;n<6;n++)api.scheduleStaleRefresh();
  assert.deepEqual(timers.map(t=>t.delay),[1000,2000,4000,8000,16000]);
  assert.match(element('finCacheUpdate').textContent,/更新尚未完成/);
  assert.equal(element('finPeriodRows').innerHTML,'previous verified data');
});

test('finance background failure preserves last report and bounded retry budget',async()=>{
  const {api,element,timers}=fixture();
  api.state.stale=true;api.state.refreshAttempts=4;
  element('finPeriodRows').innerHTML='previous verified data';
  await api.load(false,true);
  assert.equal(api.state.refreshAttempts,5);
  assert.equal(timers.length,1);
  assert.equal(element('finPeriodRows').innerHTML,'previous verified data');
  assert.match(element('finCacheUpdate').textContent,/更新失败.*offline/);
  await api.load(false,true);
  assert.equal(timers.length,1);
  assert.match(element('finCacheUpdate').textContent,/更新尚未完成/);
});

test('finance new user query receives a new retry budget',async()=>{
  const {api}=fixture();
  api.state.refreshAttempts=5;
  await api.load();
  assert.equal(api.state.refreshAttempts,0);
});
