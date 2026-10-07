import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function fixture(fetchImpl=async()=>({ok:true,status:200,json:async()=>({rows:[],changes:[],configurations:{}})})){
  const timers=new Map();let timerID=0;
  const context=vm.createContext({window:{},location:{href:''},AbortController,URLSearchParams,fetch:fetchImpl,
    setTimeout(fn){timers.set(++timerID,fn);return timerID},clearTimeout(id){timers.delete(id)}});
  vm.runInContext(readFileSync(new URL('../../monitor/channel_pricing_observations.js',import.meta.url),'utf8'),context);
  return {api:context.window.channelPricingObservations,context,timers};
}
const domain={domain:'pricing.example',key:'domain:pricing.example',upstream:{configured:true}};
const payload={collection_active:true,as_of_hour:1789441200,configurations:{group:[{channel_id:9,channel_name:'nine',local_group:'local',effective_multiplier:'1'}]},
  rows:[{source_group:'group',model_name:'model',hour_ts:1789441200,requests:20,rates:['1.2'],discounts:[],comparison:'different',evidence_status:'recent'}],changes:[]};

test('read-only control is hidden for non-root and unconfigured suppliers',()=>{
  const {api}=fixture();
  assert.equal(api.render(domain,false),'');
  assert.equal(api.render({...domain,upstream:{configured:false}},true),'');
  assert.match(api.render(domain,true),/日志倍率核对/);
});

test('only the local GET reader is called and core money is never rendered or changed',async()=>{
  const requests=[];
  const {api,timers}=fixture(async(url,options)=>{requests.push({url,options});return {ok:true,status:200,json:async()=>payload}});
  const before=JSON.stringify(payload);
  await api.toggle(domain,()=>{});
  assert.equal(requests.length,1);
  assert.match(requests[0].url,/^\/channels\/upstream\/pricing-observations\?domain=pricing.example$/);
  assert.equal(requests[0].options.method,undefined);
  assert.equal(requests[0].options.cache,'no-store');
  assert.equal(timers.size,0);
  const html=api.render(domain,true);
  assert.match(html,/1\.2×/);assert.match(html,/当前配置不同/);assert.match(html,/充值支付／到账比例不从使用日志推算/);
  assert.doesNotMatch(html,/data-cm-proposal-action|审批并排期|毛利润/);
  assert.equal(JSON.stringify(payload),before);
});

test('historical differences do not look like current change alerts and unsafe strings are escaped',async()=>{
  const data={...payload,rows:[{...payload.rows[0],evidence_status:'historical',model_name:'<img onerror="bad">'}],configurations:{group:[{...payload.configurations.group[0],channel_name:'<script>bad</script>'}]}};
  const {api}=fixture(async()=>({ok:true,status:200,json:async()=>data}));
  await api.load(domain,()=>{});
  const html=api.render(domain,true);
  assert.match(html,/历史观测，非当前状态/);assert.doesNotMatch(html,/class="cm-pricing-difference"|<img|<script>/);
  assert.match(html,/&lt;img/);assert.match(html,/&lt;script&gt;/);
});

test('mixed rates, unknown evidence and missing events cannot imply pricing stability',async()=>{
  const {api}=fixture(async()=>({ok:true,status:200,json:async()=>({...payload,rows:[{...payload.rows[0],comparison:'mixed_rates',rates:['1','1.2']},{...payload.rows[0],comparison:'unverified',evidence_status:'unverified'}]})}));
  await api.load(domain,()=>{});
  const html=api.render(domain,true);
  assert.match(html,/同小时多种倍率/);assert.match(html,/证据未核验，不能判断/);assert.match(html,/不代表历史倍率一直相同/);
});

test('truncated evidence is not displayed as a valid partial comparison',async()=>{
  const {api}=fixture(async()=>({ok:true,status:200,json:async()=>({...payload,truncated:true})}));
  await api.load(domain,()=>{});
  const html=api.render(domain,true);
  assert.match(html,/不发布局部倍率判断/);assert.doesNotMatch(html,/1\.2×/);
});

test('unconfigured group names cannot read Object prototype properties',async()=>{
  const {api}=fixture(async()=>({ok:true,status:200,json:async()=>({...payload,configurations:{},rows:[{...payload.rows[0],source_group:'constructor',comparison:'not_configured'}]})}));
  await api.load(domain,()=>{});
  assert.match(api.render(domain,true),/constructor/);
  assert.match(api.render(domain,true),/未找到同名分组配置/);
});

test('reset aborts reads and late results cannot replace another supplier context',async()=>{
  let finish,signal;
  const {api}=fixture((url,options)=>{signal=options.signal;return new Promise(resolve=>finish=resolve)});
  const pending=api.load(domain,()=>{});
  api.reset();assert.equal(signal.aborted,true);assert.equal(api.hasOpen(),false);
  finish({ok:true,status:200,json:async()=>payload});await pending;
  assert.doesNotMatch(api.render(domain,true),/1\.2×/);
});

test('network timeout has a visible retryable error and does not touch main money',async()=>{
  const {api,timers}=fixture((url,options)=>new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new Error('aborted')))));
  const pending=api.load(domain,()=>{});
  [...timers.values()][0]();await pending;
  assert.match(api.render(domain,true),/读取超时/);assert.equal(timers.size,0);
});

test('a non-JSON gateway failure exposes only a safe HTTP error',async()=>{
  const {api}=fixture(async()=>({ok:false,status:502,json:async()=>{throw new SyntaxError('private gateway contents')}}));
  await api.load(domain,()=>{});
  const html=api.render(domain,true);
  assert.match(html,/HTTP 502/);assert.doesNotMatch(html,/private gateway contents|Unexpected end/);
});

test('close and concurrent refresh ignore obsolete responses',async()=>{
  const pending=[];
  const {api}=fixture(()=>new Promise(resolve=>pending.push(resolve)));
  const first=api.load(domain,()=>{}),second=api.load(domain,()=>{});
  pending[1]({ok:true,status:200,json:async()=>({...payload,rows:[]})});await second;
  pending[0]({ok:true,status:200,json:async()=>payload});await first;
  assert.doesNotMatch(api.render(domain,true),/1\.2×/);
  await api.toggle(domain,()=>{});assert.equal(api.hasOpen(),false);
});

test('isolated Docker records render as historical evidence without sync requests',{
  skip:process.env.MONITOR_PRICING_PREVIEW_ACCEPTANCE_URL!=='http://127.0.0.1:8112',
},async()=>{
  const base=process.env.MONITOR_PRICING_PREVIEW_ACCEPTANCE_URL;
  for(const supplier of ['4sapi.com','codeyu.shop']){
    const calls=[];
    const {api}=fixture(async(url,options)=>{calls.push(url);return fetch(base+url,options)});
    const selected={domain:supplier,key:'domain:'+supplier,upstream:{configured:true}};
    await api.load(selected,()=>{});
    const html=api.render(selected,true);
    assert.equal(calls.length,1);
    assert.match(calls[0],/^\/channels\/upstream\/pricing-observations/);
    assert.match(html,/当前计价采集未启用/);
    assert.match(html,/历史观测，非当前状态/);
    assert.doesNotMatch(html,/近3小时已核验|data-cm-proposal-action|cm-cost-ledger-error/);
  }
});
