import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8');
const start=source.indexOf('function upstreamUsageSyncFeedback(');
const end=source.indexOf('async function syncUpstreamFundsNow()',start);
assert.ok(start>=0&&end>start);
const functions=source.slice(start,end);

function setup(response){
  const elements=new Map(),messages=[],requests=[];
  const $=id=>{
    if(!elements.has(id))elements.set(id,{disabled:false});
    return elements.get(id);
  };
  const context=vm.createContext({
    $,shortDateTime:ts=>`time:${ts}`,cm:{upstreamDomain:{domain:'fixture.example'}},
    renderUpstreamStatus(){},loadReport:async()=>{},
    showUpstreamMessage:(message,warning)=>messages.push({message,warning}),
    fetch:async(url,options)=>{requests.push({url,options});return {ok:true,status:200,json:async()=>response};},
  });
  vm.runInContext(functions,context);
  return {context,$,messages,requests};
}

for(const fixture of [
  {name:'isolated no-op',data:{sync_skipped:true,sync_warning:true,sync_error:'本次未执行消费同步：认证或权限异常。',account:{usage_data_until:123}},text:/未执行.*time:123/,warning:true},
  {name:'normal interval',data:{sync_skipped:true,sync_warning:false,sync_error:'尚未到下次同步时间。',retry_at:456,account:{usage_data_until:123}},text:/下次计划 time:456.*time:123/,warning:false},
  {name:'older server isolated HTTP 200',data:{account:{usage_status:'reconnect',usage_data_until:123}},text:/已暂停.*认证或权限.*time:123/,warning:true},
  {name:'older server backoff HTTP 200',data:{account:{usage_status:'error',usage_last_error:'HTTP 429',usage_data_until:123}},text:/未完成.*HTTP 429/,warning:true},
  {name:'provider pending partial collection',data:{account:{usage_status:'pending',usage_data_until:123}},text:/尚未完成/,warning:true},
  {name:'empty response',data:{},text:/未取得消费同步结果/,warning:true},
  {name:'no watermark',data:{account:{usage_status:'ok'}},text:/尚未完成.*尚无/,warning:true},
  {name:'history-only success',data:{account:{usage_status:'ok',usage_data_until:123,usage_backfill_done:true}},text:/检查完成.*time:123/,warning:false},
  {name:'history incomplete',data:{account:{usage_status:'ok',usage_data_until:123,usage_backfill_done:false}},text:/历史补数尚未完成/,warning:false},
  {name:'history error',data:{account:{usage_status:'ok',usage_data_until:123,usage_backfill_last_error:'fixture error'}},text:/历史补数仍有异常/,warning:true},
  {name:'actual upstream failure',data:{sync_error:'HTTP 403',account:{usage_status:'reconnect',usage_data_until:123}},text:/未完成.*HTTP 403.*time:123/,warning:true},
]){
  test(`manual usage feedback: ${fixture.name}`,async()=>{
    const {context,$,messages,requests}=setup(fixture.data);
    await vm.runInContext('syncUpstreamUsageNow()',context);
    assert.equal(requests.length,1);
    assert.equal(requests[0].url,'/channels/upstream/usage-sync');
    assert.equal(requests[0].options.method,'POST');
    const result=messages.at(-1);
    assert.match(result.message,fixture.text);
    assert.equal(result.warning,fixture.warning);
    assert.doesNotMatch(result.message,/当天水位已更新|历史补数按保护频率继续/);
    for(const id of ['cmUpstreamUsageSync','cmUpstreamSave','cmUpstreamSync'])assert.equal($(id).disabled,false);
  });
}

test('permanent isolation sentinel is never formatted as a retry date',()=>{
  const {context}=setup();
  context.data={sync_skipped:true,retry_at:2**62,account:{usage_data_until:123}};
  const result=vm.runInContext('upstreamUsageSyncFeedback(data)',context);
  assert.doesNotMatch(result.message,/下次计划/);
  assert.equal(result.warning,true);
});
