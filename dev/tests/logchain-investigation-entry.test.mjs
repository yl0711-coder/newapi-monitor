import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

const source=readFileSync(new URL('../../monitor/logchain.js',import.meta.url),'utf8');
const page=readFileSync(new URL('../../monitor/page.html',import.meta.url),'utf8');

class Element {
  value=''; checked=false; hidden=false; disabled=false; innerHTML=''; textContent='';
  handlers=new Map(); attributes=new Map();
  addEventListener(type,handler){this.handlers.set(type,handler)}
  setAttribute(key,value){this.attributes.set(key,value)}
  focus(){this.focused=true}
  async emit(type,event={}){return this.handlers.get(type)?.({preventDefault(){},...event})}
}

function view(fetchImpl=async()=>({ok:true,status:202,json:async()=>({investigation_id:'opaque-task',poll_after_ms:800})})) {
  const elements=new Map([...page.matchAll(/id="(lcInvestigation\w+)"/g)].map(([,id])=>[id,new Element()]));
  elements.get('lcInvestigationPanel').hidden=true;
  const requests=[],timers=[];
  const forbidden=new Proxy({}, {get(){throw new Error('sensitive input must not touch browser storage or location')},set(){throw new Error('sensitive input must not touch browser storage or location')}});
  const context=vm.createContext({window:{localStorage:forbidden,sessionStorage:forbidden,location:forbidden},location:forbidden,
    localStorage:forbidden,sessionStorage:forbidden,URLSearchParams,
    document:{getElementById:id=>elements.get(id)||null,querySelector:()=>null},
    setTimeout:(fn,delay)=>{timers.push({fn,delay});return timers.length},
    fetch:async(url,options)=>{requests.push({url,options});return fetchImpl(url,options)}});
  const end=source.lastIndexOf('})();');
  vm.runInContext(source.slice(0,end)+'\nglobalThis.entryTest={lc,initStandaloneInvestigation,submitStandaloneInvestigation,closeStandaloneInvestigation,cloudWatchBlockBody,render,STANDALONE_INVESTIGATION,cancelCloudWatchInvestigation,loadCloudWatchEvidence,clearRowInvestigationStates};\n'+source.slice(end),context);
  const api=context.entryTest;
  api.initStandaloneInvestigation();
  return {...api,context,elements,requests,timers,el:id=>elements.get(id)};
}

function setWindow(v,from='2026-10-08T08:00',to='2026-10-08T10:00') {
  v.el('lcInvestigationFrom').value=from;
  v.el('lcInvestigationTo').value=to;
}
const tick=()=>new Promise(resolve=>setImmediate(resolve));

test('entry remains present before the filter and opens with no rows or enabled sources',async()=>{
  const v=view();
  assert.equal(v.lc.rows.length,0);
  assert.equal(v.lc.cloudWatchEnabled,false);
  const start=page.indexOf('id="tab-logchain"'),entry=page.indexOf('id="lcInvestigationOpen"'),filter=page.indexOf('<div class="lc-filterbar">',start);
  assert.ok(start<entry&&entry<filter);
  await v.el('lcInvestigationOpen').emit('click');
  assert.equal(v.el('lcInvestigationPanel').hidden,false);
  assert.equal(v.el('lcInvestigationOpen').attributes.get('aria-expanded'),'true');
  assert.equal(v.el('lcInvestigationFrom').focused,true);
  assert.equal(v.requests.length,0,'opening does not query any source');
});

test('CF-only query posts UTC time and explicit contract without changing URL or storing input',async()=>{
  const v=view();setWindow(v);
  v.el('lcInvestigationCloudFrontID').value='sensitive-cf-only-id';
  let prevented=false;
  await v.el('lcInvestigationForm').emit('submit',{preventDefault(){prevented=true}});
  assert.equal(prevented,true);
  assert.equal(v.requests.length,1);
  const {url,options}=v.requests[0],body=JSON.parse(options.body);
  assert.equal(url,'/logchain/investigations');
  assert.equal(options.method,'POST');
  assert.equal(body.schema_version,'observability.v1');
  assert.equal(body.from,'2026-10-08T00:00:00.000Z');
  assert.equal(body.to,'2026-10-08T02:00:00.000Z');
  assert.equal(body.timezone,'Asia/Shanghai');
  assert.equal(body.cloudfront_request_id,'sensitive-cf-only-id');
  assert.equal(body.newapi_request_id,undefined);
  assert.equal(body.user_id,undefined);
  assert.equal(v.el('lcInvestigationFields').disabled,true);
  await v.el('lcInvestigationForm').emit('submit');
  assert.equal(v.requests.length,1,'double submit must not create a second task');
});

test('Request ID, user-only and path-only input do not need business logs or carry page filters',async()=>{
  for(const [id,value,field,expected] of [
    ['lcInvestigationRequestID','newapi-id','newapi_request_id','newapi-id'],
    ['lcInvestigationUserID','42','user_id',42],
    ['lcInvestigationPath','/v1/responses','path','/v1/responses']
  ]) {
    const v=view();setWindow(v);
    v.lc.filters={user_id:'123',request_id:'unrelated',group:'old-group'};
    v.el(id).value=value;
    await v.submitStandaloneInvestigation();
    assert.equal(v.requests.length,1);
    const body=JSON.parse(v.requests[0].options.body);
    assert.equal(body[field],expected);
    assert.equal(body.group,undefined);
    assert.equal(body.at_unix,undefined);
    assert.equal(body.request_id,undefined);
  }
});

test('invalid, reversed and over-two-hour windows or unbounded identity never send a query',async()=>{
  for(const [from,to,id,value] of [
    ['', '2026-10-08T09:00','lcInvestigationRequestID','id'],
    ['2026-02-30T08:00','2026-03-02T09:00','lcInvestigationRequestID','id'],
    ['2026-10-08T09:00','2026-10-08T08:00','lcInvestigationRequestID','id'],
    ['2026-10-08T08:00','2026-10-08T08:00','lcInvestigationRequestID','id'],
    ['2026-10-08T08:00','2026-10-08T10:01','lcInvestigationRequestID','id'],
    ['2026-10-08T08:00','2026-10-08T09:00','lcInvestigationRequestID',''],
    ['2026-10-08T08:00','2026-10-08T09:00','lcInvestigationUserID','9007199254740993'],
    ['2026-10-08T08:00','2026-10-08T09:00','lcInvestigationUserID','0'],
    ['2026-10-08T08:00','2026-10-08T09:00','lcInvestigationPath','/v1?token=secret'],
    ['2026-10-08T08:00','2026-10-08T09:00','lcInvestigationPath','https://other.invalid/v1']
  ]) {
    const v=view();setWindow(v,from,to);v.el(id).value=value;
    await v.submitStandaloneInvestigation();
    assert.equal(v.requests.length,0,`${from} / ${to} / ${id} / ${value}`);
    assert.match(v.el('lcInvestigationResult').innerHTML,/lc-cw-err/);
  }
});

test('close and leaving the page clear every input and invalidate delayed poll results',async()=>{
  const v=view();setWindow(v);v.el('lcInvestigationRequestID').value='id';
  v.el('lcInvestigationSensitive').checked=true;
  await v.submitStandaloneInvestigation();
  assert.equal(v.timers.length,1);
  await v.el('lcInvestigationClose').emit('click');
  assert.equal(v.el('lcInvestigationPanel').hidden,true);
  assert.equal(v.el('lcInvestigationResult').innerHTML,'');
  assert.equal(v.el('lcInvestigationSensitive').checked,false);
  assert.equal(v.el('lcInvestigationFields').disabled,false);
  for(const [id,el] of v.elements)if(/From|To|RequestID|CloudFrontID|UserID|Path/.test(id))assert.equal(el.value,'');
  v.timers.shift().fn();await tick();
  assert.equal(v.requests.length,1,'closed panel must stop polling');
  v.el('lcInvestigationRequestID').value='new-id';
  v.lc.inited=true;
  v.context.window.logChainDeactivate();
  assert.equal(v.el('lcInvestigationRequestID').value,'');
});

test('closing and reopening during create cannot attach the previous task to the new input',async()=>{
  const pending=[];
  const v=view(()=>new Promise(resolve=>pending.push(resolve)));
  setWindow(v);v.el('lcInvestigationRequestID').value='first';
  const first=v.submitStandaloneInvestigation();
  v.closeStandaloneInvestigation();
  setWindow(v);v.el('lcInvestigationRequestID').value='second';
  const second=v.submitStandaloneInvestigation();
  pending[0]({ok:true,json:async()=>({investigation_id:'old-task'})});await first;
  assert.equal(v.lc.cwState.get(v.STANDALONE_INVESTIGATION).investigationId,'');
  pending[1]({ok:true,json:async()=>({investigation_id:'new-task'})});await second;
  assert.equal(v.lc.cwState.get(v.STANDALONE_INVESTIGATION).investigationId,'new-task');
  assert.equal(v.timers.length,1);
});

test('polling and cancel reuse existing opaque task endpoints and shared result UI',async()=>{
  const v=view(async url=>({ok:true,status:200,json:async()=>url.endsWith('/cancel')
    ?{status:'cancelled',source_status:null,summary:{classification:'inconclusive',conclusion:'查询取消，尚不能确定原因'}}
    :url.endsWith('/opaque-task')?{status:'running',source_status:null}
    :{investigation_id:'opaque-task'}}));
  setWindow(v);v.el('lcInvestigationCloudFrontID').value='raw-private-id';
  await v.submitStandaloneInvestigation();
  v.timers.shift().fn();await tick();
  assert.equal(v.requests[1].url,'/logchain/investigations/opaque-task');
  assert.match(v.el('lcInvestigationResult').innerHTML,/任务状态：查询中/);
  assert.match(v.el('lcInvestigationResult').innerHTML,/取消/);
  await v.cancelCloudWatchInvestigation(v.STANDALONE_INVESTIGATION);
  assert.equal(v.requests[2].url,'/logchain/investigations/opaque-task/cancel');
  assert.equal(v.requests[2].options.method,'POST');
  assert.equal(v.lc.cwState.get(v.STANDALONE_INVESTIGATION).loading,false);
  assert.match(v.el('lcInvestigationResult').innerHTML,/任务状态：已取消/);
  assert.match(v.el('lcInvestigationResult').innerHTML,/查询取消，尚不能确定原因/);
  assert.ok(v.requests.every(r=>!r.url.includes('raw-private-id')));
});

test('task status remains visible when queued, running or cancelled sources are null or absent',()=>{
  for(const [status,label,loading] of [['queued','排队中',true],['running','查询中',true],['cancelled','已取消',false]]) {
    for(const fields of [{source_status:null},{}]) {
      const v=view();
      v.lc.cwState.set(v.STANDALONE_INVESTIGATION,{loading,result:{status,...fields}});
      v.render();
      const html=v.el('lcInvestigationResult').innerHTML;
      assert.match(html,new RegExp(`任务状态：${label}`));
      assert.match(html,/尚未取得来源结果/);
      assert.match(html,/不能据此确定责任方/);
      assert.doesNotMatch(html,/问题位置<\/span><b>客户|客户必然|没有发生<\/b>/);
      if(status==='cancelled')assert.doesNotMatch(html,/data-lc-cw-cancel/);
    }
  }
});

test('no evidence stays inconclusive; shared unavailable/candidate is never labeled exact',()=>{
  const v=view();
  v.lc.cwState.set(v.STANDALONE_INVESTIGATION,{result:{source_status:[{source:'cloudfront',status:'empty'}],
    summary:{classification:'downstream_disconnect',evidence_level:'exact',conclusion:'old legacy summary'},
    observability:{schema_version:'observability.v1',from:'2026-10-08T00:00:00Z',to:'2026-10-08T02:00:00Z',events:[],
      summary:{evidence_level:'unavailable',candidate:true,conclusion:'未找到足够证据，不能判断请求是否发生或由谁负责'}}}});
  v.render();
  const html=v.el('lcInvestigationResult').innerHTML;
  assert.match(html,/未找到足够证据/);
  assert.match(html,/候选（未建立唯一关联，不能据此定责）/);
  assert.doesNotMatch(html,/精确关联|问题位置<\/span><b>客户/);
  assert.match(html,/2026\/10\/8 08:00:00/);
  assert.match(html,/2026\/10\/8 10:00:00/);
  assert.match(html,/北京时间，左闭右开/);
  assert.match(html,/脱敏证据事件 0 条（不是独立请求数）/);
  assert.match(html,/FRT 只代表首个 data: 事件延迟/);
});

test('new query errors do not leave an old successful conclusion below changed inputs',async()=>{
  const v=view(async()=>({ok:false,status:503,json:async()=>({error:'来源暂不可用'})}));
  v.lc.cwState.set(v.STANDALONE_INVESTIGATION,{result:{source_status:[],summary:{conclusion:'old conclusion'}}});
  setWindow(v);v.el('lcInvestigationPath').value='/v1/messages';
  await v.submitStandaloneInvestigation();
  assert.match(v.el('lcInvestigationResult').innerHTML,/来源暂不可用/);
  assert.doesNotMatch(v.el('lcInvestigationResult').innerHTML,/old conclusion/);
});

test('a running poll that returns after cancellation cannot revive the cancelled task',async()=>{
  let resolvePoll;
  const v=view(async url=>{
    if(url.endsWith('/opaque-task'))return new Promise(resolve=>{resolvePoll=resolve});
    return {ok:true,status:200,json:async()=>url.endsWith('/cancel')
      ?{status:'cancelled',source_status:[],summary:{classification:'inconclusive'}}
      :{investigation_id:'opaque-task'}};
  });
  setWindow(v);v.el('lcInvestigationRequestID').value='request';
  await v.submitStandaloneInvestigation();
  v.timers.shift().fn();await tick();
  await v.cancelCloudWatchInvestigation(v.STANDALONE_INVESTIGATION);
  resolvePoll({ok:true,status:200,json:async()=>({status:'running',source_status:[]})});await tick();
  const state=v.lc.cwState.get(v.STANDALONE_INVESTIGATION);
  assert.equal(state.loading,false);
  assert.equal(state.result.status,'cancelled');
  assert.equal(v.timers.length,0);
});

test('legacy row action retains old request-body compatibility and shared creation behavior',async()=>{
  const v=view();
  v.lc.rows=[{id:19,request_id:'row-id',created_at:1791417600,request_path:'/v1/responses',group:'old (group)'}];
  await v.loadCloudWatchEvidence('19');
  assert.equal(v.requests.length,1);
  const body=JSON.parse(v.requests[0].options.body);
  assert.equal(body.newapi_request_id,'row-id');
  assert.equal(body.at_unix,1791417600);
  assert.equal(body.path,'/v1/responses');
  assert.equal(body.schema_version,undefined,'old row UI is not silently migrated to the new projection');
  assert.equal(body.group,undefined);
  assert.equal(v.lc.cwState.get('19').investigationId,'opaque-task');
});

test('v1 source cards and timeline use canonical candidate facts, not old exact linkage',()=>{
  const v=view();
  const event={event_id:'cloudfront:ref',evidence_ref:'ref',source:'cloudfront',occurred_at:'2026-10-08T00:00:01Z',evidence_level:'unavailable',candidate:true,complete:false,frt_ms:null};
  v.lc.cwState.set(v.STANDALONE_INVESTIGATION,{result:{
    source_status:[{source:'cloudfront',status:'found',events:1,linkage:'exact'}],
    timeline:[{evidence_ref:'ref',source:'cloudfront',node:'CloudFront',fact:'其他请求候选',evidence_level:'exact'},
      {evidence_ref:'not-versioned',source:'cloudfront',fact:'旧时间线独有，不能混入新事件'}],
    observability:{schema_version:'observability.v1',from:'2026-10-08T00:00:00Z',to:'2026-10-08T02:00:00Z',
      summary:{evidence_level:'unavailable',candidate:true,conclusion:'仍需补强证据'},events:[event],
      source_quality:[{source_id:'cloudfront',query_complete:false,coverage_complete:null,parse_failures:null,quality_metadata_complete:false}]}
  }});
  v.render();
  const html=v.el('lcInvestigationResult').innerHTML;
  assert.doesNotMatch(html,/精确关联|旧时间线独有/);
  assert.match(html,/其他请求候选/);
  assert.match(html,/本次查询未完成/);
  assert.match(html,/持续采集覆盖未知/);
  assert.match(html,/解析失败数未知/);
  assert.match(html,/来源质量信息仍有缺项/);
  assert.match(html,/title="2026-10-08T00:00:01Z"/);
  assert.doesNotMatch(html,/FRT 0ms|TTFT 0/);
});

test('v1 observed FRT is labeled separately and a finished query does not imply continuous coverage',()=>{
  const v=view();
  v.lc.cwState.set(v.STANDALONE_INVESTIGATION,{result:{
    source_status:[{source:'newapi_database',status:'found',events:1,linkage:'exact'}],
    observability:{schema_version:'observability.v1',from:'2026-10-08T00:00:00Z',to:'2026-10-08T02:00:00Z',
      summary:{evidence_level:'exact',candidate:false,conclusion:'已读到消费记录，不代表协议成功'},
      events:[{event_id:'db:ref',source:'newapi_database',occurred_at:'2026-10-08T00:01:00Z',evidence_level:'exact',candidate:false,complete:true,frt_ms:3500}],
      source_quality:[{source_id:'newapi_database',query_complete:true,coverage_complete:null,parse_failures:0,quality_metadata_complete:false}]}
  }});
  v.render();
  const html=v.el('lcInvestigationResult').innerHTML;
  assert.match(html,/FRT 3,500ms（非 TTFT）/);
  assert.match(html,/本次查询已完成/);
  assert.match(html,/持续采集覆盖未知/);
  assert.doesNotMatch(html,/持续采集覆盖完整|TTFT 3/);
});

test('changing log filters discards per-row investigations without stopping independent investigation',()=>{
  const v=view();
  const independent={seq:9,loading:true,investigationId:'independent-task'};
  v.lc.cwState.set(v.STANDALONE_INVESTIGATION,independent);
  v.lc.cwState.set('10',{seq:10,loading:true,investigationId:'old-row-task'});
  v.lc.cwState.set('11',{seq:11,loading:false,result:{}});
  v.clearRowInvestigationStates();
  assert.equal(v.lc.cwState.size,1);
  assert.equal(v.lc.cwState.get(v.STANDALONE_INVESTIGATION),independent);
  assert.equal(v.lc.cwState.has('10'),false);
  assert.equal(v.lc.cwState.has('11'),false);
});

test('independent investigation preserves failed attempt and consumption without declaring healthy or final failure',()=>{
  const v=view();
  v.lc.cwState.set(v.STANDALONE_INVESTIGATION,{result:{status:'partial',source_status:[],
    summary:{classification:'business_completed',evidence_level:'exact',conclusion:'旧摘要只看消费记录'},
    timeline:[{source:'newapi_database',evidence_ref:'failed',node:'NewAPI',fact:'先发生了一条错误记录'},
      {source:'newapi_database',evidence_ref:'consumed',node:'NewAPI',fact:'后续写入正常消费记录'}],
    observability:{schema_version:'observability.v1',from:'2026-10-08T00:00:00Z',to:'2026-10-08T02:00:00Z',
      summary:{fault_class:'unknown',evidence_level:'exact',candidate:false,conclusion:'同时记录到错误和正常消费；责任方待判，重试后的最终结果尚未确认'},
      events:[{source:'newapi_database',event_id:'db:failed',evidence_ref:'failed',occurred_at:'2026-10-08T00:01:00Z',fault_class:'unknown',evidence_level:'exact',candidate:false},
        {source:'newapi_database',event_id:'db:consumed',evidence_ref:'consumed',occurred_at:'2026-10-08T00:01:01Z',fault_class:null,evidence_level:'exact',candidate:false}]}
  }});
  v.render();
  const html=v.el('lcInvestigationResult').innerHTML;
  assert.match(html,/待判（已记录错误或异常，责任尚不明确）/);
  assert.match(html,/先发生了一条错误记录/);
  assert.match(html,/后续写入正常消费记录/);
  assert.match(html,/重试后的最终结果尚未确认/);
  assert.doesNotMatch(html,/未发现平台或上游故障|最终失败|最终成功|旧摘要只看消费记录/);
});

test('a consumption-only observation is not labeled as proof of no platform or upstream fault',()=>{
  const v=view();
  v.lc.cwState.set(v.STANDALONE_INVESTIGATION,{result:{status:'partial',source_status:[],
    summary:{classification:'business_completed',evidence_level:'exact',conclusion:'日志记录到正常消费'},
    observability:{schema_version:'observability.v1',events:[],summary:{fault_class:null,evidence_level:'exact',candidate:false,conclusion:'日志记录到正常消费，不代表完整交付'}}
  }});
  v.render();
  const html=v.el('lcInvestigationResult').innerHTML;
  assert.match(html,/已记录消费，不能据此确认无故障/);
  assert.doesNotMatch(html,/未发现平台或上游故障|最终成功/);
});

test('shared unknown fault taxonomy does not erase a proven fault location',()=>{
  for(const [classification,location] of [['upstream_error','上游渠道'],['platform_error','平台配置 / 路由 / 运行时'],['database_error','平台数据库']]){
    const v=view();
    v.lc.cwState.set(v.STANDALONE_INVESTIGATION,{result:{status:'partial',source_status:[],
      summary:{classification,evidence_level:'exact',conclusion:'已有直接责任证据'},
      observability:{schema_version:'observability.v1',events:[],summary:{fault_class:'unknown',evidence_level:'exact',candidate:false,conclusion:'已有直接责任证据'}}
    }});
    v.render();
    const html=v.el('lcInvestigationResult').innerHTML;
    assert.match(html,new RegExp(`问题位置</span><b>${location}`));
    assert.doesNotMatch(html,/责任尚不明确/);
  }
});
