import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
const source=fs.readFileSync(new URL('../../monitor/logchain.js',import.meta.url),'utf8');
function view(){
  const elements=new Map(['lcDetailTh','lcDetailThSub'].map(x=>[x,{textContent:''}]));
  const ctx=vm.createContext({window:{},document:{getElementById:x=>elements.get(x)||null,querySelector:()=>null},URLSearchParams,AbortController,setTimeout,clearTimeout});
  const end=source.lastIndexOf('})();');
  vm.runInContext(source.slice(0,end)+'\nglobalThis.api={lc,syncDetailHeader,contentCell,faultCell,detailHTML,cloudWatchBlockBody,applyInvestigationDiagnoses};\n'+source.slice(end),ctx);
  return {...ctx.api,elements};
}
const diagnosis={rule_id:'LC-140',version:'2026-11-08.1',document_lines:[221],reason:'请求的 thinking 参数与模型要求不兼容，需要 adaptive',responsibility:'客户侧',responsible_parties:['downstream'],cause_known:true,error_layer:'上游响应',action:'客户端改 adaptive',action_owners:['downstream'],basis:'原始行 221：requires adaptive thinking',missing:[{field:'error_param',reason:'原日志未记录',record_at:'客户端校验层记录字段路径'}]};
function row(){return {id:10,request_id:'request-a',created_at:1794000000,type:5,content:'status_code=502, Relay upstream error: future-model requires adaptive thinking',fault:'downstream',fault_reason:diagnosis.reason,diagnosis,anomaly_tags:[],diagnostic_facts:{fields:{current_channel_host:{value:'example.test',source:'channel_snaps',observed_at:1794000500,scope:'current_config'}}}}}
test('all customer troubleshooting scopes keep error cause header',()=>{
 const v=view();for(const scope of ['error','stream','client_gone','billing','anomaly_all','err_anom','all']){v.lc.scope=scope;v.syncDetailHeader();assert.equal(v.elements.get('lcDetailTh').textContent,'错误原因')}
});
test('specific Chinese reason and documented responsibility replace raw primary columns',()=>{
 const v=view(),r=row();assert.match(v.contentCell(r),/thinking 参数/);assert.doesNotMatch(v.contentCell(r),/Relay upstream/);assert.match(v.faultCell(r),/客户侧/);assert.doesNotMatch(v.faultCell(r),/推断/);
 const html=v.detailHTML(r,'10');for(const term of ['处理建议','客户端改 adaptive','判定依据','待补','当前配置','请求当时','status_code=502'])assert.ok(html.includes(term),term);
 r.diagnosis={...diagnosis,responsibility:'待查'};assert.match(v.faultCell(r),/原因已明确/);assert.match(v.faultCell(r),/责任方待查/);
});
test('evidence refresh updates only the same source log and preserves filters and expansion',()=>{
 const v=view(),r=row();r.diagnosis={...diagnosis,cause_known:false,reason:'请求被拒绝'};v.lc.rows=[r,{...row(),id:11}];v.lc.expanded.add('10');v.lc.scope='error';
 v.applyInvestigationDiagnoses({requests:[row(),{...row(),id:11,request_id:'unrelated',diagnosis:{...diagnosis,reason:'wrong'}}]});
 assert.equal(r.diagnosis.reason,diagnosis.reason);assert.notEqual(v.lc.rows[1].diagnosis.reason,'wrong');assert.ok(v.lc.expanded.has('10'));assert.equal(v.lc.scope,'error');
 v.applyInvestigationDiagnoses({status:'failed'});assert.equal(r.diagnosis.reason,diagnosis.reason);
 v.applyInvestigationDiagnoses({requests:[{...row(),diagnosis:{...diagnosis,cause_known:false,reason:'unknown'}}]});assert.equal(r.diagnosis.reason,diagnosis.reason);
});
test('full chain retains parameter cause instead of generic downstream disconnect',()=>{
 const v=view();v.lc.rows=[row()];v.lc.cwState.set('10',{result:{requests:[row()],source_status:[],summary:{classification:'downstream_disconnect',conclusion:'下游断连'},observability:{schema_version:'observability.v1',summary:{fault_class:'client_gone',evidence_level:'exact',conclusion:'下游断连'},events:[]}}});
 const html=v.cloudWatchBlockBody('10');assert.match(html,/thinking 参数/);assert.match(html,/客户侧/);assert.doesNotMatch(html,/<b>下游断连<\/b>/);
});

test('versioned investigation updates by opaque row and request identity without raw SQL rows',()=>{
 const v=view(),r=row();r.diagnostic_ref='bound-row-request';r.diagnosis={...diagnosis,cause_known:false,reason:'请求失败，原因未透出'};v.lc.rows=[r,{...row(),id:11,diagnostic_ref:'different-attempt'}];
 v.applyInvestigationDiagnoses({schema_version:'observability.v1',requests:null,request_diagnoses:[{...row(),ref:'bound-row-request'}]});
 assert.equal(r.diagnosis.reason,diagnosis.reason);
 v.applyInvestigationDiagnoses({request_diagnoses:[{...row(),ref:'unrelated',diagnosis:{...diagnosis,reason:'wrong'}}]});
 assert.equal(r.diagnosis.reason,diagnosis.reason);
});

test('actual channel path is identical in list, detail and full-chain summary',()=>{
 for(const path of ['/v1/alpha/search','/v1/responses','/custom/chat/completions']){
  const v=view(),r=row();r.diagnosis={...diagnosis,rule_id:'LC-119',reason:'通道不支持 '+path,responsibility:'平台侧/上游'};r.fault_reason=r.diagnosis.reason;
  v.lc.rows=[r];v.lc.cwState.set('10',{result:{requests:[r],source_status:[],summary:{classification:'request_diagnosis',diagnosis:r.diagnosis,conclusion:r.diagnosis.reason}}});
  for(const html of [v.contentCell(r),v.detailHTML(r,'10'),v.cloudWatchBlockBody('10')]){
   assert.ok(html.includes('通道不支持 '+path));if(path!='/v1/alpha/search')assert.ok(!html.includes('/v1/alpha/search'));
  }
 }
});

test('request identities retain attribution, conflict, source and escaping in details',()=>{
 const v=view(),r=row();r.diagnostic_facts.request_ids=[
  {value:'relay-1',owner:'upstream_relay',status:'bound',source:'logs:content',observed_at:1794000000,attempt_id:'try-b'},
  {value:'<script>bad</script>',owner:'unknown',status:'unattributed',source:'logs:other',reason:'需补归属'},
  {value:'provider-2',owner:'model_provider',status:'retry_conflict',source:'cloudwatch:newapi',reason:'其他请求尝试'}];
 r.diagnostic_facts.request_ids_truncated=true;
 for(const diagnosis of [r.diagnosis,null]){
  const html=v.detailHTML({...r,diagnosis},'10');
  for(const text of ['relay-1','上游中转','模型商','归属待确认','请求尝试冲突','try-b','logs:content','不能证明唯一性'])assert.ok(html.includes(text),text);
  assert.ok(html.includes('&lt;script&gt;bad&lt;/script&gt;'));assert.ok(!html.includes('<script>bad'));
 }
});

test('specific code correction propagates without resetting list state',()=>{
 for(const reason of ['客户请求中的数组长度超限','上游 HTTP/2 流错误']){
  const v=view(),r=row();r.diagnostic_ref='attempt-a';r.diagnosis={...diagnosis,cause_known:false,reason:'请求失败'};v.lc.rows=[r];v.lc.expanded.add('10');v.lc.scope='err_anom';
  const updated={...row(),ref:'attempt-a',fault_reason:reason,diagnosis:{...diagnosis,reason}};
  v.applyInvestigationDiagnoses({request_diagnoses:[updated]});v.lc.cwState.set('10',{result:{request_diagnoses:[updated],source_status:[],summary:{classification:'request_diagnosis'}}});
  for(const html of [v.contentCell(r),v.detailHTML(r,'10'),v.cloudWatchBlockBody('10')])assert.ok(html.includes(reason));
  assert.ok(v.lc.expanded.has('10'));assert.equal(v.lc.scope,'err_anom');
 }
});

test('supplement outcomes remain distinct per source in detail and full chain',()=>{
 const v=view(),r=row();
 r.diagnosis={...diagnosis,missing:[{field:'connect_ms',reason:'耗时待补',status:'source_query_timeout',sources:['nginx','newapi'],source_results:{nginx:'not_recorded_in_queried_evidence',newapi:'source_query_timeout'},record_at:'应用转发层记录 connect_ms',available_from:'源端部署后新请求可取 <script>'}]};
 v.lc.rows=[r];v.lc.cwState.set('10',{result:{requests:[r],source_status:[],summary:{classification:'request_diagnosis',diagnosis:r.diagnosis}}});
 for(const html of [v.detailHTML(r,'10'),v.cloudWatchBlockBody('10')]){
  for(const term of ['nginx：已查询证据未记录','newapi：补查超时','connect_ms','源端部署后新请求可取 &lt;script&gt;'])assert.ok(html.includes(term),term);
  assert.ok(!html.includes('<script>'));
 }
});
