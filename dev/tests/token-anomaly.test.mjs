import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {at,access,health,detectionHealth,notifyHealth,item,report,detail,receipt} from '../token-anomaly-fixture.mjs';
const source=readFileSync(new URL('../../monitor/token_anomaly.js',import.meta.url),'utf8');
function harness(){
 const elements=new Map(),requests=[],location={href:''},navigation=[];
 const element=id=>{
  if(!elements.has(id)){
   const listeners={};elements.set(id,{id,value:'',checked:false,disabled:false,hidden:false,open:false,innerHTML:'',textContent:'',attrs:{},listeners,
    setAttribute(k,v){this.attrs[k]=v},addEventListener(k,f){listeners[k]=f},focus(){},showModal(){this.open=true},close(){this.open=false}});
  }return elements.get(id);
 };
 const context=vm.createContext({window:{logChainOpen:c=>navigation.push(c)},document:{getElementById:element,querySelectorAll:()=>[]},location,URLSearchParams,AbortController,crypto:{randomUUID:()=>String(requests.length)+'-id'},URL,setTimeout,fetch:(url,options)=>new Promise((resolve,reject)=>requests.push({url,options,resolve,reject}))});
 const end=source.lastIndexOf('})();');
 vm.runInContext(source.slice(0,end)+'\nglobalThis.testing={state,load,readFilters,openDetail,renderDetail,renderReport,init,createExport,downloadExport,invalidateExport,syncExport,parseTime,fraction,whole};\n'+source.slice(end),context);
 const h={...context.testing,window:context.window,element,requests,location,navigation};
 h.init();h.state.active=true;h.state.query={from:at-86400,to:at+3600,status:'candidate'};element('taDimension').value='channel_id';
 return h;
}
const response=(v,status=200)=>({ok:status===200,status,json:async()=>v});
function settle(h,offset=0,r=report(),status=200){
 for(const [i,v]of [r,health,detectionHealth,notifyHealth,access].entries())h.requests[offset+i].resolve(response(v,i===0?status:200));
}
test('Token money keeps integer precision, fractions and null distinct from zero',()=>{
 const h=harness();assert.equal(h.whole('9007199254740993'),'9,007,199,254,740,993');assert.equal(h.fraction('1/250000'),'0.000004');assert.equal(h.fraction('1/3'),'≈0.333333');assert.equal(h.fraction('-1/3'),'≈-0.333333');assert.equal(h.whole(0),'0');assert.match(h.whole(null),/不可计算/);assert.match(h.fraction('1/0'),/不可计算/);
});

test('list and detail disclose effective gaps, rule, source and acquisition state',async()=>{
 const h=harness(),r=report(),d=detail();
 const display={missing_fields:[{name:'route_snapshot',reason:'source_missing'}],acquisition:[{name:'route_snapshot',rule:'request_provenance',source:'newapi.logs.other.route_snapshot',status:'source_not_recorded',action:'部署后从新请求获取 <unsafe>'}]};
 Object.assign(r.items[0].display,display);
 const p=h.load();settle(h,0,r);await p;
 d.display=display;h.renderDetail(d);
 for(const html of [h.element('taList').innerHTML,h.element('taDetailBody').innerHTML]){
   assert.match(html,/取数与补取状态/);assert.match(html,/请求来源核验/);assert.match(html,/newapi.logs.other.route_snapshot/);assert.match(html,/源日志未记录/);
   assert.match(html,/&lt;unsafe&gt;/);assert.doesNotMatch(html,/<unsafe>/);
 }
});

test('bounded incomplete totals remain unavailable while request details stay usable',async()=>{
 const h=harness(),p=h.load(),r=report();
 Object.assign(r.summary,{truncated:true,observed_results:null,candidate_requests:null,insufficient_requests:null,amounts:null,unique_candidate_dimensions:null,unavailable_by_method_and_reason:null});
 Object.assign(r.analysis,{status_counts:null,calculable_candidates:null,breakdown:null});
 settle(h,0,r);await p;
 assert.equal(h.element('taError').hidden,true);assert.match(h.element('taSummary').innerHTML,/未就绪/);assert.doesNotMatch(h.element('taSummary').innerHTML,/<b>0<\/b>/);
 assert.match(h.element('taList').innerHTML,/fixture-request-10/);assert.match(h.element('taUnavailable').innerHTML,/未就绪/);
});

test('versioned operands preserve raw missing states and disclose reference exclusions',()=>{
 const h=harness(),d=detail(),u=d.evidence.input.usage;
 u.source_contract={version:'newapi-rc48-text-v1',source_epoch:'fixture',from:at-60,evidence:'fixture version witness'};
 u.contract_operands={cache_creation_tokens_1h:{state:'derived',value:'0',reason:'service/text_quota.go'}};
 u.fields.cache_creation_tokens_1h={state:'missing',reason:'source_missing'};
 d.evidence.result.reference.missing_comparable_fields=['entry_route_status_version'];
 d.evidence.result.reference.exclusion_reasons={'incompatible:request_conversion':3};
 d.evidence.result.reference.search=[{tolerance_bytes:16,read:4,eligible:1,channels:1}];
 h.renderDetail(d);const html=h.element('taDetailBody').innerHTML;
 for(const s of ['newapi-rc48-text-v1','原始字段保持其缺失状态','service/text_quota.go','entry_route_status_version','request_conversion','16'])assert.ok(html.includes(s),s);
 assert.match(html,/缺失/);
});
test('Beijing input rejects invalid dates and keeps left-closed right-open UTC values',()=>{
 const h=harness();assert.equal(h.parseTime('2026-10-16T00:00'),Date.UTC(2026,9,15,16)/1000);assert.ok(Number.isNaN(h.parseTime('2026-02-30T00:00')));assert.ok(Number.isNaN(h.parseTime('2026-10-16T25:00')));
 h.element('taFrom').value='2026-10-16T00:00';h.element('taTo').value='2026-10-17T00:00';h.state.query.user_id=12;h.state.query.method='historical_formula_v1';h.state.query.subject='customer_wallet';h.state.query.unit='USD';h.state.query.quota_per_unit='500000';h.element('taMethod').value='historical_formula_v1';
 const q=h.readFilters();assert.equal(q.to-q.from,86400);assert.equal(q.user_id,12);assert.equal(q.subject,'customer_wallet');h.element('taMethod').value='';assert.equal(h.readFilters().subject,undefined);
});
test('Token report distinguishes source off, incomplete, candidate, amounts and unavailable evidence',async()=>{
 const h=harness(),p=h.load();settle(h);await p;
 assert.match(h.element('taFactsHealth').innerHTML,/采集未启用/);assert.match(h.element('taDetectionHealth').innerHTML,/检测未启用/);assert.match(h.element('taNotifyHealth').innerHTML,/实际投递关闭/);
 assert.match(h.element('taList').innerHTML,/异常候选，待核查/);assert.match(h.element('taAmounts').innerHTML,/0.000004/);assert.match(h.element('taUnavailable').innerHTML,/不可核算/);assert.match(h.element('taCoverage').textContent,/采集未启用.*不能确认完整/);assert.doesNotMatch(h.element('taList').innerHTML,/确认上游多扣/);assert.equal(h.requests.length,5);
});
test('health separates scheduled late-evidence reviews from unresolved associations',async()=>{
 const h=harness(),p=h.load();
 const facts={...health,available:true,facts:100,pending_links:100,closed_link_reviews:0,unresolved_links:2,link_states:{unique:98,waiting:1,ambiguous:1}};
 for(const [i,v]of [report(),facts,detectionHealth,notifyHealth,access].entries())h.requests[i].resolve(response(v));await p;
 const text=h.element('taFactsHealth').innerHTML;
 assert.match(text,/待执行关联复查 100（含已唯一关联）/);assert.match(text,/未解决关联 2/);assert.match(text,/98 \/ 1 \/ 1 \/ 0 \/ 0/);
 assert.doesNotMatch(text,/关联待补 100/);
});
test('refresh, switch away, stale errors and late 401 are fenced',async()=>{
 const h=harness();const p=h.load(),q=h.load();assert.ok(h.requests[0].options.signal.aborted);const newer=report({},'new');newer.items[0].display.customer_name='new customer';settle(h,5,newer);await q;settle(h,0,{error:'old'},401);await p;assert.equal(h.location.href,'');assert.match(h.element('taList').innerHTML,/new customer/);assert.equal(h.state.items[0].request.model,'new');
 const r=h.load();h.window.tokenAnomalyDeactivate();const stale=report({},'stale');stale.items[0].display.customer_name='stale customer';settle(h,10,stale);await r;assert.doesNotMatch(h.element('taList').innerHTML,/stale customer/);
});
test('next page failure is retryable and duplicate identities do not duplicate selection or counts',async()=>{
 const h=harness();let p=h.load();settle(h);await p;p=h.load(true);settle(h,5,{error:'temporary'},503);await p;assert.equal(h.element('taNext').disabled,false);
 p=h.load(true);const r=report();r.items=[item(),item(11)];r.has_more=false;settle(h,10,r);await p;assert.equal(h.state.items.length,2);assert.equal(h.element('taNext').disabled,true);assert.equal(JSON.parse(h.requests[10].options.body).after_id,10);
});

test('same-window refresh preserves visible pages until all replacement pages succeed',async()=>{
 const h=harness();let p=h.load();settle(h);await p;
 const second=report();second.items=[item(11)];second.has_more=false;
 p=h.load(true);settle(h,5,second);await p;
 const previous=h.element('taList').innerHTML,range=JSON.stringify(h.state.query);
 p=h.load();assert.equal(h.element('taList').innerHTML,previous);assert.equal(h.state.items.length,2);
 settle(h,10,report());await new Promise(setImmediate);
 assert.equal(h.requests.length,16);assert.equal(JSON.parse(h.requests[15].options.body).after_id,10);
 assert.equal(h.element('taList').innerHTML,previous);
 h.requests[15].resolve(response(second));await p;
 assert.equal(h.state.items.length,2);assert.equal(h.state.loadedPages,2);assert.equal(JSON.stringify(h.state.query),range);
 p=h.load();settle(h,16,report());await new Promise(setImmediate);
 h.requests[21].resolve(response({error:'second page unavailable'},503));await p;
 assert.equal(h.state.items.length,2);assert.equal(h.element('taList').innerHTML,previous);
 assert.equal(h.element('taError').hidden,false);assert.match(h.element('taUpdated').textContent,/查询失败/);
});
test('malformed report and unavailable health never claim success; input remains usable',async()=>{
 const h=harness(),p=h.load();settle(h,0,{});await p;assert.equal(h.element('taError').hidden,false);assert.equal(h.state.report,null);assert.equal(h.element('taFrom').disabled,false);assert.equal(h.element('taList').attrs['aria-busy'],'false');
});
test('detail is frozen revision, escapes text, and closing fences late evidence',async()=>{
 const h=harness();h.renderDetail(detail(1));assert.match(h.element('taDetailBody').innerHTML,/1/);assert.match(h.element('taDetailBody').innerHTML,/不是纯正文/);assert.match(h.element('taDetailBody').innerHTML,/缺失/);assert.match(h.element('taDetailBody').innerHTML,/null/);
 const d=detail();d.evidence.input.fact.model='<img src=x>';h.renderDetail(d);assert.doesNotMatch(h.element('taDetailBody').innerHTML,/<img src/);assert.match(h.element('taDetailBody').innerHTML,/&lt;img/);
 const p=h.openDetail(item(),1);h.element('taDetailClose').listeners.click();h.requests[0].resolve(response(detail()));await p;assert.equal(h.element('taDetail').open,false);assert.ok(h.requests[0].options.signal.aborted);
});

test('real frozen fact missing_fields JSON and report arrays retain the same reasons',async()=>{
 const h=harness(),missing=[{name:'upstream_request_id',reason:'source_missing'},{name:'route',reason:'not_recorded_at_request_time'}];
 for(const encoded of [missing,JSON.stringify(missing)]){
  const d=detail();d.evidence.input.fact.missing_fields=encoded;
  h.renderDetail(d);
  const text=h.element('taDetailBody').innerHTML;
  assert.match(text,/fixture-request-10/);assert.match(text,/upstream_request_id:source_missing/);assert.match(text,/route:not_recorded_at_request_time/);
  assert.doesNotMatch(text,/unique \/ pending/);
  const p=h.load(),offset=h.requests.length-5,r=report();r.items[0].display.missing_fields=encoded;
  settle(h,offset,r);await p;
  assert.match(h.element('taList').innerHTML,/upstream_request_id: source_missing/);
 }
});

test('pending detail keeps request identity, dimensions, transport provenance and missing reasons',async()=>{
 const h=harness(),p=h.openDetail(item());
 h.requests[0].resolve(response({pending:true,request:{source_epoch:'retained-epoch',log_id:10,request_id:'pending-request',created_at:at,user_id:2,channel_id:3,model:'pending-model',request_path:'/v1/messages',parser_version:'parser-v7'},detection_state:'association_pending',association_reason:'exact_request_evidence_not_found',missing_fields:[{name:'upstream_request_id',reason:'source_missing'}],display:{request_length_bytes:'1234',request_length_meaning:'whole_http_request',request_length_source:'newapi.logs.other.request_length',request_length_observed_at:at,adapter_version:'adapter-v1',field_sources:{prompt_tokens:'newapi.logs.type2:sql_scalar:valid'}}}));
 await p;
 const html=h.element('taDetailBody').innerHTML;
 for(const text of ['pending-request','retained-epoch:10','客户 #2','渠道 #3','pending-model','/v1/messages','parser-v7','adapter-v1','1,234 字节','whole_http_request','缺失字段','upstream_request_id:source_missing','精确请求证据未找到','规则、参考窗口与样本数：检测尚未完成'])assert.ok(html.includes(text),text);
 assert.doesNotMatch(html,/证据不足，未确认|已确认多扣/);
});

test('corrupt missing-field metadata is explicit and cannot break or inject the detail',()=>{
 const h=harness();
 for(const raw of ['{invalid','{}',123,[null]]){
  const d=detail();d.evidence.input.fact.missing_fields=raw;
  h.renderDetail(d);assert.match(h.element('taDetailBody').innerHTML,/missing_fields:stored_metadata_invalid/);
 }
 const d=detail();d.evidence.input.fact.missing_fields=JSON.stringify([{name:'<img src=x>',reason:'<script>alert(1)</script>'}]);
 h.renderDetail(d);assert.doesNotMatch(h.element('taDetailBody').innerHTML,/<(?:img|script)[ >]/);assert.match(h.element('taDetailBody').innerHTML,/&lt;img src=x&gt;/);
 for(const raw of [null,undefined,'']){
  d.evidence.input.fact.missing_fields=raw;h.renderDetail(d);
  assert.match(h.element('taDetailBody').innerHTML,/missing_fields:not_recorded_in_fact/);
 }
});
test('export binds exact revisions, retries same id, and target change discards stale result',async()=>{
 const h=harness();h.state.access=access;h.state.query.source_epoch='fixture-epoch';h.state.selected.set(10,{log_id:10,revision:2,decision_hash:'a'.repeat(64)});h.element('taTarget').value='supplier-fixture';h.element('taExportConfirm').checked=true;h.syncExport();
 const p=h.createExport();const first=JSON.parse(h.requests[0].options.body);assert.equal(first.selection[0].revision,2);assert.equal(h.requests[0].options.headers['X-Monitor-Token-CSRF'],'fixture-csrf');
 h.requests[0].reject(Error('network'));await p;const q=h.createExport();assert.equal(JSON.parse(h.requests[1].options.body).request_id,first.request_id);
 h.element('taTarget').value='supplier-second';h.element('taTarget').listeners.change();h.requests[1].resolve(response(receipt));await q;assert.equal(h.state.receipt,null);assert.ok(h.requests[1].options.signal.aborted);assert.equal(h.element('taDownloadExport').disabled,true);
});
test('unchecking consent while export is pending invalidates its response',async()=>{
 const h=harness();h.state.access=access;h.state.selected.set(10,{log_id:10,revision:2,decision_hash:'a'.repeat(64)});h.element('taTarget').value='supplier-fixture';h.element('taExportConfirm').checked=true;h.syncExport();const p=h.createExport();h.element('taExportConfirm').checked=false;h.element('taExportConfirm').listeners.change();h.requests[0].resolve(response(receipt));await p;assert.equal(h.state.receipt,null);
});
test('customer diagnosis entry carries only safe local context, with no fetch from detail click',()=>{
 const h=harness();h.state.detailItem=item();h.element('taTroubleshoot').listeners.click();assert.equal(h.requests.length,0);assert.equal(h.navigation[0].preset,'token_local_review');assert.equal(h.navigation[0].user_id,'2');assert.equal(h.navigation[0].model,'fixture-model');
});
test('notification link preserves explicit frozen time, target and rule',async()=>{
 const h=harness();h.window.monitorNavigationContext=()=>({channel_id:'3',model:'fixture-model',group:'actual-group',token_from:String(at-100),token_to:String(at+1),token_rule:'historical_price_difference'});
 h.window.tokenAnomalyActivate();const q=JSON.parse(h.requests[0].options.body);assert.equal(q.from,at-100);assert.equal(q.to,at+1);assert.equal(q.group,'actual-group');assert.equal(q.rule,'historical_price_difference');settle(h);await new Promise(setImmediate);
});
test('all new API requests are local and export stays independently disabled',async()=>{
 const h=harness(),p=h.load();settle(h);await p;assert.ok(h.requests.every(r=>r.url.startsWith('/anomalies/token/')));h.state.access={...access,export_enabled:false};h.element('taExportConfirm').checked=true;h.element('taTarget').value='supplier-fixture';h.state.selected.set(10,{});h.syncExport();assert.equal(h.element('taCreateExport').disabled,true);assert.match(h.element('taExportNote').textContent,/开关关闭/);
});

test('unknown dimension is explicit, and clearing a known channel does not become channel zero',async()=>{
 const h=harness();h.state.report=report();h.state.report.analysis.breakdown=[{kind:'group',key:'',requests:1,candidate_requests:1}];
 h.element('taBreakdown').listeners.click({target:{closest:()=>({dataset:{taDimension:'0'}})}});
 const q=JSON.parse(h.requests[0].options.body);assert.equal(q.group_set,true);assert.equal(q.group,'');settle(h);await new Promise(setImmediate);
 h.state.query.channel_id=3;h.state.query.channel_set=true;h.element('taChannel').value='';const f=h.readFilters();assert.equal(f.channel_set,false);assert.equal(f.group_set,true);
});

test('unsafe JSON numeric integers are never displayed or sent as exact identities',async()=>{
 const h=harness();assert.match(h.whole(9007199254740992),/精度/);assert.equal(h.whole('9007199254740993'),'9,007,199,254,740,993');
 const i=item();i.request.log_id=9007199254740992;await h.openDetail(i);assert.equal(h.requests.length,0);assert.match(h.element('taError').textContent,/精度/);
 h.element('taChannel').value='9007199254740993';assert.throws(()=>h.readFilters());
});

test('default table exposes customer, channel, request time and frozen Token comparison without relabeling quota',async()=>{
 const h=harness(),p=h.load(),r=report();
 r.items[0].display={customer_name:'客户 <甲>',channel_name:'渠道甲',input_tokens:'9007199254740993',output_tokens:'0',cache_read_tokens:null,cache_write_tokens:null,reference_input_tokens:'100',input_difference_tokens:'9007199254740893',comparison_kind:'statistical_reference'};
 settle(h,0,r);await p;
 const text=h.element('taList').innerHTML;
 assert.match(text,/客户 &lt;甲&gt;/);assert.match(text,/渠道甲/);assert.match(text,/ID：2/);assert.match(text,/ID：3/);assert.match(text,/9,007,199,254,740,993/);assert.match(text,/输出 0/);assert.match(text,/缓存读取 不可计算/);assert.match(text,/参考上界 100/);assert.match(text,/统计参考差异，待核实/);assert.match(text,/<td>未知<\/td>/);
 assert.doesNotMatch(text,/涉及额度|已确认多扣/);
 const p2=h.load();settle(h,5,report());await p2;
 assert.match(h.element('taList').innerHTML,/差值：不可计算/);
});

test('local formula confirmation is separate from statistical candidates and upstream billing',async()=>{
 const h=harness(),p=h.load(),r=report();
 const assessment={state:'local_metering_difference',evidence_level:'exact',local_difference_confirmed:true,upstream_overcharge_confirmed:false,request_length_bytes:'1000',input_tokens:'20000',output_tokens:'2',model:'fixture-model',request_path:'/v1/messages',normal_quota_from:'10002',normal_quota_to:'10002',actual_quota:'10019',formula_difference_quota:'17',missing_fields:['statistical_reference:reference_insufficient']};
 r.items[0].display.assessment=assessment;settle(h,0,r);await p;
 assert.match(h.element('taList').innerHTML,/本地计量差异已核实/);
 const d=detail();d.assessment=assessment;h.renderDetail(d);
 const text=h.element('taDetailBody').innerHTML;
 assert.match(text,/上游真实 Token 与扣账未确认/);assert.match(text,/10019 − 10002 = 17/);assert.match(text,/不是正文大小或输入 Token/);assert.match(text,/统计参考/);
 assert.doesNotMatch(text,/<p class="ta-warning">异常候选，待核查/);
 d.assessment={...assessment,state:'evidence_insufficient',local_difference_confirmed:false,output_tokens:null,normal_quota_from:null,normal_quota_to:null,formula_difference_quota:null,missing_fields:['completion_tokens:missing']};h.renderDetail(d);
 assert.match(h.element('taDetailBody').innerHTML,/输出 Token：missing/);assert.match(h.element('taDetailBody').innerHTML,/本地公式正常范围<\/dt><dd>不可计算/);
});
