/* Local Token findings: candidates do not confirm supplier overcharging. */
(function(){
'use strict';
const $=id=>document.getElementById(id);
const state={active:false,inited:false,seq:0,detailSeq:0,exportSeq:0,abort:null,detailAbort:null,exportAbort:null,refreshTimer:null,refreshHandlersInited:false,query:null,report:null,items:[],loadedQuery:'',loadedPages:0,selected:new Map(),access:null,receipt:null,exportRequest:null,breakdownLimit:40,
 // health 保留本轮原值,供空态指出具体原因。健康详情已折叠,空态不能只说"没有数据"。
 health:{facts:null,detection:null,notify:null,failed:[]}};
const rules={input_inflation:'输入计量放大',cache_structure:'缓存计费结构变化',usage_format:'用量格式异常',historical_price_difference:'日志计价差异',telemetry_gap:'采集缺口'};
const levels={exact:'直接日志事实',correlated:'相关性证据',inferred:'统计推断',unavailable:'证据不可用'};
const methods={observed_consumption_not_loss:'涉及消费（不是损失）',historical_formula_v1:'可复算日志费用差额',input_upper_reference_v1:'基于参考的估计损失'};
const statuses={candidate:'异常候选，待核查',insufficient:'有规则待补证',not_hit:'可用规则未命中（不等于确认正常）'};
const detectionStates={source_pending:'来源待处理',source_failed:'来源读取失败，等待重试',association_pending:'关联待处理',association_ambiguous:'关联多义，等待复核',association_conflict:'关联冲突，等待证据',detection_pending:'检测排队中',evidence_insufficient:'证据不足，未确认',statistical_candidate:'统计候选，待核查',log_formula_difference:'日志公式差额',local_metering_difference:'本地计量差异已核实',local_formula_consistent:'本地公式一致（不代表上游正常）',not_hit:'可用规则未命中（不等于确认正常）'};
const fieldStates={derived:'按已核验版本推导',valid:'有效',missing:'缺失',null:'null',sentinel:'哨兵',invalid:'非法',unsupported:'不支持'};
const gapLabels={detection_disabled:'检测未启用；仅查询已保存历史',detection_pending_or_stale:'存在待检测或已过时的判断',source_range_incomplete:'所选窗口来源覆盖不完整',summary_truncated_not_full_population:'汇总超过 5000 条预算，完整总量未就绪；请缩小范围',reference_loss_not_supported:'参考不足或不支持此估计',historical_pricing_adapter_unverified:'历史计价适配未经确认',source_window_incomplete:'来源窗口尚未完整',comparable_fields_missing:'缺少可比字段',reference_insufficient:'参考不足',threshold_unconfigured:'规则阈值未配置',smtp_disabled_or_unavailable:'SMTP 未启用或不可用',notification_delivery_disabled:'实际通知已关闭',notification_ttl_expired:'通知已过有效期',smtp_outcome_unknown:'SMTP 结果不确定，将有界重试',notification_attempts_exhausted:'重试次数已耗尽',local_notification_incomplete:'本轮通知处理未完整',notification_capacity_or_budget_limited:'通知容量或日预算受限'};
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
Object.assign(gapLabels,{model_mapping_unverified:'历史模型映射未记录；仅作统计候选判断',reference_samples_or_channels_insufficient:'同模型、相近大小的跨渠道参考不足',reference_query_truncated:'参考查询达到上限，尚不能完成比较',reference_snapshot_capacity:'参考样本超过证据保存上限，尚不能完成比较',unverified_statistical_reference:'统计参考；不代表已确认真实 Token 数',input_exceeds_frozen_statistical_bound:'完整输入超过冻结的同模型统计参考上界',statistical_comparison_not_token_truth:'完整输入未超过统计阈值；不证明真实 Token 数',supplier_independence_unverified:'不同渠道的供应商独立性未确认',same_size_does_not_prove_same_content:'入口大小相同不证明正文相同',encoding_and_conversion_unknown:'历史编码与协议转换信息不足',evidence_insufficient:'部分请求的判断证据不足'});
Object.assign(gapLabels,{anchor_frozen_reference_capacity:'新增参考达到上限，继续使用已校验的冻结参考',reference_expansion_capacity:'本次参考扩展受限；判断使用原冻结窗口及样本'});
Object.assign(gapLabels,{logged_format_consistent_not_usage_truth:'日志字段格式一致；不代表已核实真实用量',usage_fields_or_semantic_incomplete:'用量字段或计量语义不足',cache_reference_or_price_incomplete:'缓存参考或历史价格不足',cache_reference_insufficient:'缓存对照样本不足',local_formula_consistent_not_upstream_verification:'本站历史公式复算一致；不代表已核实上游用量'});
gapLabels.recorded_total_differs_from_usage_components='来源记录的总输入与按已记录计量语义合计的输入字段不一致';
Object.assign(gapLabels,{reference_samples_insufficient:'满足可比条件的样本数量不足',reference_channels_insufficient:'满足可比条件的其他渠道数量不足',no_comparable_samples_in_window:'窗口内没有满足全部可比条件的样本',source_field_not_adapted:'源字段存在，但当前适配尚不支持',comparable_samples_unavailable:'字段可比，但合格参考不足',versioned_emitter_semantic:'由已核验生产版本的日志写入语义确定'});
const reason=v=>{const text=String(v||'');if(gapLabels[text])return gapLabels[text];if(text.startsWith('incompatible:'))return '不可比字段：'+text.slice(13);const at=text.indexOf(':');return at>0&&rules[text.slice(0,at)]?rules[text.slice(0,at)]+'：'+reason(text.slice(at+1)):text||'无已记录错误'};
const time=v=>Number(v)>0?new Date(Number(v)*1000).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}):'尚无记录';
function contractOperandsBody(u){
 if(!u.source_contract)return '';
 return '<h4>已核验来源版本与派生操作数</h4><p>原始字段保持其缺失状态；以下仅按指定来源、版本和时间范围解释日志省略字段。</p><pre>'+esc(JSON.stringify(u.source_contract,null,2))+'</pre><dl>'+Object.entries(u.contract_operands||{}).map(([k,v])=>'<dt>'+esc(k)+'</dt><dd>'+esc(v.value)+'；依据 '+esc(v.reason)+'</dd>').join('')+'</dl>';
}
function whole(v){if(v==null||v==='')return '不可计算';if(typeof v==='number'&&!Number.isSafeInteger(v))return '超出页面整数精度，请核对原始计数字符串';try{return BigInt(v).toLocaleString('zh-CN')}catch{return '不可计算'}}
function fraction(v){
 if(v==null)return '不可计算';
 try{const parts=String(v).split('/');if(parts.length>2)return '不可计算';let n=BigInt(parts[0]),d=BigInt(parts[1]||'1');if(d<=0n)return '不可计算';const sign=n<0n?'-':'';if(n<0n)n=-n;const scaled=n*1000000n/d,tail=String(scaled%1000000n).padStart(6,'0').replace(/0+$/,'');return (n*1000000n%d?'≈':'')+sign+(scaled/1000000n).toLocaleString('zh-CN')+(tail?'.'+tail:'')}catch{return '不可计算'}
}
let timeCalendar=null,timeTarget='';
function closeTimePicker(){if($('taTimeDialog')?.open)$('taTimeDialog').close();timeTarget=''}
function openTimePicker(id){
 const input=$(id);if(!input||!['taFrom','taTo'].includes(id))return;
 if(!window.flatpickr){input.focus();showError('本地日历资源不可用，可继续键盘输入北京时间。');return}
 timeTarget=id;
 if(!timeCalendar){timeCalendar=window.flatpickr($('taCalendarInput'),{inline:true,appendTo:$('taCalendar'),locale:'zh',dateFormat:'Y-m-d',disableMobile:true,allowInput:false});timeCalendar.monthsDropdownContainer?.addEventListener('keyup',e=>e.stopPropagation());for(const [key,n]of [['taHour',24],['taMinute',60]])$(key).innerHTML=Array.from({length:n},(_,i)=>'<option>'+String(i).padStart(2,'0')+'</option>').join('')}
 const value=Number.isFinite(parseTime(input.value))?input.value:inputTime(Math.floor(Date.now()/60000)*60);
 timeCalendar.setDate(value.slice(0,10),false,'Y-m-d');$('taHour').value=value.slice(11,13);$('taMinute').value=value.slice(14,16);$('taTimeTitle').textContent=(id==='taFrom'?'开始':'结束')+'日期和时间（北京时间）';$('taTimeDialog').showModal();timeCalendar.calendarContainer.querySelector('.selected')?.focus();
}
function applyTimePicker(){const date=timeCalendar?.selectedDates[0],input=$(timeTarget);if(!date||!input)return;input.value=timeCalendar.formatDate(date,'Y-m-d')+'T'+$('taHour').value+':'+$('taMinute').value;$('taTimeMode').value='0';closeTimePicker();input.focus()}
function inputTime(v){return new Date((v+28800)*1000).toISOString().slice(0,16)}
function parseTime(v){if(!/^\d{4}-\d\d-\d\dT\d\d:\d\d$/.test(v))return NaN;const ts=Date.parse(v+':00+08:00')/1000;return Number.isFinite(ts)&&inputTime(ts)===v?ts:NaN}
gapLabels.detection_queue_rebuilding='检测队列正在分批恢复；当前待处理计数尚不完整';
const assessmentLabels={statistical_candidate:'统计候选，待核查',evidence_insufficient:'证据不足',log_formula_difference:'日志公式差额',local_metering_difference:'本地计量差异已核实',local_formula_consistent:'本地日志公式一致',logged_usage_inconsistency:'日志用量字段不一致'};
Object.assign(gapLabels,{health_counts_rebuilding:'健康计数正在重建，当前计数不可用',health_counts_inconsistent:'健康计数不一致，等待后台重建',local_health_unavailable:'本地健康状态暂不可用',no_detection_checkpoint:'尚无检测提交记录',local_detection_health_unavailable:'检测状态暂不可用',local_detection_capacity_unavailable:'检测容量计数暂不可用',local_detection_backlog_unavailable:'检测队列状态暂不可用',upstream_billing_evidence_missing:'上游真实 Token 与扣账证据未取得'});
assessmentLabels.partially_evaluated='部分规则已判断，其余等待证据';
detectionStates.partially_evaluated='部分规则已判断，其余等待证据';
function resultStatus(r){if(r.candidate)return 'candidate';return !r.source_complete||(r.rules||[]).some(x=>!x.eligible)?'insufficient':'not_hit'}
function init(){
 if(state.inited)return;state.inited=true;
 const to=Math.floor(Date.now()/60000)*60;$('taFrom').value=inputTime(to-900);$('taTo').value=inputTime(to);$('taTimeMode').value='900';$('taStatusFilter').value='';
 $('taTimeMode').addEventListener('change',applyFilters);
 for(const id of ['taFrom','taTo'])$(id).addEventListener('input',()=>{$('taTimeMode').value='0'});
 $('taFilters').addEventListener('submit',e=>{e.preventDefault();applyFilters()});
 $('taRefresh').addEventListener('click',()=>load(false,true));
 $('taReset').addEventListener('click',()=>{for(const id of ['taChannel','taGroup','taModel','taRule','taEvidence','taMethod'])$(id).value='';$('taStatusFilter').value='';$('taTimeMode').value='900';const end=Math.floor(Date.now()/60000)*60;$('taFrom').value=inputTime(end-900);$('taTo').value=inputTime(end);state.query=null;applyFilters()});
 document.querySelectorAll('[data-ta-window]').forEach(b=>b.addEventListener('click',()=>{const end=Math.floor(Date.now()/60000)*60;$('taTimeMode').value='0';$('taFrom').value=inputTime(end-Number(b.dataset.taWindow)*86400);$('taTo').value=inputTime(end);applyFilters()}));
 document.querySelectorAll('[data-ta-picker]').forEach(b=>b.addEventListener('click',()=>openTimePicker(b.dataset.taPicker)));
 $('taTimeCancel').addEventListener('click',closeTimePicker);$('taTimeApply').addEventListener('click',applyTimePicker);
 $('taTimeDialog').addEventListener('cancel',()=>{timeTarget=''});
 $('taNext').addEventListener('click',()=>load(true));
 $('taList').addEventListener('click',e=>{const b=e.target.closest?.('[data-ta-detail]');if(b){const item=state.items[Number(b.dataset.taDetail)];if(item)openDetail(item)}});
 $('taList').addEventListener('change',e=>{const el=e.target;if(!el.matches('[data-ta-select]'))return;const item=state.items[Number(el.dataset.taSelect)];if(!item)return;const h=item.request;if(!Number.isSafeInteger(h.log_id))return;if(el.checked)state.selected.set(h.log_id,{log_id:h.log_id,revision:h.revision,decision_hash:h.decision_hash});else state.selected.delete(h.log_id);invalidateExport();syncExport()});
 $('taSummary').addEventListener('click',e=>{const b=e.target.closest?.('[data-ta-status]');if(b){$('taStatusFilter').value=b.dataset.taStatus;applyFilters()}});
 $('taAmounts').addEventListener('click',e=>{const b=e.target.closest?.('[data-ta-amount]');if(!b)return;const a=state.report?.summary.amounts[b.dataset.taAmount];if(a){state.query={...state.query,status:'candidate',method:a.method,subject:a.subject,unit:a.unit,quota_per_unit:a.quota_per_unit,unavailable:false};syncInputs();load()}});
 $('taUnavailable').addEventListener('click',e=>{const b=e.target.closest?.('[data-ta-unavailable]');if(b){state.query={...state.query,status:'candidate',method:b.dataset.taUnavailable,unavailable:true,subject:'',unit:'',quota_per_unit:''};syncInputs();load()}});
 $('taDimension').addEventListener('change',()=>{state.breakdownLimit=40;renderBreakdown()});
 $('taBreakdownMore').addEventListener('click',()=>{state.breakdownLimit+=40;renderBreakdown()});
 $('taBreakdown').addEventListener('click',e=>{const b=e.target.closest?.('[data-ta-dimension]');if(!b)return;const v=state.report?.analysis.breakdown[Number(b.dataset.taDimension)];if(!v)return;const q={...state.query};if(v.kind==='hour'){q.follow_seconds=0;q.from=Math.max(q.from,Number(v.key));q.to=Math.min(q.to,Number(v.key)+3600)}else {q[v.kind]=['channel_id','user_id'].includes(v.kind)?Number(v.key):v.key;q[({channel_id:'channel_set',user_id:'user_set',group:'group_set',model:'model_set'})[v.kind]]=true}state.query=q;syncInputs();load()});
 $('taDetailClose').addEventListener('click',closeDetail);
 $('taTroubleshoot').addEventListener('click',()=>{const h=state.detailItem?.request;if(!h||!Number.isSafeInteger(h.user_id)||!Number.isSafeInteger(h.channel_id))return;window.logChainOpen?.({preset:'token_local_review',user_id:String(h.user_id),channel_id:String(h.channel_id),model:h.model,date:inputTime(h.created_at).slice(0,10)});closeDetail()});
 $('taDetail').addEventListener('cancel',()=>{state.detailSeq++;state.detailAbort?.abort()});
 $('taRevisionForm').addEventListener('submit',e=>{e.preventDefault();if(state.detailItem)openDetail(state.detailItem,Number($('taRevision').value))});
 $('taOriginal').addEventListener('click',()=>{if(state.detailItem)openDetail(state.detailItem,1)});
 $('taLatest').addEventListener('click',()=>{if(state.detailItem)openDetail(state.detailItem,state.detailItem.request.revision)});
 $('taTarget').addEventListener('change',()=>{invalidateExport();$('taExportConfirm').checked=false;syncExport()});
 $('taCustomerPrices').addEventListener('change',()=>{invalidateExport();syncExport()});
 $('taExportConfirm').addEventListener('change',()=>{if(!$('taExportConfirm').checked)invalidateExport();syncExport()});
 $('taCreateExport').addEventListener('click',createExport);$('taDownloadExport').addEventListener('click',downloadExport);
 ensureRefreshHandlers();
}
window.tokenAnomalyActivate=function(){
 init();state.active=true;const nav=window.monitorNavigationContext?.()||{};
 if(nav.channel_id||nav.model||nav.token_from){
  const to=Math.floor(Date.now()/60000)*60;state.query={from:to-86400,to,status:'candidate',channel_id:Number(nav.channel_id)||0,model:nav.model||'',group:nav.group||'',source_epoch:nav.token_epoch||''};
  const from=Number(nav.token_from),end=Number(nav.token_to);
  if(nav.token_from){$('taTimeMode').value='0';if(!Number.isSafeInteger(from)||!Number.isSafeInteger(end)||from<=0||end<=from||end-from>7*86400){showError('通知冻结窗口超出本地查询限制，请明确选择最多 7 天的范围；未自动替换日期。');syncInputs();return}state.query.from=from;state.query.to=end;state.query.group_set=true;state.query.model_set=true}
  if(rules[nav.token_rule]&&nav.token_rule!=='telemetry_gap')state.query.rule=nav.token_rule;syncInputs()
 }

 if(!state.query){try{state.query=readFilters()}catch(e){showError(e.message);return}}load();scheduleRefresh();
};
window.tokenAnomalyDeactivate=function(){closeTimePicker();state.active=false;state.seq++;state.abort?.abort();state.abort=null;state.exportSeq++;state.exportAbort?.abort();state.exportAbort=null;closeDetail();if(state.refreshTimer){clearTimeout(state.refreshTimer);state.refreshTimer=null}syncExport()};
function scheduleRefresh(){
 if(state.refreshTimer)clearTimeout(state.refreshTimer);
 state.refreshTimer=null;
 const pane=$('tab-anomalies');
 if(!state.active||document.hidden||pane?.hidden)return;
 state.refreshTimer=setTimeout(()=>{
  state.refreshTimer=null;
  if(state.active&&!document.hidden&&!pane?.hidden)load(false,true);
  scheduleRefresh();
 },30000);
 state.refreshTimer?.unref?.();
}
function ensureRefreshHandlers(){
 if(state.refreshHandlersInited)return;
 state.refreshHandlersInited=true;
 const refreshIfVisible=()=>{
  const pane=$('tab-anomalies');
  if(!state.active||document.hidden||pane?.hidden)return;
  load(false,true);scheduleRefresh();
 };
 window.addEventListener?.('focus',refreshIfVisible);
 document.addEventListener?.('visibilitychange',()=>{
  if(document.hidden){if(state.refreshTimer){clearTimeout(state.refreshTimer);state.refreshTimer=null}}
  else refreshIfVisible();
 });
}
function readFilters(){
 const from=parseTime($('taFrom').value),to=parseTime($('taTo').value),channel=$('taChannel').value.trim();
 if(!Number.isFinite(from)||!Number.isFinite(to)||to<=from||to-from>7*86400)throw Error('请选择有效的北京时间起止，结束晚于开始，范围最多 7 天。');
 if(channel&&(!/^[1-9]\d*$/.test(channel)||!Number.isSafeInteger(Number(channel))))throw Error('渠道 ID 必须是正整数');
 return {follow_seconds:[900,3600].includes(Number($('taTimeMode')?.value))?Number($('taTimeMode').value):0,user_id:state.query?.user_id||0,user_set:!!state.query?.user_set,channel_set:!channel&&state.query?.channel_id===0&&!!state.query?.channel_set,group_set:$('taGroup').value.trim()===(state.query?.group||'')&&!!state.query?.group_set,model_set:$('taModel').value.trim()===(state.query?.model||'')&&!!state.query?.model_set,...(state.query?.method===$('taMethod').value?{subject:state.query?.subject||'',unit:state.query?.unit||'',quota_per_unit:state.query?.quota_per_unit||'',unavailable:!!state.query?.unavailable}:{}),source_epoch:state.access?.source_epoch||state.query?.source_epoch||'',from,to,channel_id:channel?Number(channel):0,model:$('taModel').value.trim(),group:$('taGroup').value.trim(),status:$('taStatusFilter').value,rule:$('taRule').value,evidence_level:$('taEvidence').value,method:$('taMethod').value,limit:50};
}
function syncInputs(){const q=state.query||{};if($('taTimeMode'))$('taTimeMode').value=String(q.follow_seconds||0);if(q.from)$('taFrom').value=inputTime(q.from);if(q.to)$('taTo').value=inputTime(q.to);for(const [id,key]of [['taChannel','channel_id'],['taGroup','group'],['taModel','model'],['taStatusFilter','status'],['taRule','rule'],['taEvidence','evidence_level'],['taMethod','method']])$(id).value=q[key]||''}
function applyFilters(){try{state.query=readFilters();load()}catch(e){showError(e.message)}}
function showError(message){$('taError').hidden=false;$('taError').textContent=message}
async function api(path,body,controller,csrf){
 const headers={Accept:'application/json'};if(body!==undefined)headers['Content-Type']='application/json';if(csrf)headers['X-Monitor-Token-CSRF']=csrf;
 const r=await fetch('/anomalies/token/'+path,{method:body===undefined?'GET':'POST',headers,body:body===undefined?undefined:JSON.stringify(body),cache:'no-store',credentials:'same-origin',signal:controller?.signal});
 const data=await r.json();if(!r.ok){const e=Error(data.error||'本地查询失败');e.status=r.status;throw e}return data;
}
function tokenQueryIdentity(q){return JSON.stringify([Number(q?.follow_seconds||0),['from','to','user_id','channel_id'].map(k=>q?.follow_seconds&&['from','to'].includes(k)?0:Number(q?.[k]||0)),['user_set','channel_set','group_set','model_set','unavailable'].map(k=>!!q?.[k]),['source_epoch','group','model','status','rule','evidence_level','method','subject','unit','quota_per_unit'].map(k=>String(q?.[k]||''))])}
async function load(next=false,live=false){
 if(!state.active)return;
 if(!next&&[900,3600].includes(state.query?.follow_seconds)){const to=Math.floor(Date.now()/1000);state.query={...state.query,from:to-state.query.follow_seconds,to};if(Number($('taTimeMode')?.value)===state.query.follow_seconds&&!['taFrom','taTo'].includes(document.activeElement?.id)){$('taFrom').value=inputTime(state.query.from);$('taTo').value=inputTime(to)}}
 state.abort?.abort();const controller=new AbortController(),seq=++state.seq;state.abort=controller;if(!live)closeDetail();
 state.exportSeq++;state.exportAbort?.abort();state.exportAbort=null;if(!live)state.selected.clear();state.receipt=null;state.exportRequest=null;syncExport();
 const q={...state.query,newest_first:!!state.query.follow_seconds,limit:50};
 const preserving=!!state.report&&state.loadedQuery===tokenQueryIdentity(q),refreshPages=!next&&preserving?Math.max(1,state.loadedPages):1;
 if(next&&state.items.length){const h=state.items.at(-1).request;q.after_at=h.created_at;q.after_id=h.log_id}else if(!preserving){state.items=[];$('taList').innerHTML='';state.report=null;state.loadedPages=0;$('taSummary').textContent='正在读取本地结果…';$('taAmounts').innerHTML='';$('taUnavailable').innerHTML='';$('taBreakdown').innerHTML=''}
 $('taError').hidden=true;$('taList').setAttribute('aria-busy','true');$('taNext').disabled=true;$('taUpdated').textContent='读取中…';
 const paths=[['report','detection/query',q],['facts','health'],['detection','detection/health'],['notify','notifications/health'],['access','access']];
 const results=await Promise.allSettled(paths.map(([,path,body])=>api(path,body,controller)));
 if(seq!==state.seq||!state.active)return;
 try{
  const got={};results.forEach((r,i)=>got[paths[i][0]]=r);
  if(results.some(r=>r.status==='rejected'&&r.reason?.status===401)){location.href='/login';return}
  if(got.access.status==='fulfilled'){state.access=got.access.value;renderAccess()}else{state.access=null;$('taExportNote').textContent='导出权限读取失败，暂不可导出'}
  renderHealth(got);
  if(got.report.status!=='fulfilled')throw got.report.reason;let r=got.report.value;
  if(r.available===false)throw Error('本地检测结果暂不可用，保留上次结果；当前覆盖未知');
  if(!r.summary||!Array.isArray(r.items)||!r.range||!r.analysis)throw Error('本地报告结构不完整');
  let pages=1,refreshed=r.items;
  // Refresh the pages already opened. Publish them together so a slow or
  // failed later page keeps the previous visible snapshot and page position.
  while(!next&&preserving&&pages<refreshPages&&r.has_more&&r.items.length){
   const last=r.items.at(-1).request;
   r=await api('detection/query',{...q,source_epoch:r.range.source_epoch,after_at:last.created_at,after_id:last.log_id},controller);
   if(seq!==state.seq||!state.active)return;
   if(r.available===false)throw Error('本地检测结果暂不可用，保留上次结果；当前覆盖未知');
   if(!r.summary||!Array.isArray(r.items)||!r.range||!r.analysis)throw Error('本地报告结构不完整');
   refreshed=refreshed.concat(r.items);pages++;
  }
  if(!next)r={...r,items:[...new Map(refreshed.map(x=>[x.request.source_epoch+':'+x.request.log_id,x])).values()]};
  const liveView=live?captureLiveView():null;state.reportReadFailed=false;
  state.report=r;state.query.source_epoch=r.range.source_epoch;state.items=next?[...new Map(state.items.concat(r.items).map(x=>[x.request.source_epoch+':'+x.request.log_id,x])).values()]:r.items;
  state.loadedPages=next?state.loadedPages+1:pages;state.loadedQuery=tokenQueryIdentity(state.query);
  renderReport(r);$('taUpdated').textContent=(r.available===false?'所选窗口覆盖未知':r.summary.source_complete?'所选窗口采集完整':'所选窗口采集未完整')+' · 最新已采集请求 '+time(r.latest_progress?.collected_through)+' · 最新已判断请求 '+time(r.latest_progress?.detected_through)+' · 最近判断提交 '+time(r.latest_progress?.decision_committed_at)+' · 历史连续覆盖截止 '+time(r.source_complete_through)+' · 响应生成于 '+time(r.generated_at)+'（北京时间）';
  if(liveView)restoreLiveView(liveView,seq);
 }catch(e){if(seq!==state.seq||!state.active)return;if(e?.name!=='AbortError'){state.reportReadFailed=true;$('taCoverage').textContent=coverageStatus();$('taCoverage').hidden=false;if(!state.items.length)renderList();showError(e?.message||'本地结果不可用');$('taUpdated').textContent='查询失败，不能据此确认无异常'}}
 finally{if(seq===state.seq){state.abort=null;$('taList').setAttribute('aria-busy','false');$('taNext').disabled=!state.report?.has_more;syncExport()}}
}

// A background refresh replaces the list DOM. Capture the current browser view
// just before publishing the complete replacement snapshot and restore it once
// been published; a late/failed page must never move the operator away from
// the request or detail currently under review.
function captureLiveView(){
 const active=document.activeElement;
 const detail=active?.getAttribute?.('data-ta-detail'),select=active?.getAttribute?.('data-ta-select');
 const index=detail??select,item=index!=null?state.items[Number(index)]:null;
 const focus={id:active?.id||'',kind:detail!=null?'detail':select!=null?'select':'',epoch:item?.request.source_epoch,logID:item?.request.log_id};
 return {x:window.scrollX||0,y:window.scrollY||0,focus};
}
function restoreLiveView(view,seq){
 if(!state.active||!view)return;
 const apply=()=>{
  if(!state.active||seq!==state.seq)return;
  window.scrollTo?.(view.x,view.y);
  const f=view.focus||{},index=state.items.findIndex(item=>item.request.source_epoch===f.epoch&&item.request.log_id===f.logID);
  const el=f.id?$(f.id):index>=0&&f.kind?document.querySelector('[data-ta-'+f.kind+'="'+index+'"]'):null;
  if(el&&typeof el.focus==='function'){try{el.focus({preventScroll:true})}catch{el.focus()}}
 };
 if(typeof requestAnimationFrame==='function')requestAnimationFrame(apply);else apply();
}

function renderHealth(got){
 state.health={facts:null,detection:null,notify:null,failed:[]};
 for(const key of ['facts','detection','notify']){
  if(got[key].status==='fulfilled')state.health[key]=got[key].value;else state.health.failed.push(key);
 }
 function block(key,id,title,render){const r=got[key];$(id).innerHTML='<h3>'+title+'</h3>'+(r.status==='fulfilled'?(r.value.available===false?'<p class="ta-warning">状态不可用，当前计数未知。'+esc((r.value.gap_reasons||[]).map(reason).join('；'))+'</p>':render(r.value)):'<p>状态查询失败，当前覆盖未知。</p>')}
 block('facts','taFactsHealth','事实与关联',h=>'<p>'+(h.enabled?'采集已配置启用':'采集未启用')+' · '+(h.source_available?'来源连接可用':'来源不可用或未接入')+'</p><p>扫描至 '+time(h.state?.scan_through)+'<br>完整至 '+time(h.state?.complete_through)+'</p><p>本地事实 '+whole(h.facts)+'；待执行关联复查 '+whole(h.pending_links)+'（含已唯一关联）；已结束复查 '+whole(h.closed_link_reviews)+'；未解决关联 '+whole(h.unresolved_links)+'；窗口待补 '+whole(h.pending_windows)+'</p><p>唯一 / 等待 / 多义 / 冲突 / 已过期：'+whole(h.link_states?.unique??(h.available===true?0:null))+' / '+whole(h.link_states?.waiting??(h.available===true?0:null))+' / '+whole(h.link_states?.ambiguous??(h.available===true?0:null))+' / '+whole(h.link_states?.conflict??(h.available===true?0:null))+' / '+whole(h.link_states?.expired??(h.available===true?0:null))+'</p><p>'+esc((h.gap_reasons||[]).map(reason).join('；'))+'</p>');
 block('detection','taDetectionHealth','参考与检测',h=>'<p>'+(h.enabled?'检测已配置启用':'检测未启用')+' · '+(h.policy_configured?'规则版本已配置':'规则未配置')+'</p><p>最近完成 '+time(h.state?.last_success_at)+'；待处理抽样 '+whole(h.pending_sample_count)+' / '+whole(h.pending_sample_limit)+'</p><p>有效参考 '+whole(h.active_references)+'；延后任务 '+whole(h.deferred_requests)+'</p><p>检测状态：'+esc(JSON.stringify(h.detection_states||{}))+'；重试原因：'+esc(JSON.stringify(h.retry_reasons||{}))+'</p><p>'+esc(reason(h.state?.last_reason))+'</p><p>'+esc((h.gap_reasons||[]).map(reason).join('；'))+'</p>');
 block('notify','taNotifyHealth','通知',h=>'<p>'+(h.evaluation_enabled?'通知评估已配置启用':'通知评估关闭')+' · '+(h.delivery_enabled?'实际投递已配置启用':'实际投递关闭')+'</p><p>最近预算日尝试 '+whole(h.state?.budget_used??0)+'；最近评估 '+time(h.state?.last_evaluated_at)+'</p><p>'+esc((h.counts||[]).map(x=>(x.state||x.State)+': '+(x.count??x.Count)).join('；')||'尚无通知记录')+'</p><p>'+esc(reason(h.state?.last_reason))+'</p>');
 if(got.facts.status==='fulfilled'){const h=got.facts.value;$('taFactsHealth').innerHTML+='<details><summary>字段支持、关联原因与来源缺口</summary><div class="ta-table"><table><thead><tr><th>字段</th><th>各状态已知数量</th></tr></thead><tbody>'+Object.entries(h.field_states||{}).map(([key,counts])=>'<tr><td>'+esc(key)+'</td><td>'+Object.entries(counts).map(([k,v])=>esc(fieldStates[k]||k)+': '+whole(v)).join(' / ')+'</td></tr>').join('')+'</tbody></table></div><p>关联：'+esc(JSON.stringify(h.link_reasons||{}))+'</p><p>大小支持：'+esc(JSON.stringify(h.request_length_states||{}))+'</p><p>历史计价：'+esc(JSON.stringify(h.pricing_support||{}))+'</p><p>这里只展示本地已保留事实的字段分布；缺失不是 0。</p></details>'}
 const n=got.notify.status==='fulfilled'?got.notify.value.recent||[]:[];
 $('taNotifications').innerHTML=n.length?'<div class="ta-table"><table><thead><tr><th>创建</th><th>优先级 / 证据</th><th>状态</th><th>尝试 / 下次</th><th>原因</th></tr></thead><tbody>'+n.map(x=>'<tr><td>'+esc(time(x.created_at))+'</td><td>'+esc(x.severity)+' / '+esc(levels[x.evidence_level]||x.evidence_level)+'</td><td>'+esc(x.state)+'</td><td>'+whole(x.attempts)+' / '+esc(time(x.next_at))+'</td><td>'+esc(reason(x.reason))+'</td></tr>').join('')+'</tbody></table></div>':'<p>尚无通知记录，或通知状态不可用。实际发送受独立开关和预算控制。</p>';
}
function renderReport(r){
 const s=r.summary,a=r.analysis;
 const dims=s.unique_candidate_dimensions||{};
 const summaryCount=v=>s.truncated?'未就绪（超出本次汇总预算）':whole(v);
 $('taCoverage').textContent=coverageStatus();$('taCoverage').hidden=!state.items.length;
 $('taSummary').innerHTML=['candidate','insufficient','not_hit'].map(k=>'<button type="button" data-ta-status="'+k+'">'+esc(statuses[k])+'<b>'+summaryCount(a.status_counts?.[k])+'</b></button>').join('')+'<button type="button" data-ta-status="">范围内已保存结果<b>'+summaryCount(s.observed_results)+'</b></button><p>候选涉及：客户 '+summaryCount(dims.customers)+' / 渠道 '+summaryCount(dims.channels)+' / 实际服务组 '+summaryCount(dims.groups)+' / 模型 '+summaryCount(dims.models)+'。各维度不能相加。</p>';
 const amounts=Object.entries(s.amounts||{});
 $('taAmounts').innerHTML=amounts.length?'<div class="ta-table"><table><thead><tr><th>独立核算方法</th><th>主体 / 单位</th><th>有依据的请求数</th><th>金额（最多六位小数）</th><th>精确额度 / 换算单位</th><th>明细</th></tr></thead><tbody>'+amounts.map(([key,v])=>'<tr><td>'+esc(methods[v.method]||v.method)+'</td><td>'+esc(v.subject)+' / '+esc(v.unit)+'</td><td>'+whole(v.count)+'</td><td title="'+esc(v.amount_fraction)+'">'+esc(fraction(v.amount_fraction))+'</td><td>'+esc(v.quota)+' / '+esc(v.quota_per_unit)+'</td><td><button type="button" data-ta-amount="'+esc(key)+'">查看组成请求</button></td></tr>').join('')+'</tbody></table></div>':'<p class="ta-warning">当前范围没有可汇总金额；缺价格、单位、参考或记录时，不补为 0。'+esc(emptyReason())+'</p>';
 $('taUnavailable').innerHTML='<p>候选核算覆盖：公式差额 '+summaryCount(a.calculable_candidates?.historical_formula_v1)+' / '+summaryCount(s.candidate_requests)+'；参考估计 '+summaryCount(a.calculable_candidates?.input_upper_reference_v1)+' / '+summaryCount(s.candidate_requests)+'</p>'+['historical_formula_v1','input_upper_reference_v1'].map(k=>'<button type="button" data-ta-unavailable="'+k+'">'+esc(methods[k])+'：查看不可核算请求</button>').join(' ')+'<ul>'+Object.entries(s.unavailable_by_method_and_reason||{}).map(([k,v])=>'<li>'+esc(k)+'：'+whole(v)+'</li>').join('')+'</ul>';
 const scope=state.query;
 $('taScope').textContent='汇总与列表使用同一筛选。'+(scope.user_id||scope.user_set?' 客户代号 #'+scope.user_id+'。':'')+(scope.group_set&&!scope.group?' 仅未记录服务分组。':'')+(scope.model_set&&!scope.model?' 仅未记录模型。':'')+(scope.channel_set&&!scope.channel_id?' 仅未记录渠道。':'')+(scope.subject?' 已固定金额主体 '+scope.subject+'、单位 '+scope.unit+'、方法 '+methods[scope.method]+'。':'')+(scope.unavailable?' 当前只看不可核算候选。':'')+tokenEntryObservation(r.entry_recent_observations);
 renderList();renderBreakdown();$('taNext').disabled=!r.has_more;$('taNext').textContent=r.has_more?'加载下一页':'当前筛选已无下一页';
}
function tokenEntryObservation(observations){
 if(!observations?.available)return ' 近期入口证据观测状态暂不可用。';
 const windows=observations.windows||[],through=Math.max(0,...windows.map(w=>Number(w.success_through)||0));
 const retry=windows.filter(w=>w.last_state==='deferred'||w.last_state==='source_failed').length;
 return ' 近期入口证据成功读取至 '+time(through)+'；'+retry+' 个小窗口等待重试。此为近期分窗观测，不代表历史连续覆盖。';
}
// 无数据时必须说清是哪一种原因,不能让空表被读成"已确认没有异常"。
// 判定顺序即排查顺序：采集 → 来源 → 检测 → 规则 → 窗口无结果。
// 全部取自已读到的 health / summary 字段,不新增后端接口。
function coverageStatus(){
 if(state.reportReadFailed||state.report?.available===false)return '本地检测结果读取失败，当前覆盖未知；不能据此确认无异常。';
 const f=state.health.facts,d=state.health.detection,s=state.report?.summary;
 if(state.health.failed.some(k=>k==='facts'||k==='detection'))return '采集与检测状态读取失败，当前无法判断原因；不能据此确认无异常。';
 // 开关字段缺失按未知处理,绝不当成"已启用"：否则接口少返回一个字段,
 // 页面就会把未知状态显示成"一切正常、确实没有异常",正是最危险的那种误读。
 if(!f||!d||typeof f.enabled!=='boolean'||typeof f.source_available!=='boolean'||typeof d.enabled!=='boolean')
  return '采集与检测状态不完整，当前无法判断原因；不能据此确认无异常。';
 if(f.available===false||d.available===false)
  return '健康状态不可用，当前无法确认检测覆盖；不能据此确认无异常。';
 if(f.enabled===false)return '采集未启用：仅展示已保存的历史结果，当前范围不能确认完整。';
 if(f.source_available===false)return '来源不可用或未接入：采集已配置，但来源连接不可用，本窗口事实可能缺失。';
 if(d.enabled===false)return '检测未启用：仅显示已保存的历史判断结果，当前窗口不会产生新结果。';
 if(d.policy_configured!==true)return '规则未配置或状态未知，无法确认检测结果。';
 if(s?.status_count_unavailable)return '状态汇总未就绪：本次计数预算不足；已读取明细仍可查看，不能用部分总量判断该窗口。';
 if(d.queue_ready===false)return '检测队列索引正在恢复，已提交水位可查，尚不能确认检测覆盖完整。';
 if(!s||s.source_complete!==true)return '来源覆盖未完整，正在采集或等待迟到数据；不能确认该窗口没有异常。';
 if(s.unprocessed_requests>0)return '检测中：存在尚未检测或等待修订的请求，数据可能不完整。';
 if(s.insufficient_requests>0)return '证据不足：部分请求不可判断，不能确认该窗口没有异常。';
 if(s.unprocessed_requests!==0||s.insufficient_requests!==0)return '检测覆盖未知：尚未取得完整处理证明。';
 if(s.truncated||(s.gaps||[]).length)return '覆盖不完整：'+(s.gaps||[]).map(reason).join('；');
 return '采集及检测覆盖完整。';
}
function emptyReason(){
 const status=coverageStatus(),q=state.query||{},p=state.report?.latest_progress;
 if(p&&status!=='采集及检测覆盖完整。'){if(!p.sample_count)return '所选范围尚未采集到请求；请核对采集状态，不能据此确认无异常。';const labels={...detectionStates,not_hit:'已检测未命中'};const text=Object.entries(p.states||{}).map(([k,v])=>(labels[k]||k)+' '+v+' 条').join('；');return '当前筛选没有匹配记录。最近 '+p.sample_count+' 条已采集请求：'+text+'。切换“全部结果”可核对明细；'+status}
 if(status!=='采集及检测覆盖完整。')return status;
 const narrowed=q.user_id||q.user_set||q.channel_id||q.channel_set||q.group||q.group_set||q.model||q.model_set||q.rule||q.evidence_level||q.method||q.subject||q.unavailable||(q.status&&q.status!=='candidate');
 return narrowed?'当前筛选没有匹配记录；这不代表所选时间内没有其他 Token 异常。':'当前所选时间内没有 Token 异常情况';
}
function tokenQuantities(d={}){
 const observed='日志输入 '+whole(d.recorded_input_tokens)+'；完整输入 '+whole(d.input_tokens)+'；输出 '+whole(d.output_tokens)+'；缓存读取 '+whole(d.cache_read_tokens)+'；缓存写入 '+whole(d.cache_write_tokens)+' Token';
 const comparable=d.comparison_kind==='statistical_reference'&&d.reference_input_tokens!=null&&d.input_difference_tokens!=null;
 return esc(observed)+'<br>来源记录总输入：'+whole(d.recorded_total_input_tokens)+' Token；日志扣费额度：'+whole(d.recorded_quota)+'；历史公式差额：'+whole(d.assessment?.formula_difference_quota)+'<br><small>'+(comparable?'输入参考上界 '+whole(d.reference_input_tokens)+' Token；差值 '+whole(d.input_difference_tokens)+' Token（统计参考差异，待核实）':'对照值 / 差值：不可计算（缺少可靠可比依据）')+'</small>';
}
function tokenUpstream(d){
 const label={local_current_snapshot:'当前渠道快照',local_last_snapshot:'删除前最后快照'}[d.upstream_source];
 if(!d.upstream||!label)return '未知';
 return esc(d.upstream)+'<br><small title="'+esc(d.upstream_observed_at?'快照时间：'+time(d.upstream_observed_at):'快照时间未知')+'">'+label+'；请求时归属未确认</small>';
}
function tokenRequestIdentity(d={}){
 const id=d.request_id||'missing';
 const source=d.source_log_id||'unknown';
 return '<small title="source log id">Request ID: '+esc(id)+'<br>source log: '+esc(source)+'</small>';
}
function tokenMissingFields(raw){
 if(raw==null||raw==='')return [{name:'missing_fields',reason:'not_recorded_in_fact'}];
 if(typeof raw==='string'){try{raw=JSON.parse(raw)}catch{return [{name:'missing_fields',reason:'stored_metadata_invalid'}]}}
 if(!Array.isArray(raw))return [{name:'missing_fields',reason:'stored_metadata_invalid'}];
 return raw.map(v=>typeof v==='string'?v:v&&typeof v==='object'&&typeof v.name==='string'?{name:v.name,reason:v.reason||'source_missing'}:{name:'missing_fields',reason:'stored_metadata_invalid'});
}
function tokenRequestSummary(d={},r={}){
 const missing=tokenMissingFields(d.missing_fields).map(v=>typeof v==='string'?v:v.name+': '+v.reason).join('；');
 const association=d.association_reason==='exact_request_evidence_not_found'?'精确请求证据未找到':d.association_reason||'待处理';
 const ruleReasons=(r.rules||[]).map(v=>(rules[v.name]||v.name)+'：'+(v.hit?'命中':v.eligible===false?'无法判断':'未命中')+'；'+tokenRuleExplanation(v)).join('；');
 return tokenRequestIdentity(d)+'<br><small>模型：'+esc(d.model||'未知')+'；路径：'+esc(d.request_path||'缺失')+'；请求大小：'+esc(d.request_length_bytes==null?'缺失':whole(d.request_length_bytes)+' 字节')+'<br>关联：'+esc((d.association_state||'waiting')+' / '+association)+'<br>缺失字段：'+esc(missing||'无已记录缺失项')+'</small><details><summary>字段来源与判断依据</summary><p>'+esc(Object.entries(d.field_sources||{}).map(([k,v])=>k+': '+v).join('；'))+'</p><p>'+esc(ruleReasons||'检测尚未完成')+'</p>'+tokenAcquisitionBody(d)+'<p>参考窗口：'+esc(d.reference_from?time(d.reference_from)+' — '+time(d.reference_through):'尚未形成')+'；样本数：'+esc(d.reference_samples??'未知')+'；'+esc(d.reference_reason||'')+'</p><p>计价快照：'+esc(d.pricing_snapshot||'缺失')+'</p></details>';
}
function tokenAcquisitionBody(d={}){
 const labels={source_not_recorded:'源日志未记录或不可用',source_replay_required:'待源日志重读',source_field_not_adapted:'已有字段待适配',source_value_unusable:'来源值不可用',raw_source_missing_contract_available:'原字段未记录，已由计量契约提供操作数',comparable_samples_unavailable:'可比参考不足',comparable_fields_missing:'可比字段未齐'};
 return (d.acquisition||[]).length?'<div class="ta-acquisition"><h4>取数与补取状态</h4><dl>'+d.acquisition.map(g=>'<dt>'+esc(g.name)+'</dt><dd>影响：'+esc((g.rule||'request_provenance').split(',').map(v=>rules[v]||v==='request_provenance'&&'请求来源核验'||v).join('、'))+'；来源：'+esc(g.source)+'；状态：'+esc(labels[g.status]||g.status)+'；'+esc(g.action||g.reason)+'</dd>').join('')+'</dl></div>':'';
}
function tokenRuleExplanation(v={}){
 const operands=Object.entries(v.operands||{}).map(([k,value])=>k+'='+value).join('；');
 const missing=(v.missing_fields||[]).map(x=>x.name+'（'+(x.source||'来源未记录')+'；'+(x.status||x.reason)+'；'+(x.action||'')+'）').join('，');
 return reason(v.reason||v.unsupported_reason||'')+(operands?'；依据：'+operands:'')+(missing?'；待补：'+missing:'');
}
function tokenDecisionLabel(d={},r={}){
 const label=assessmentLabels[d.assessment?.state]||detectionStates[d.detection_state]||statuses[resultStatus(r)];
 return d.assessment?.state==='local_formula_consistent'&&(r.rules||[]).some(v=>v.eligible===false)?label+'；其他规则仍待证据':label;
}
function tokenFactMetadata(h,u,d={}){
 const missing=tokenMissingFields(d.missing_fields??h.missing_fields).map(v=>typeof v==='string'?v:(v.name+':'+v.reason)).join(', ')||'无已记录缺失项';
 const entry=h.entry_event_id||'缺失',length=h.request_length??d.request_length_bytes;
 return '<section class="ta-fact-metadata"><h3>已采集请求证据</h3><dl>'+
  '<dt>Request ID</dt><dd>'+esc(h.request_id||'缺失')+'</dd>'+
  '<dt>来源 epoch / log id / 时间</dt><dd>'+esc((h.source_epoch||'缺失')+':'+(h.log_id||'缺失'))+' / '+esc(time(h.created_at))+'</dd>'+
  '<dt>客户 / 渠道 / 请求模型</dt><dd>'+esc((h.customer_name_snapshot||'客户名称未记录')+'（客户 #'+(h.user_id||'未知')+'） / 渠道 #'+(h.channel_id||'未知')+' / '+(h.model||'缺失'))+'</dd>'+
  '<dt>客户名称来源 / 快照时间</dt><dd>'+esc((h.customer_name_source||'缺失')+' / '+time(h.customer_name_observed_at))+'</dd>'+
  '<dt>请求方法 / 路径</dt><dd>'+esc((h.request_method||'缺失')+' / '+(h.request_path||'缺失'))+'</dd>'+
  '<dt>上游 Request ID / 模型</dt><dd>'+esc((h.upstream_request_id||'缺失')+' / '+(h.upstream_model_name||'缺失'))+'</dd>'+
  '<dt>入口事件 / 节点 / 路径 / 方法 / 状态</dt><dd>'+esc(entry+' / '+(h.entry_node||'缺失')+' / '+(h.entry_route||'缺失')+' / '+(h.entry_method||'缺失')+' / '+(h.entry_status||'缺失'))+'</dd>'+
  '<dt>请求大小 / 含义 / 来源版本 / 采集时间</dt><dd>'+esc(length==null?'缺失':whole(length)+' 字节')+'；'+esc(h.request_length_meaning||d.request_length_meaning||'含义缺失')+'；'+esc(h.request_length_version||d.request_length_source||'来源版本缺失')+'；'+esc(time(h.request_length_observed_at||d.request_length_observed_at))+'</dd>'+
  '<dt>计价快照 / 适配器 / 生效时间</dt><dd>'+esc(h.pricing_adapter||'缺失')+' / '+esc(time(h.pricing_effective_at))+'<pre>'+esc(h.pricing_snapshot||'缺失')+'</pre></dd>'+
  '<dt>请求时路由 / base path / 模型映射快照</dt><dd>'+esc((h.route_snapshot||'缺失')+' / '+(h.base_path_snapshot||'缺失')+' / '+(h.model_mapping_snapshot||'缺失'))+'</dd>'+
  '<dt>缺失字段</dt><dd>'+esc(missing)+'</dd>'+
  '<dt>关联 / 检测状态</dt><dd>'+esc((h.link_state||'pending')+' / '+(detectionStates[h.detection_state]||h.detection_state||'检测排队中'))+'</dd>'+
  '<dt>解析器 / 适配器版本</dt><dd>'+esc((h.parser_version||u?.version||d.parser_version||'缺失')+' / '+(u?.adapter||d.adapter_version||'缺失'))+'</dd>'+
  '<dt>用量字段来源</dt><dd>'+esc(Object.entries(u?.fields||{}).map(([k,v])=>k+'='+(v.kind||'')+':'+(v.state||'')).join(', ')||'见字段来源清单')+'</dd>'+
  '</dl>'+tokenAcquisitionBody(d)+'</section>';
}
function renderList(){
 $('taList').innerHTML=state.items.length?'<div class="ta-table"><table><thead><tr><th class="ta-select-column">选择</th><th>客户</th><th>上游</th><th>渠道</th><th>发生时间（北京时间）</th><th>异常类型</th><th>异常数量 / 差值</th><th>详情</th></tr></thead><tbody>'+state.items.map((item,i)=>{
 const h=item.request,r=item.result,d={...(item.display||{}),model:item.request.model},hits=(r.rules||[]).filter(x=>x.hit);
 const customerLabel=(d.customer_name||'unknown')+' | '+(d.request_id?'Request ID: '+d.request_id:'Request ID: missing');
 return '<tr><td class="ta-select-column"><input type="checkbox" data-ta-select="'+i+'" '+(!Number.isSafeInteger(h.log_id)?'disabled':'')+(state.selected.has(h.log_id)?' checked':'')+' aria-label="选择第 '+(i+1)+' 条冻结证据"></td><td>'+esc(customerLabel||'未知')+'<br>ID：'+(h.user_id>0?whole(h.user_id):'未知')+'</td><td>'+tokenUpstream(d)+'</td><td>'+esc(d.channel_name||'未知')+'<br>ID：'+(h.channel_id>0?whole(h.channel_id):'未知')+'</td><td>'+esc(time(h.created_at))+'</td><td>'+hits.map(x=>'<span class="ta-tag">'+esc(rules[x.name]||x.name)+'</span>').join('')+'<br>'+esc(tokenDecisionLabel(d,r))+'</td><td>'+tokenQuantities(d)+'<br>'+tokenRequestSummary(d,r)+'</td><td><button type="button" data-ta-detail="'+i+'">详情</button></td></tr>';
 }).join('')+'</tbody></table></div>':'<p class="ta-warning">'+esc(emptyReason())+'</p>';
}
function renderBreakdown(){
 const list=state.report?.analysis.breakdown||[],kind=$('taDimension').value,rows=list.map((v,i)=>({v,i})).filter(x=>x.v.kind===kind),shown=rows.slice(0,state.breakdownLimit);
 $('taBreakdown').innerHTML=shown.length?'<div class="ta-table"><table><thead><tr><th>维度</th><th>已保存请求</th><th>去重候选</th><th>明细</th></tr></thead><tbody>'+shown.map(({v,i})=>'<tr><td>'+esc(kind==='hour'?time(v.key):v.key||'未知')+'</td><td>'+whole(v.requests)+'</td><td>'+whole(v.candidate_requests)+'</td><td><button type="button" data-ta-dimension="'+i+'">按此项下钻</button></td></tr>').join('')+'</tbody></table></div>':'<p class="ta-warning">'+esc(emptyReason())+'</p>';
 $('taBreakdownMore').hidden=shown.length>=rows.length;
}
function closeDetail(){state.detailSeq++;state.detailAbort?.abort();state.detailAbort=null;if($('taDetail')?.open)$('taDetail').close()}
async function openDetail(item,revision){
 if(!Number.isSafeInteger(item.request.log_id)){showError('消费编号超出页面整数精度，已停止操作；请通过受控本地接口核对。');return}
 state.detailAbort?.abort();const controller=new AbortController(),seq=++state.detailSeq;state.detailAbort=controller;state.detailItem=item;
 $('taDetailBody').textContent='正在读取冻结证据…';$('taRevision').value=revision||item.request.revision;$('taRevision').max=item.request.revision;
 if(!$('taDetail').open)$('taDetail').showModal();
 try{const r=await api('detection/detail',{source_epoch:item.request.source_epoch,log_id:item.request.log_id,revision:revision||item.request.revision},controller);if(seq!==state.detailSeq||!state.active)return;if(r.pending&&!r.evidence){renderPendingDetail(r);return}if(!r.evidence)throw Error('冻结证据结构不完整');renderDetail(r)}
 catch(e){if(seq!==state.detailSeq||e?.name==='AbortError')return;if(e.status===401){location.href='/login';return}$('taDetailBody').textContent=e.message||'冻结证据不可用'}
 finally{if(seq===state.detailSeq)state.detailAbort=null}
}
function renderPendingDetail(d){
 const h=d.request||{},missing=tokenMissingFields(d.missing_fields).map(v=>typeof v==='string'?v:(v.name+':'+v.reason)).join(', ')||'none';
 $('taDetailBody').innerHTML='<section class="ta-warning"><h3>'+esc(detectionStates[d.detection_state]||d.detection_state||'处理排队中')+'</h3><p>检测尚未完成。以下为已采集的请求事实。</p>'+tokenQuantities(d.display||{})+tokenFactMetadata({...h,missing_fields:d.missing_fields,detection_state:d.detection_state},null,d.display||{})+'<p>关联原因：'+esc(d.association_reason==='exact_request_evidence_not_found'?'精确请求证据未找到':d.association_reason||h.link_reason||'待处理')+'</p><p>字段来源：'+esc(Object.entries(d.display?.field_sources||{}).map(([k,v])=>k+': '+v).join('；'))+'</p><p>规则、参考窗口与样本数：检测尚未完成，尚无本次判断依据。</p></section>';
}
function renderAssessment(a){
 if(!a)return '';
 const row=(label,value)=>'<dt>'+esc(label)+'</dt><dd>'+esc(value??'不可计算')+'</dd>';
 const missingLabel=value=>{
  const [key,...tail]=String(value).split(':'),labels={request_length:'整包请求字节',unique_entry_association:'唯一入口关联',model:'模型',request_path:'请求路径',entry_route_status_or_length_version:'入口路径、状态或长度版本',input_tokens:'输入 Token 语义',completion_tokens:'输出 Token',quota:'实际 quota',pricing_snapshot:'计价快照',saved_formula_not_reproduced:'冻结公式尚不能复算一致',statistical_reference:'统计参考',source_window_incomplete:'来源窗口尚未完整'};
  return (labels[key]||key)+(tail.length?'：'+reason(tail.join(':')):'');
 };
 const range=a.normal_quota_from!=null&&a.normal_quota_to!=null?a.normal_quota_from+' — '+a.normal_quota_to+' quota（按日志输入和冻结计价快照复算）':'不可计算';
 return '<section class="ta-assessment"><h3>'+esc(assessmentLabels[a.state]||'证据状态未知')+'</h3><p>上游真实 Token 与扣账未确认。本地计量差异仅核实日志金额与历史公式之间的差异。</p><dl>'+row('证据等级',levels[a.evidence_level]||a.evidence_level)+row('模型 / 路径',(a.model||'缺少模型')+' / '+(a.request_path||'缺少路径'))+row('输入证据 / 输出 Token',whole(a.input_tokens)+' / '+whole(a.output_tokens))+row('整包 HTTP 请求大小',a.request_length_bytes==null?'缺失':whole(a.request_length_bytes)+' 字节；不是正文大小或输入 Token')+row('本地公式正常范围',range)+row('实际 quota',a.actual_quota)+row('日志计量差额',a.formula_difference_quota==null?'不可计算':a.actual_quota+' − '+a.normal_quota_from+' = '+a.formula_difference_quota)+row('统计输入参考上界',a.reference_input_upper==null?'不可计算':whole(a.reference_input_upper)+' Token；不是真实 Token 上界')+row('输入与统计参考差值',a.reference_input_difference)+row('具体证据缺口',(a.missing_fields||[]).map(missingLabel).join('；')||'本地公式核验所需字段完整；上游扣账证据仍缺失')+'</dl></section>';
}
function renderDetail(d){
 const f=d.evidence,r=f.result,u=f.input.usage,h={...f.input.fact,detection_state:d.assessment?.state||f.input.fact.detection_state||(r.candidate?'candidate':resultStatus(r)==='insufficient'?'evidence_insufficient':'not_hit')},ref=r.reference,price=r.price;
 const row=(a,b)=>'<dt>'+esc(a)+'</dt><dd>'+esc(b==null?'不可计算':b)+'</dd>';
 const fields=Object.entries(u.fields||{});
 const factMetadata=tokenFactMetadata(h,u,d.display||{});
 const stateLabel=tokenDecisionLabel({...d,detection_state:h.detection_state},r);
 $('taDetailBody').innerHTML=factMetadata+renderAssessment(d.assessment)+'<p class="ta-warning">'+esc(stateLabel)+'。exact 只说明所述日志事实或公式复算，不认证上游 Token 真值或钱包多扣。</p><dl>'+row('冻结时间 / 修订',time(d.recorded_at)+' / '+d.revision)+row('原判定时间',time(h.created_at))+row('客户 / 渠道','#'+h.user_id+' / #'+h.channel_id)+row('实际服务组 / 模型',h.user_group+' / '+h.model)+row('计量语义',u.semantic+' / '+u.semantic_reason)+row('完整输入 / 输出',whole(u.total_input)+' / '+whole(u.fields?.completion_tokens?.value))+row('整包请求大小',h.request_length==null?'缺失':whole(h.request_length)+' 字节；不是纯正文大小')+row('关联状态',h.link_state+' / '+h.link_reason+' / '+h.evidence_level)+row('检测状态',h.detection_state||'pending')+row('来源与解析版本',u.source+' / '+u.adapter+' / '+u.version)+'</dl><h3>为什么命中，哪些不可判断</h3><div class="ta-table"><table><thead><tr><th>规则</th><th>结果</th><th>证据等级</th><th>原因</th></tr></thead><tbody>'+(r.rules||[]).map(v=>'<tr><td>'+esc(rules[v.name]||v.name)+'</td><td>'+(v.hit?'命中候选':v.eligible?'可用规则未命中':'不可判断')+'</td><td>'+esc(levels[v.evidence_level]||v.evidence_level)+'</td><td>'+esc(tokenRuleExplanation(v))+'</td></tr>').join('')+'</tbody></table></div><h3>冻结参考</h3><dl>'+row('状态 / 原因',ref.status+' / '+reason(ref.reason))+row('参考窗口',time(ref.from)+' — '+time(ref.through))+row('有效样本 / 参考上界',ref.sample_count+' / '+whole(ref.upper))+row('缺少的可比字段',(ref.missing_comparable_fields||[]).join('、')||'无')+row('分阶段检索',(ref.search||[]).map(x=>x.tolerance_bytes+' 字节：读取 '+x.read+'，合格 '+x.eligible+'，渠道 '+x.channels).join('；')||'未检索')+row('样本排除原因',Object.entries(ref.exclusion_reasons||{}).map(([k,n])=>reason(k)+'：'+n).join('；')||'无')+row('语义限制','相同大小不证明相同正文；多个渠道不证明独立供应商。')+'</dl><details><summary>参考样本与版本</summary><pre>'+esc(JSON.stringify(ref,null,2))+'</pre></details><h3>金额与计算依据</h3><dl>'+row('主体 / 单位',price.subject+' / '+price.unit+'；每单位额度 '+price.quota_per_unit)+row('涉及消费（不是损失）',r.actual_quota)+row('按历史公式复算额度',price.supported?price.quota:'不可计算：'+reason(price.reason))+row('日志费用差额',r.formula_difference_quota==null?'不可计算：'+reason(price.reason):r.actual_quota+' − '+price.quota+' = '+r.formula_difference_quota)+row('参考估计损失',r.estimated_loss_quota==null?'不可计算：'+reason(r.estimate_reason):'max(0, '+r.actual_quota+' − '+r.reference_price?.quota+') = '+r.estimated_loss_quota)+row('估计假设',r.estimate_reason)+row('动态计价',price.tier?'当前档位 '+price.tier+'；参考档位 '+(r.reference_price?.tier||'不可计算'):'见冻结价格支持范围；缺条件不套普通公式')+'</dl><p>OpenAI 输入已含缓存读取子量，Anthropic 原生输入按已确认分类合并，不重复加缓存。金额按冻结计价版本计算；动态表达式遵循来源的浮点运算和舍入，直接差额与参考估计分别列示。</p><details><summary>原始白名单值、存在性与冻结操作数</summary><div class="ta-table"><table><thead><tr><th>字段</th><th>状态</th><th>值</th><th>来源</th></tr></thead><tbody>'+fields.map(([k,v])=>'<tr><td>'+esc(k)+'</td><td>'+esc(fieldStates[v.state]||v.state)+'</td><td>'+esc(v.state==='valid'?v.value:'不可用')+'</td><td>'+esc(v.source||u.source)+'</td></tr>').join('')+'</tbody></table></div>'+contractOperandsBody(u)+'<pre>'+esc(JSON.stringify({policy:f.policy,price:r.price,reference_price:r.reference_price,entry:f.input.entry,hmac_refs:f.input.hmac_refs,decision_hash:d.decision_hash},null,2))+'</pre></details><h3>仍无法确认</h3><p>'+esc((r.gaps||[]).concat(ref.gaps||[]).map(reason).join('；')||'当前规则没有额外字段缺口，仍不代表确认上游多扣。')+'</p>';
}

function renderAccess(){
 const a=state.access,old=$('taTarget').value;
 $('taTarget').innerHTML='<option value="">选择已授权供应商范围</option>'+(a.export_targets||[]).map(t=>'<option value="'+esc(t.id)+'">'+esc(t.id)+' · 渠道 '+esc(t.channel_ids.join(', '))+'</option>').join('');
 if((a.export_targets||[]).some(t=>t.id===old))$('taTarget').value=old;syncExport();
}
function invalidateExport(){state.exportSeq++;state.exportAbort?.abort();state.exportAbort=null;state.receipt=null;state.exportRequest=null;$('taExportResult').textContent=''}
function syncExport(){
 if(!$('taCreateExport'))return;
 const a=state.access,t=a?.export_targets?.find(v=>v.id===$('taTarget').value),count=state.selected.size,busy=!!state.exportAbort||!!state.abort;
 $('taCustomerPrices').disabled=!t?.allow_customer_pricing;if(!t?.allow_customer_pricing)$('taCustomerPrices').checked=false;
 $('taCreateExport').disabled=busy||!a?.is_root||!a?.export_enabled||!a?.export_configured||!t||!count||count>(a?.max_requests||100)||!$('taExportConfirm').checked;
 $('taDownloadExport').disabled=busy||!state.receipt;
 $('taExportNote').textContent=(!a?'权限尚未读取':!a.is_root?'只有超级管理员可生成和下载':!a.export_enabled?'导出开关关闭':!a.export_configured?'供应商导出范围尚未配置':'仅生成本地脱敏包，不自动外发')+'。已选择 '+count+' 条冻结修订；每包最多 '+(a?.max_requests||100)+' 条，保留一小时。'+(t?'授权时段 '+time(t.from)+' — '+time(t.to)+'。':'');
}
async function createExport(){
 if($('taCreateExport').disabled)return;
 const q={target_id:$('taTarget').value,source_epoch:state.query.source_epoch,from:state.query.from,to:state.query.to,include_customer_pricing:$('taCustomerPrices').checked,selection:[...state.selected.values()]};
 const signature=JSON.stringify(q);
 if(!state.exportRequest||state.exportRequest.signature!==signature)state.exportRequest={signature,body:{...q,request_id:crypto.randomUUID()}};
 const controller=new AbortController(),seq=++state.exportSeq;state.exportAbort=controller;syncExport();$('taExportResult').textContent='正在生成受控本地证据包…';
 try{const r=await api('exports',state.exportRequest.body,controller,state.access.csrf_token);if(seq!==state.exportSeq||!state.active)return;state.receipt=r;$('taExportResult').textContent='已生成 '+r.count+' 条，'+r.bytes+' 字节；到期 '+time(r.expires_at)+'；SHA256 '+r.sha256+'。请核对后下载。'}
 catch(e){if(seq===state.exportSeq&&e.name!=='AbortError')$('taExportResult').textContent=e.message||'导出失败'}
 finally{if(seq===state.exportSeq){state.exportAbort=null;syncExport()}}
}
async function downloadExport(){
 if($('taDownloadExport').disabled)return;
 const controller=new AbortController(),seq=++state.exportSeq;state.exportAbort=controller;syncExport();
 try{
  const r=await fetch('/anomalies/token/exports/'+state.receipt.id+'/download',{method:'POST',headers:{Accept:'application/zip','Content-Type':'application/json','X-Monitor-Token-CSRF':state.access.csrf_token},body:'{}',cache:'no-store',credentials:'same-origin',signal:controller.signal});
  if(!r.ok){const j=await r.json();throw Error(j.error||'下载失败')}
  const blob=await r.blob();if(seq!==state.exportSeq||!state.active)return;
  if(blob.size>state.access.max_bytes||!r.headers.get('Content-Type')?.includes('application/zip'))throw Error('下载包格式或体积不符合限制');
  const href=URL.createObjectURL(blob),link=document.createElement('a');link.href=href;link.download='token-evidence-'+state.receipt.id.slice(0,16)+'.zip';link.click();setTimeout(()=>URL.revokeObjectURL(href),1000);$('taExportResult').textContent='脱敏包已下载；请按授权范围人工核对后使用。';
 }catch(e){if(seq===state.exportSeq&&e.name!=='AbortError')$('taExportResult').textContent=e.message||'下载失败'}
 finally{if(seq===state.exportSeq){state.exportAbort=null;syncExport()}}
}
window.TokenAnomalyFormatting={whole,fraction,parseTime,resultStatus};
})();
