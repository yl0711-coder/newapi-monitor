(function(){
'use strict';
const $=id=>document.getElementById(id),esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const state={inited:false,active:false,seq:0,abort:null,detailSeq:0,detailAbort:null,healthSeq:0,healthAbort:null,timer:null,query:null,live:true,rows:[],report:null,paged:false,detail:null};
const evidencePages={observations:'imObsMore',segments:'imSegMore',impacts:'imImpactMore',impacts_archived:'imArchiveMore',revisions:'imRevisionMore'};
const time=v=>Number(v)>0?new Date(Number(v)*1000).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}):'未知';
const number=v=>v===null||v===undefined?'未知':Number.isSafeInteger(v)?v.toLocaleString('zh-CN'):'不可计算';
const inputTime=v=>new Date(v*1000+8*3600000).toISOString().slice(0,16);
const parseTime=v=>{if(!/^\d{4}-\d\d-\d\dT\d\d:\d\d$/.test(v))return NaN;const t=Date.parse(v+':00+08:00')/1000;return Number.isFinite(t)&&inputTime(t)===v?t:NaN};
const labels={open:'仍在观测',resolved:'观测段已结束（不等于恢复确认）',exact:'直接日志事实',correlated:'相关性证据',inferred:'统计推断',unavailable:'证据不可用',unknown:'未知',upstream:'上游',downstream:'下游',ours:'我方',channel:'渠道',model:'模型',user_group:'服务分组',ingress_protocol:'入口协议',node:'节点',user:'客户',transport_connect:'连接失败',transport_timeout:'连接超时',upstream_first_byte_slow:'上游首字节慢',stream_first_event_slow:'流式首事件慢',text_first_content_slow:'正文首内容慢',stream_interrupted_before_first_event:'首事件前中断',stream_interrupted_midstream:'流式途中中断',client_gone:'客户端断连',protocol_invalid:'协议异常',responses_incomplete:'响应不完整',thinking_signature_invalid:'思考签名异常',tool_call_invalid:'工具调用异常',route_no_channel:'无可用渠道',model_not_found:'模型不存在',auth_quota_account:'鉴权、额度或账户异常',rate_limit_capacity:'限流或容量不足',upstream_5xx:'上游服务错误',empty_or_truncated_output:'输出为空或截断',billing_anomaly:'计费异常',semantic_quality:'语义质量异常',client_retry_loop:'客户端反复重试',client_compaction:'客户端压缩',telemetry_gap:'采集缺口',site_wide_or_security:'全站或安全事故',primary_no_fallback:'主渠道故障且无备用',degraded_with_fallback:'有备用的服务降级',isolated_or_drift:'局部异常或漂移',no_fallback_config_fact:'缺少备用渠道配置事实',gap_backfilled_anomalous:'缺口补齐后发现异常'};
const label=v=>labels[v]||v||'未知';
function preserve(fn){return window.MonitorView?window.MonitorView.preserve($('incidentView'),fn):fn()}
let tabsInited=false;
window.anomalyPageActivate=function(){
 if(!tabsInited){tabsInited=true;document.querySelectorAll('[data-anomaly-view]').forEach(b=>{
  b.addEventListener('click',()=>window.monitorNavigate('anomalies',{view:b.dataset.anomalyView}));
  b.addEventListener('keydown',e=>{if(!['ArrowLeft','ArrowRight','Home','End'].includes(e.key))return;e.preventDefault();const target=e.key==='Home'?'token':e.key==='End'?'incidents':b.dataset.anomalyView==='token'?'incidents':'token';document.querySelector('[data-anomaly-view="'+target+'"]').focus();window.monitorNavigate('anomalies',{view:target})});
 })}
 const incident=window.monitorNavigationContext?.().view==='incidents';
 $('tokenAnomalyView').hidden=incident;$('incidentView').hidden=!incident;
 document.querySelectorAll('[data-anomaly-view]').forEach(b=>{const on=(b.dataset.anomalyView==='incidents')===incident;b.classList.toggle('active',on);b.setAttribute('aria-selected',String(on));b.tabIndex=on?0:-1});
 if(incident){window.tokenAnomalyDeactivate?.();activate()}else{deactivate();window.tokenAnomalyActivate?.()}
};
window.anomalyPageDeactivate=function(){deactivate();window.tokenAnomalyDeactivate?.()};
function defaultRange(){const to=Math.floor(Date.now()/60000)*60;return {from:to-86400,to}}
function sync(){for(const key of ['from','to'])$('im'+(key==='from'?'From':'To')).value=inputTime(state.query[key])}
function init(){
 state.inited=true;state.query=defaultRange();sync();
 $('imFilters').addEventListener('submit',e=>{e.preventDefault();const from=parseTime($('imFrom').value),to=parseTime($('imTo').value);if(!Number.isFinite(from)||!Number.isFinite(to)||from<=0||to<=from||to-from>90*86400){error('请选择有效的北京时间范围，结束晚于开始，最多 90 天。');return}state.query={from,to};state.live=false;state.paged=false;load()});
 $('imReset').addEventListener('click',()=>{state.query=defaultRange();state.live=true;state.paged=false;sync();load()});
 $('imRefresh').addEventListener('click',()=>{state.paged=false;load()});
 $('imNext').addEventListener('click',()=>load(true));
 $('imList').addEventListener('click',e=>{const button=e.target.closest('[data-incident-id]');if(button){const row=state.rows.find(r=>r.id===button.dataset.incidentId);if(row)openDetail(row)}});
 $('imDetailClose').addEventListener('click',closeDetail);
 $('imDetail').addEventListener('close',()=>{state.detailSeq++;state.detailAbort?.abort();state.detailAbort=null;schedule()});
 for(const [key,id]of Object.entries(evidencePages))$(id).addEventListener('click',()=>openDetail(state.detail.incident,key));
 $('imHealth').addEventListener('toggle',()=>{if($('imHealth').open)loadHealth()});
 window.addEventListener('focus',refreshVisible);
 document.addEventListener('visibilitychange',()=>{if(document.hidden){clearTimeout(state.timer);state.timer=null}else refreshVisible()});
}
function activate(){if(!state.inited)init();state.active=true;if(!state.paged&&!$('imDetail').open)load();schedule()}
function deactivate(){state.active=false;state.seq++;state.abort?.abort();state.abort=null;state.healthSeq++;state.healthAbort?.abort();state.healthAbort=null;clearTimeout(state.timer);state.timer=null;closeDetail()}
function refreshVisible(){if(!state.active||document.hidden)return;if(!state.paged&&!$('imDetail').open)load();schedule()}
function schedule(){clearTimeout(state.timer);state.timer=null;if(!state.active||document.hidden)return;state.timer=setTimeout(()=>{refreshVisible()},30000)}
function error(message){$('imError').hidden=false;$('imError').textContent=message}
async function api(path,controller){const r=await fetch(path,{headers:{Accept:'application/json'},cache:'no-store',signal:controller?.signal});if(r.status===401){location.href='/login';throw Error('请重新登录')}const data=await r.json();if(!r.ok){const e=Error(data.error||(r.status===403?'没有事故监控查看权限':'事故查询失败'));e.status=r.status;throw e}return data}
async function load(more=false){
 if(!state.active||more&&(!state.report?.has_more||state.abort))return;
 if(!more&&state.live){const draft=$('imFrom').value!==inputTime(state.query.from)||$('imTo').value!==inputTime(state.query.to);state.query=defaultRange();if(!draft)sync()}
 state.abort?.abort();const ac=new AbortController(),seq=++state.seq;state.abort=ac;
 $('imError').hidden=true;$('imList').setAttribute('aria-busy','true');$('imNext').disabled=true;
 $('imUpdated').textContent=state.report?'正在更新，保留当前结果…':'正在读取本地事故事实…';
 const q=new URLSearchParams({...state.query,limit:50,consistent:1});
 if(more){q.set('cursor',state.report.next_cursor);q.set('snapshot',state.report.snapshot)}
 try{const r=await api('/incidents?'+q,ac);if(seq!==state.seq)return;
  if(r.enabled!==false&&(!Array.isArray(r.incidents)||!r.coverage))throw Error('事故响应不完整，覆盖未知');
  state.rows=more?state.rows.concat(r.incidents||[]):r.incidents||[];state.report=r;if(more)state.paged=true;
  preserve(render);
 }catch(e){if(seq!==state.seq||e.name==='AbortError')return;error(e.message);$('imUpdated').textContent='查询失败，保留上次结果；无法据此判断没有事故';if(e.status===403){state.rows=[];state.report=null}if(!state.rows.length){$('imList').textContent='本次查询不可用，覆盖未知。';$('imCoverage').hidden=true}if(e.status===409)state.paged=true}
 finally{if(seq===state.seq){state.abort=null;$('imList').setAttribute('aria-busy','false');$('imNext').disabled=!state.report?.has_more;$('imRefresh').hidden=!state.paged;schedule()}}
}
function coverageText(r){
 if(r.enabled===false)return r.note||'事故监控未启用，尚不能判断所选时间内的事故。';
 const c=r.coverage||{};
 if(c.status==='disabled')return '事故采集未启用，仅展示已保存的历史事故；所选范围不能确认完整。';
 if(c.complete===true)return '所选范围的采集及处理覆盖完整。';
 if(c.status==='finalizing')return '所选范围已封口部分的采集及处理覆盖完整，尾部正在正常等待封口；尚不能确认整个所选范围。';
 return '覆盖不足或尚未处理，不能据此确认没有事故。'+(c.lag_seconds>0?' 当前处理进度落后目标 '+number(c.lag_seconds)+' 秒。':'');
}
function render(){
 const r=state.report,c=r.coverage||{};
 $('imCoverage').textContent=coverageText(r);
 $('imUpdated').textContent='所选范围完整处理截止 '+time(c.actual_through_ts)+' · 响应生成于 '+time(r.generated_at)+'（北京时间）'+(state.paged?' · 正在浏览分页，自动刷新暂停':state.live?' · 近 24 小时随时间更新':' · 保持所选时间范围');
 $('imRange').textContent='查询范围 '+time(r.from_ts||state.query.from)+' — '+time(r.to_ts||state.query.to)+'（左闭右开）；列出与范围相交的事故，按最近观测时间降序、事故 ID 升序排列。'+(c.finalize_lag_seconds!=null?' 封口等待 '+number(c.finalize_lag_seconds)+' 秒，另按事故窗口对齐；扫描到 '+time(c.scan_through_ts)+'，不代表处理完成。':'')+' 全局历史完整处理截止 '+time(c.global_complete_through_ts)+'；各事故自身的证据状态见详情。';
 $('imList').innerHTML=state.rows.length?'<div class="im-table"><table><thead><tr><th>事故 / 类型</th><th>状态 / 严重度</th><th>影响范围</th><th>首次 / 最近观测（北京时间）</th><th>详情与证据</th></tr></thead><tbody>'+state.rows.map(r=>'<tr data-incident-id="'+esc(r.id)+'"><td>'+esc(r.id)+'<br>'+esc(label(r.primary_fault_class))+'</td><td>'+esc(label(r.status))+'<br>'+esc(r.severity||'尚未定级')+(r.scenario_pending===true?'<br>场景待确认':r.scenario_pending==null?'<br>待确认状态未知':'')+'</td><td>'+esc(r.channel_scope_label||'渠道未知')+(r.channel_id!=null?' #'+esc(r.channel_id):'')+'<br>模型：'+esc(r.model||'未知')+'<br>分组：'+esc(r.user_group||'未知')+'</td><td>'+esc(time(r.first_seen_ts))+'<br>'+esc(time(r.last_seen_ts))+'</td><td><button type="button" data-incident-id="'+esc(r.id)+'">详情与证据</button></td></tr>').join('')+'</tbody></table></div>':'<p class="im-empty">'+esc(r.enabled!==false&&c.complete===true?'当前所选时间内没有已记录事故。':coverageText(r))+'</p>';
 $('imCoverage').hidden=!state.rows.length;$('imNext').hidden=!r.has_more;
}
function closeDetail(){state.detailSeq++;state.detailAbort?.abort();state.detailAbort=null;if($('imDetail')?.open)$('imDetail').close()}
async function openDetail(row,more=''){
 if(!row||!state.active)return;
 state.detailAbort?.abort();const ac=new AbortController(),seq=++state.detailSeq;state.detailAbort=ac;
 for(const id of Object.values(evidencePages))$(id).disabled=true;
 $('imDetailError').hidden=true;
 const q=new URLSearchParams({id:row.id,limit:50,consistent:1});if(row.revision!=null)q.set('revision',row.revision);
 if(row.row_snapshot)q.set('row_snapshot',row.row_snapshot);
 if(more&&state.detail.evidence_snapshot)q.set('evidence_snapshot',state.detail.evidence_snapshot);
 if(more==='observations')q.set('observations_cursor',state.detail.observations_next_cursor_key);
 else if(more)q.set(more+'_cursor',state.detail[more+'_next_cursor']);
 if(!more){state.detail=null;$('imDetailBody').textContent='正在读取事故及证据…';for(const id of Object.values(evidencePages))$(id).hidden=true}
 if(!$('imDetail').open)$('imDetail').showModal();
 try{const r=await api('/incidents/detail?'+q,ac);if(seq!==state.detailSeq||!state.active)return;
  if(!r.incident||r.incident.id!==row.id)throw Error('事故详情不可用');
  if(more){const old=state.detail;for(const key of Object.keys(evidencePages))if(more===key)r[key]=(old[key]||[]).concat(r[key]||[]);else{r[key]=old[key];for(const suffix of ['has_more','next_cursor','next_cursor_key','truncated'])r[key+'_'+suffix]=old[key+'_'+suffix]}}
  state.detail=r;preserve(()=>renderDetail(r));
 }catch(e){if(seq!==state.detailSeq||e.name==='AbortError')return;$('imDetailError').hidden=false;$('imDetailError').textContent=e.message}
 finally{if(seq===state.detailSeq){state.detailAbort=null;for(const id of Object.values(evidencePages))$(id).disabled=false}}
}
function table(head,rows){return '<div class="im-table"><table><thead><tr>'+head.map(v=>'<th>'+esc(v)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(cells=>'<tr>'+cells.map(v=>'<td>'+esc(v)+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>'}
function renderDetail(d){
 const r=d.incident;
 $('imDetailBody').innerHTML='<p>'+esc(r.id)+' · '+esc(label(r.status))+' · '+esc(r.severity||'尚未定级')+'</p><p>场景：'+esc(label(r.scenario))+'；'+esc(r.scenario_pending===true?label(r.pending_reason):r.scenario_pending==null?'待确认状态未知':'无待确认标记')+'；证据：'+esc(label(r.evidence_level))+'；责任方：'+esc(label(r.responsibility))+'</p><p class="im-note">'+esc(d.anomaly_rate_caveat)+'</p><h4>影响范围</h4>'+table(['维度 / 值','时间范围','样本 / 独立请求 / 可见尝试 / 日志行','覆盖'],(d.impacts||[]).map(x=>[label(x.Dimension)+' / '+x.DimValue,time(x.WindowFromTs)+' — '+time(x.WindowToTs),[x.SampleTotal,x.DistinctRequests,x.VisibleAttempts,x.LogRows].map(number).join(' / '),x.CoverageComplete&&!x.Partial?'已核验':'覆盖不完整']))+'<p>缺少事实的影响维度：'+esc((d.impact_dimension_gaps||[]).join('；'))+'</p><h4>异常分段</h4>'+table(['段号','开始 / 结束','严重度','本段依据'],(d.segments||[]).map(x=>[number(x.SeqNo),time(x.StartTs)+' — '+(x.EndTs?time(x.EndTs):'尚未结束'),x.Severity||'尚未定级','连续 '+number(x.ConsecutiveWindows)+' 窗；硬信号 '+(x.HardSignal?'有':'无')+'；多源确认 '+(x.MultiSourceConfirmed?'是':'否')]))+'<h4>观测证据</h4>'+table(['窗口（左闭右开）','事实状态','来源 / 证据等级','样本 / 独立请求 / 尝试 / 日志','证据说明'],(d.observations||[]).map(x=>[time(x.WindowFromTs)+' — '+time(x.WindowToTs),!x.DataPresent?'数据缺失':x.Anomalous?'异常观测':'正常观测',x.SourceIDs+' / '+label(x.EvidenceLevel),[x.SampleTotal,x.DistinctRequests,x.VisibleAttempts,x.LogRows].map(number).join(' / '),x.Note||'无补充说明']))+'<details><summary>迟到修订与归档证据</summary><p>原判断保留；修订展示最新实测，不能替换原判定依据。归档仅保留区间摘要。</p><pre>'+esc(JSON.stringify({revisions:d.revisions,archived:d.impacts_archived},null,2))+'</pre></details>';
 for(const [key,id]of Object.entries(evidencePages))$(id).hidden=!d[key+'_has_more'];
 if(Object.keys(evidencePages).some(key=>d[key+'_has_more']))$('imDetailBody').insertAdjacentHTML('beforeend','<p class="im-note">当前仅展示部分证据，可用下方按钮继续读取同一快照。分页数量不代表事故总影响。</p>');
}
async function loadHealth(){
 state.healthAbort?.abort();const ac=new AbortController(),seq=++state.healthSeq;state.healthAbort=ac;
 try{const h=await api('/incidents/health',ac);if(!state.active||seq!==state.healthSeq)return;$('imHealthBody').textContent=JSON.stringify(h,null,2)}catch(e){if(state.active&&seq===state.healthSeq&&e.name!=='AbortError')$('imHealthBody').textContent=e.message}finally{if(seq===state.healthSeq)state.healthAbort=null}
}
})();
