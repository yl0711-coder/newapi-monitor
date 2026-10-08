(function(){
'use strict';

const state={inited:false,loading:false,window:'24h',generation:0,abort:null,report:null,expanded:new Set(),expandedGroups:new Set()};
const $=id=>document.getElementById(id);
const esc=value=>String(value==null?'':value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=value=>(Number(value)||0).toLocaleString('zh-CN');
const pct=(value,total)=>total>0?((value*100)/total).toFixed(1)+'%':'—';
const WINDOW_LABEL={'24h':'近24小时','3d':'近3天','7d':'近7天'};

window.modelStatisticsActivate=function(){
  if(!state.inited)init();
  load(state.window);
};
window.modelStatisticsDeactivate=function(){
  if(!state.inited)return;
  state.generation++;
  state.abort?.abort();
  state.abort=null;
  state.loading=false;
};

function init(){
  state.inited=true;
  document.querySelectorAll('[data-ms-window]').forEach(button=>button.addEventListener('click',()=>{
    const key=button.getAttribute('data-ms-window');
    if(WINDOW_LABEL[key])load(key);
  }));
  $('msModels')?.addEventListener('click',event=>{
    const groupButton=event.target.closest('[data-ms-group-key]');
    if(groupButton){
      const key=groupButton.getAttribute('data-ms-group-key');
      if(state.expandedGroups.has(key))state.expandedGroups.delete(key);else state.expandedGroups.add(key);
      if(state.report)render(state.report);
      return;
    }
    const button=event.target.closest('[data-ms-expand]');
    if(!button)return;
    const model=button.getAttribute('data-ms-expand');
    if(state.expanded.has(model))state.expanded.delete(model);else state.expanded.add(model);
    if(state.report)render(state.report);
  });
}

async function load(windowKey){
  if(!WINDOW_LABEL[windowKey])return;
  state.window=windowKey;
  state.abort?.abort();
  const controller=new AbortController();
  state.abort=controller;
  const generation=++state.generation;
  state.loading=true;
  setActiveWindow();
  setStatus('正在读取本地模型事实…','');
  try{
    const response=await fetch('/model-statistics/report?window='+encodeURIComponent(windowKey),{
      headers:{'Accept':'application/json'},cache:'no-store',signal:controller.signal
    });
    if(response.status===401){location.href='/login';return}
    const body=await response.text();
    let data={};try{data=body?JSON.parse(body):{}}catch(_){throw new Error('接口返回了无效 JSON')}
    if(!response.ok)throw new Error(data.error||'模型统计读取失败');
    if(generation!==state.generation)return;
    state.report=data;
    render(data);
    setStatus('已更新 '+new Date(Number(data.generated_at||0)*1000).toLocaleString('zh-CN',{hour12:false}),'ok');
  }catch(error){
    if(error?.name==='AbortError')return;
    if(generation!==state.generation)return;
    state.report=null;
    renderError(error?.message||'模型统计读取失败');
  }finally{
    if(generation===state.generation){state.loading=false;state.abort=null}
  }
}

function setActiveWindow(){
  document.querySelectorAll('[data-ms-window]').forEach(button=>button.classList.toggle('active',button.getAttribute('data-ms-window')===state.window));
}
function setStatus(text,kind){
  const target=$('msStatus');if(!target)return;
  target.textContent=text;target.className='ms-status '+(kind||'');
}
function renderError(message){
  const error=$('msError');if(error){error.hidden=false;error.textContent=message}
  const summary=$('msSummary');if(summary)summary.innerHTML='';
  const models=$('msModels');if(models)models.innerHTML='<div class="ms-empty">暂时无法读取模型统计。</div>';
}
function render(report){
  const error=$('msError');if(error){error.hidden=true;error.textContent=''}
  const models=Array.isArray(report.models)?report.models:[];
  const total=models.reduce((sum,item)=>sum+(Number(item.requests)||0),0);
  const unavailable=models.reduce((sum,item)=>sum+(Number(item.unavailable_channel_requests)||0),0);
  if($('msSummary'))$('msSummary').innerHTML=[
    summaryCard('统计模型',models.length,'个模型'),
    summaryCard('统计分组',Number(report.group_count)||0,'个分组'),
    summaryCard('模型请求总数',total,'次（已路由 + 无可用渠道）'),
    summaryCard('已进入渠道',total-unavailable,'次'),
    summaryCard('无可用渠道',unavailable,'次，表示客户有需求但当前没有可用渠道')
  ].join('');
  if($('msCoverage')){
    const complete=report.source?.facts_complete===true;
    const requestsComplete=report.source?.requests_complete===true||
      (report.source?.requests_complete===undefined&&report.source?.facts_complete===true);
    const ttftComplete=report.source?.frt_complete===true||
      (report.source?.frt_complete===undefined&&report.source?.ttft_complete===true);
    const pending=!complete&&report.source?.coverage_status==='pending';
    const note=report.source?.note||'已路由请求与前置无可用渠道请求按互斥事实合并统计。';
    const boundary=coverage=>{
      if(!coverage?.through_ts)return '';
      return new Date(Number(coverage.through_ts)*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'});
    };
    const routed=boundary(report.source?.routed_coverage),rejected=boundary(report.source?.unavailable_coverage);
    const watermarks=[routed?'已路由水位 '+routed:'',rejected?'拒绝日志水位 '+rejected:''].filter(Boolean).join('；');
    const rejectionSource=report.source?.unavailable_coverage?.source||'';
    const sourceNote={mixed:'拒绝来源：CloudWatch 与旧采集器混合，覆盖不完整',collector:'拒绝来源：旧采集器（无连续水位证明）',cloudwatch:'拒绝来源：CloudWatch 直采'}[rejectionSource]||'';
    const actualFrom=Number(report.from_ts)||0,actualTo=Number(report.to_ts)||0;
    const actualWindow=actualFrom>0&&actualTo>actualFrom?'统计区间 '+new Date(actualFrom*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'})+' 至 '+new Date(actualTo*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'统计区间未知';
    // Coverage describes this closed window; freshness describes the tail that
    // is not yet in it. A collector lag must not invalidate complete history,
    // and older responses without this contract must not invent a freshness.
    const target=numericField(report,['target_ts']),lag=numericField(report,['lag_seconds']);
    const freshness=actualTo>0&&target!==null&&target>=actualTo&&lag!==null&&lag>0?
      '数据更新到 '+new Date(actualTo*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'})+'；采集进度比目标落后 '+num(Math.ceil(lag/60))+' 分钟。':'';
    const stateText='请求事实：'+(requestsComplete?'完整':'未完整')+'；FRT（首个数据事件延迟）：'+(ttftComplete?'完整':'未完整')+'。';
    $('msCoverage').textContent=freshness+(complete?'数据覆盖已确认：':pending?'历史覆盖已核验，实时尾段待定稿：':'历史覆盖尚未全部确认：')+stateText+actualWindow+'；'+note+(sourceNote?'；'+sourceNote:'')+(watermarks?' '+watermarks:'');
    $('msCoverage').classList.toggle('incomplete',!complete&&!pending);
  }
  if(!$('msModels'))return;
  $('msModels').innerHTML=models.length?renderModels(models):'<div class="ms-empty">当前时间范围没有可用的模型请求记录。</div>';
}
function summaryCard(title,value,note){
  return '<div class="ms-summary-card"><small>'+esc(title)+'</small><b>'+num(value)+'</b><span>'+esc(note)+'</span></div>';
}
function renderModels(models){
  const total=models.reduce((sum,item)=>sum+(Number(item.requests)||0),0);
  const ttftComplete=state.report?.source?.frt_complete===true||
    (state.report?.source?.frt_complete===undefined&&state.report?.source?.ttft_complete===true);
  let rows='';
  models.forEach(model=>{
    const name=model.model||'未标注模型';
    const open=state.expanded.has(name);
    const groups=Array.isArray(model.groups)?model.groups:[];
    const requests=Math.max(Number(model.requests)||0,0);
    const unavailable=Math.max(Number(model.unavailable_channel_requests)||0,0);
    const highUnavailable=requests>0&&unavailable/requests>0.4;
    const ttft=ttftStats(model,ttftComplete);
    // A row is slow when either the estimated p95 exceeds the strict 3s
    // threshold or at least one observed request crossed that threshold.  The
    // latter matters when the histogram's p95 bucket is coarse (for example a
    // single slow request among a large fast sample) and keeps model/group/
    // channel highlighting consistent with the API's exact over-3s count.
    const slowRow=ttft.p95>3000||ttftOver3s(model,ttftComplete)>0;
    rows+='<tr class="ms-model-row'+(open?' is-open':'')+(highUnavailable?' ms-model-row-high-unavailable':'')+(slowRow?' ms-ttft-row-slow':'')+'"'+(highUnavailable?' title="无可用渠道请求占比超过40%"':'')+'><td class="ms-model-name"><button type="button" class="ms-expand" data-ms-expand="'+esc(name)+'" aria-expanded="'+String(open)+'" aria-label="'+(open?'收起':'展开')+esc(name)+'">'+(open?'▾':'▸')+'</button><strong>'+esc(name)+'</strong></td><td>'+num(model.requests)+'</td><td>'+num(model.routed_requests)+'</td><td>'+num(model.unavailable_channel_requests)+'</td><td>'+pct(Number(model.requests)||0,total)+'</td><td>'+ttftCell(ttft,'observed')+'</td><td>'+ttftCell(ttft,'p50')+'</td><td>'+ttftCell(ttft,'p95')+'</td><td>'+ttftCell(ttft,'p99')+'</td><td>'+ttftCell(ttft,'max')+'</td><td>'+ttftCell(ttft,'over')+'</td></tr>';
    if(open){
      const groupRows=groups.map(group=>renderGroup(name,group,Number(model.requests)||0,ttftComplete)).join('');
      rows+='<tr class="ms-detail-row"><td colspan="11"><div class="ms-detail"><div class="ms-detail-title">'+esc(name)+' 的分组明细（点击分组查看渠道和客户）</div><table><thead><tr><th>分组</th><th>请求次数</th><th>已进入渠道</th><th>无可用渠道</th><th>占该模型</th><th>FRT样本</th><th>FRT P50</th><th>FRT P95</th><th>FRT P99</th><th>最大FRT</th><th>超3秒</th></tr></thead><tbody>'+groupRows+'</tbody></table></div></td></tr>';
    }
  });
  return '<div class="ms-model-table-wrap"><table><thead><tr><th>模型（点击展开分组）</th><th>请求次数</th><th>已进入渠道</th><th>无可用渠道</th><th>占全部模型请求</th><th>FRT样本</th><th>FRT P50</th><th>FRT P95</th><th>FRT P99</th><th>最大FRT</th><th>超3秒</th></tr></thead><tbody>'+rows+'</tbody></table></div>';
}
function renderGroup(modelName,group,modelTotal,ttftComplete=false){
  const groupName=group.group||'未标注分组';
  const key=JSON.stringify([modelName,groupName]);
  const open=state.expandedGroups.has(key);
  const customers=Array.isArray(group.customers)?group.customers:[];
  const ttft=ttftStats(group,ttftComplete);
  const slowRow=ttft.p95>3000||ttftOver3s(group,ttftComplete)>0;
  let html='<tr class="ms-group-row'+(open?' is-open':'')+(slowRow?' ms-ttft-row-slow':'')+'" data-ms-group-key="'+esc(key)+'"><td><button type="button" class="ms-expand ms-group-expand" data-ms-group-key="'+esc(key)+'" aria-expanded="'+String(open)+'" aria-label="'+(open?'收起':'展开')+esc(groupName)+'的渠道和客户明细">'+(open?'▾':'▸')+'</button><strong>'+esc(groupName)+'</strong></td><td>'+num(group.requests)+'</td><td>'+num(group.routed_requests)+'</td><td>'+num(group.unavailable_channel_requests)+'</td><td>'+pct(Number(group.requests)||0,modelTotal)+'</td><td>'+ttftCell(ttft,'observed')+'</td><td>'+ttftCell(ttft,'p50')+'</td><td>'+ttftCell(ttft,'p95')+'</td><td>'+ttftCell(ttft,'p99')+'</td><td>'+ttftCell(ttft,'max')+'</td><td>'+ttftCell(ttft,'over')+'</td></tr>';
  if(!open)return html;
  const channels=Array.isArray(group.channels)?group.channels:[];
  const channelRows=channels.map(channel=>renderChannel(channel,ttftComplete)).join('');
  const customerRows=customers.map(customer=>{
    const customerID=Number(customer.customer_id)||0;
    return '<tr><td>'+(customerID>0?num(customerID):'无法识别')+'</td><td>'+esc(customer.customer_name||'未知客户')+'</td><td>'+num(customer.requests)+'</td><td>'+pct(Number(customer.requests)||0,Number(group.requests)||0)+'</td></tr>';
  }).join('');
  const channelHTML='<div class="ms-subtitle">'+esc(groupName)+' 的渠道首个数据事件延迟（FRT）</div><table class="ms-channel-table"><thead><tr><th>渠道</th><th>请求次数</th><th>FRT样本</th><th>FRT P50</th><th>FRT P95</th><th>FRT P99</th><th>最大FRT</th><th>超3秒</th></tr></thead><tbody>'+(channelRows||'<tr><td colspan="8" class="ms-customer-empty">暂无渠道 FRT 数据</td></tr>')+'</tbody></table>';
  html+='<tr class="ms-customer-detail-row"><td colspan="11"><div class="ms-customer-detail">'+channelHTML+'<div class="ms-subtitle">'+esc(groupName)+' 的客户明细</div><table><thead><tr><th>客户 ID</th><th>客户名</th><th>请求次数</th><th>占该分组</th></tr></thead><tbody>'+(customerRows||'<tr><td colspan="4" class="ms-customer-empty">暂无可识别的客户明细</td></tr>')+'</tbody></table></div></td></tr>';
  return html;
}

// Legacy ttft_* values are carried by the model-statistics API in milliseconds;
// they are FRT (the first observed data event), not a verified model-token TTFT.
// Keep the renderer tolerant of older responses so missing fields are shown as an
// unknown value instead of being mistaken for a fast request.
function numericField(item,names){
  for(const name of names){
    if(item&&item[name]!==undefined&&item[name]!==null&&item[name]!==''&&Number.isFinite(Number(item[name])))return Number(item[name]);
  }
  return null;
}
function ttftStats(item,complete=false){
  // A partial coverage window may contain valid FRT rows, but showing their
  // percentile as if it described the whole request window would make missing
  // historical samples look fast. Keep the numbers in the API for diagnosis,
  // while the page displays an explicit unknown value until coverage is whole.
  if(!complete)return {observed:0,p50:0,p95:0,p99:0,max:0,over:0,overPct:null};
  const observed=numericField(item,['frt_observed','ttft_observed','ttft_samples','ttft_sample_count','ttft_count']);
  const p50=numericField(item,['frt_p50_ms','ttft_p50_ms']);
  const p95=numericField(item,['frt_p95_ms','ttft_p95_ms']);
  const max=numericField(item,['frt_max_ms','ttft_max_ms']);
  const overRaw=numericField(item,['frt_over_3s','ttft_over_3s','ttft_over_3000ms','ttft_slow_count']);
  // An exact over-3s counter without an observed denominator is an invalid
  // legacy/partial projection. Keep every FRT value unknown in that case.
  const p99=numericField(item,['frt_p99_ms','ttft_p99_ms']);
  if(observed===null||observed<=0)return {observed:0,p50:0,p95:0,p99:0,max:0,over:0,overPct:null};
  // Keep a missing over-3s field distinct from an explicit zero.  Older
  // projections do not carry this counter; rendering that absence as
  // "0次" would falsely claim that the full FRT distribution was observed.
  const over=overRaw===null||overRaw<0||overRaw>observed?null:overRaw;
  let overPct=numericField(item,['frt_over_3s_pct','ttft_over_3s_pct','ttft_over_3000ms_pct','ttft_slow_pct']);
  if(overPct===null&&over!==null&&observed!==null&&observed>0)overPct=over*100/observed;
  return {observed:observed===null?0:Math.max(0,observed),p50:p50===null?0:Math.max(0,p50),p95:p95===null?0:Math.max(0,p95),p99:p99===null?0:Math.max(0,p99),max:max===null?0:Math.max(0,max),over:over,overPct:overPct===null?null:Math.max(0,overPct)};
}
function ttftOver3s(item,complete=false){
  if(!complete)return 0;
  const observed=numericField(item,['frt_observed','ttft_observed','ttft_samples','ttft_sample_count','ttft_count']);
  if(observed===null||observed<=0)return 0;
  const over=numericField(item,['frt_over_3s','ttft_over_3s','ttft_over_3000ms','ttft_slow_count']);
  return over===null||over<0||over>observed?0:over;
}
function formatMs(ms){return ms>0?(ms/1000).toFixed(ms>=10000?0:1)+'秒':'—'}
function ttftCell(stats,kind){
  if(kind==='observed')return stats.observed>0?num(stats.observed):'—';
  if(kind==='over'){
    if(stats.over===null||stats.observed<=0)return '—';
    const share=stats.overPct===null?'':(' '+stats.overPct.toFixed(1)+'%');
    return '<span class="'+(stats.over>0?'ms-ttft-slow':'')+'">'+num(stats.over)+'次'+share+'</span>';
  }
  const value=stats[kind];
  if(value<=0)return '—';
  return '<span class="'+(value>3000?'ms-ttft-slow':'')+'">'+formatMs(value)+'</span>';
}
function renderChannel(channel,ttftComplete=false){
  const rawID=Number(channel?.channel_id);
  const id=Number.isFinite(rawID)?rawID:0;
  const kind=String(channel?.channel_kind||'').trim().toLowerCase();
  const rawName=String(channel?.channel_name||'').trim();
  // New responses use channel_kind/is_unavailable plus channel_id=-1 for
  // pre-route rejection. Keep the legacy boolean/name fallback only for old
  // local databases; the localized name is never used as an identity key.
  const unavailable=channel?.is_unavailable===true||channel?.unavailable===true||channel?.available===false||kind==='unavailable'||id===-1;
  const resolvedKind=unavailable?'unavailable':(kind|| (id===0?'routed_unknown':'routed'));
  const label=unavailable?'无可用渠道':(rawName||(id>0?'#'+id:'未标注渠道'));
  // Keep a structured key on the row for future drill-downs and browser
  // automation. It deliberately uses kind + numeric ID, never the localized
  // channel name, so channel_id=-1 cannot collide with routed channel_id=0.
  const identity=resolvedKind+':'+id;
  const identityTitle=unavailable?'无可用渠道（channel_id=-1）':(id===0?'未标注渠道（channel_id=0）':label+'（channel_id='+id+'）');
  const ttft=ttftStats(channel,ttftComplete);
  const slow=ttft.p95>3000||ttftOver3s(channel,ttftComplete)>0;
  return '<tr class="ms-channel-row'+(slow?' ms-ttft-row-slow':'')+'" data-ms-channel-key="'+esc(identity)+'" data-ms-channel-kind="'+esc(resolvedKind)+'" data-ms-channel-id="'+String(id)+'"><td title="'+esc(identityTitle)+'">'+esc(label)+'</td><td>'+num(channel.requests)+'</td><td>'+ttftCell(ttft,'observed')+'</td><td>'+ttftCell(ttft,'p50')+'</td><td>'+ttftCell(ttft,'p95')+'</td><td>'+ttftCell(ttft,'p99')+'</td><td>'+ttftCell(ttft,'max')+'</td><td>'+ttftCell(ttft,'over')+'</td></tr>';
}
})();
