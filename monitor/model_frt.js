(function(){
'use strict';
const $=id=>document.getElementById(id);
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const labels={'24h':'近24小时','3d':'近3天','7d':'近7天'};
const reasons={no_valid_samples:'没有有效 FRT 样本；缺失或无效值不计作 0 耗时。',historical_unsupported:'历史口径不支持，耗时与有效样本不可确认。',invalid_histogram:'直方图不一致，耗时与有效样本不可确认。'};
const meanReasons={historical_sum_unavailable:'历史样本缺少真实耗时总和，平均 FRT 不可计算。',invalid_sum:'耗时总和或样本数不一致，平均 FRT 不可计算。'};
const state={inited:false,active:false,window:'24h',from:'',to:'',range:'',dates:null,filters:{},generation:0,abort:null,report:null,open:new Set()};
const time=value=>{const d=new Date(value);return Number.isFinite(d.getTime())?d.toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'未确认'};
const duration=value=>typeof value==='number'&&Number.isFinite(value)&&value>0?(value/1000).toLocaleString('zh-CN',{maximumFractionDigits:3})+' 秒':'—';

window.modelStatisticsPageActivate=function(){
  if(!state.inited)init();
  const context=window.monitorNavigationContext?.()||{};
  const frt=context.view==='frt';
  window.modelStatisticsDeactivate?.();
  deactivate();
  $('msRequestsView').hidden=frt;$('msFRTView').hidden=!frt;
  document.querySelectorAll('[data-ms-view]').forEach(b=>{
    const selected=(b.dataset.msView==='frt')===frt;
    b.classList.toggle('active',selected);b.setAttribute('aria-selected',String(selected));b.tabIndex=selected?0:-1;
  });
  if(!frt){window.modelStatisticsActivate?.();return}
  state.active=true;
  window.ModelReportDates?.restore(state,context);
  if(!window.ModelReportDates){state.window=labels[context.window]?context.window:'24h';state.to=context.to||''}
  if(['channel_id','group','model','mean_gt_s'].some(k=>Object.hasOwn(context,k)))state.filters={channel_id:context.channel_id||'',group:context.group||'',model:context.model||'',mean_gt_s:context.mean_gt_s||''};
  $('frtChannel').value=state.filters.channel_id||'';$('frtGroup').value=state.filters.group||'';$('frtModel').value=state.filters.model||'';$('frtMeanGt').value=state.filters.mean_gt_s||'';
  const comparison=window.modelStatisticsComparison?.();
  $('frtCompare').disabled=!comparison;
  $('frtCompare').title=comparison?'使用原模型统计上次成功加载的实际窗口':'请先加载原模型统计，再按相同实际窗口核对';
  load();
  window.modelStatisticsEnsureRefreshHandlers?.();
  window.modelStatisticsScheduleRefresh?.();
};
window.modelStatisticsPageDeactivate=function(){window.modelStatisticsDeactivate?.();deactivate()};
function deactivate(){state.active=false;state.generation++;state.abort?.abort();state.abort=null}
function navigate(view){window.monitorNavigate('model-statistics',view==='frt'?{view,...(window.ModelReportDates?.context(state)||{window:state.window,to:state.to}),...state.filters}:(window.modelStatisticsNavigationContext?.()||{}))}
function init(){
  state.inited=true;
  state.dates=window.ModelReportDates?.mount('frt',()=>state,selection=>{window.ModelReportDates.apply(state,selection);navigate('frt')});
  document.querySelectorAll('[data-ms-view]').forEach(button=>{
    button.addEventListener('click',()=>navigate(button.dataset.msView));
    button.addEventListener('keydown',event=>{
      if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key))return;
      event.preventDefault();const target=event.key==='Home'?'msRequestsTab':event.key==='End'?'msFRTTab':button.id==='msFRTTab'?'msRequestsTab':'msFRTTab';
      $(target).focus();navigate($(target).dataset.msView);
    });
  });
  document.querySelectorAll('[data-frt-window]').forEach(button=>button.addEventListener('click',()=>{state.window=button.dataset.frtWindow;state.from='';state.to='';state.range='';navigate('frt')}));
  $('frtFilters').addEventListener('submit',event=>{event.preventDefault();state.filters={channel_id:$('frtChannel').value.trim(),group:$('frtGroup').value.trim(),model:$('frtModel').value.trim(),mean_gt_s:$('frtMeanGt').value.trim()};navigate('frt')});
  $('frtReset').addEventListener('click',()=>{state.filters={};$('frtMeanGt').value='';navigate('frt')});
  $('frtRefresh').addEventListener('click',load);
  $('frtLatest').addEventListener('click',()=>{state.from='';state.to='';state.range='';navigate('frt')});
  $('frtCompare').addEventListener('click',()=>{const c=window.modelStatisticsComparison?.();if(c){state.window=c.window||state.window;state.from=c.from||'';state.to=c.to;state.range='exact';navigate('frt')}});
  $('frtTree').addEventListener('toggle',event=>{
    const e=event.target,key=e.dataset?.frtKey;if(!key||!e.isConnected)return;
    if(e.open)state.open.add(key);else state.open.delete(key);
  },true);
}
async function load(){
  if(!state.active)return;
  state.abort?.abort();const controller=new AbortController(),generation=++state.generation;
  state.abort=controller;
  $('frtError').hidden=true;$('frtTree').setAttribute('aria-busy','true');
  $('frtStatus').textContent=state.report?'正在更新，当前仍显示上次结果…':'正在读取本地 FRT 事实…';
  if(!state.report){$('frtCoverage').textContent='当前窗口读取中…';$('frtCoverage').classList.remove('gap')}
  state.dates?.sync(state);
  $('frtLatest').hidden=!state.to&&!state.from;
  document.querySelectorAll('[data-frt-window]').forEach(b=>b.classList.toggle('active',!state.from&&!state.to&&b.dataset.frtWindow===state.window));
  const query=new URLSearchParams(window.ModelReportDates?.context(state)||{window:state.window});if(state.to)query.set('to',state.to);
  for(const [key,value] of Object.entries(state.filters))if(value!=='')query.set(key,value);
  try{
    const response=await fetch('/model-statistics/frt?'+query,{headers:{Accept:'application/json'},cache:'no-store',signal:controller.signal});
    if(generation!==state.generation)return;
    if(response.status===401){location.href='/login';return}
    const data=await response.json();
    if(generation!==state.generation)return;
    if(!response.ok)throw Error(data.error||'首字耗时读取失败');
    if(!Array.isArray(data.channels)||!data.coverage||!Number.isFinite(Date.parse(data.from))||!Number.isFinite(Date.parse(data.to)))throw Error('接口返回的 FRT 报告不完整');
    state.report=data;render(data);state.dates?.sync(state,data);
    $('frtStatus').textContent='响应生成于 '+time(data.generated_at)+'（北京时间）';
  }catch(error){
    if(generation!==state.generation||error?.name==='AbortError')return;
    $('frtError').hidden=false;$('frtError').textContent=error?.message||'首字耗时读取失败';
    if(!state.report)$('frtCoverage').textContent='本次读取未成功，无法确认数据覆盖。';
    $('frtStatus').textContent=state.report?'刷新失败，保留上次成功结果；当前覆盖未确认':'读取失败，可刷新重试';
  }finally{
    if(generation===state.generation){state.abort=null;$('frtTree').setAttribute('aria-busy','false')}
  }
}
// The shared model-statistics timer and focus/visibility handlers live in the
// parent page script. Expose the active FRT loader so those handlers refresh
// this view as well instead of silently calling the hidden request report.
window.modelFRTRefresh=()=>{if(state.active)load()};
function metrics(frt){
  frt=frt||{};
  const count=typeof frt.observed==='number'&&Number.isSafeInteger(frt.observed)&&frt.observed>=0?frt.observed.toLocaleString('zh-CN'):'—';
  const values=[['FRT 样本',count],['平均 FRT',typeof frt.mean_ms==='number'&&Number.isFinite(frt.mean_ms)&&frt.mean_ms>0?duration(frt.mean_ms):'不可计算'],['P50',duration(frt.p50_ms)],['P95',duration(frt.p95_ms)],['P99',duration(frt.p99_ms)],['最大 FRT',duration(frt.max_ms)],['数据覆盖',frt.coverage_complete?'已核验':'未完整']];
  return '<dl class="frt-values">'+values.map(([k,v])=>'<div><dt>'+k+'</dt><dd>'+esc(v)+'</dd></div>').join('')+'</dl>'+(frt.unavailable_reason?'<span class="frt-reason">'+esc(reasons[frt.unavailable_reason]||'样本不可用，原因待确认。')+'</span>':'')+(frt.mean_unavailable_reason&&frt.mean_unavailable_reason!==frt.unavailable_reason?'<span class="frt-reason">'+esc(meanReasons[frt.mean_unavailable_reason]||reasons[frt.mean_unavailable_reason]||'平均 FRT 不可计算，缺少总和覆盖证明。')+'</span>':'');
}
function node(key,title,frt,children,kind){return '<details class="frt-node frt-'+kind+'" data-frt-key="'+esc(key)+'"'+(state.open.has(key)?' open':'')+'><summary><b class="frt-title">'+title+'</b>'+metrics(frt)+'</summary>'+children+'</details>'}
function render(report){
  if(window.MonitorView)return window.MonitorView.preserve($('msFRTView'),()=>renderContent(report));
  renderContent(report);
}
function renderContent(report){
  const c=report.coverage,sync=window.MonitorView?.freshness(report);
  const f=report.filter||state.filters;
  const meanGt=typeof f.mean_gt_ms==='number'&&Number.isFinite(f.mean_gt_ms)?f.mean_gt_ms/1000:null;
  const scope=[f.channel_id!=null&&f.channel_id!==''?'渠道 #'+f.channel_id:'',f.group?'实际分组 '+f.group:'',f.model?'模型 '+f.model:'',meanGt!=null?'平均 FRT 严格大于 '+meanGt.toLocaleString('zh-CN',{maximumFractionDigits:3})+' 秒':''].filter(Boolean).join(' / ');
  // 不可计算的渠道既没命中也没未命中,必须单独说明,否则会被读成"没有这种渠道"。
  const excluded=report.mean_filter_applied&&Number(report.mean_filter_excluded)>0
    ?'另有 '+Number(report.mean_filter_excluded).toLocaleString('zh-CN')+' 个渠道的平均 FRT 不可计算，未参与本次阈值筛选，既不表示达标也不表示超标。':'';
  $('frtCoverage').textContent=(window.ModelReportDates?.describe(report.range)||'')+(scope?'筛选：'+scope+'；各层统计均为此筛选范围。':'')+'统计区间 '+time(report.from)+' 至 '+time(report.to)+'（北京时间，左闭右开）。'+(state.to&&state.range!=='calendar'?'已固定为原统计窗口；':'')+c.note+(c.from&&c.through?' FRT 连续证明范围 '+time(c.from)+' 至 '+time(c.through)+'。':'')+excluded;
  $('frtCoverage').textContent+=' '+(window.MonitorView?.timing(report)||'');
  const right=Number(report.actual_through_ts??report.through_ts??report.to_ts??Math.floor(Date.parse(report.to)/1000));
  $('frtCoverage').textContent+=' · 数据截至 '+(right>0?time(right*1000):'未知');
  $('frtCoverage').classList.toggle('gap',!c.complete||!!sync?.warning||!!report.collection_failed);
  $('frtTree').innerHTML=report.channels.map(channel=>{
    const groups=(channel.groups||[]).map(group=>{
      const models=(group.models||[]).map(model=>'<div class="frt-model" data-frt-model-key="'+esc(JSON.stringify([channel.channel_id,group.group,model.model]))+'"><b class="frt-title">'+esc(model.model||'未记录模型')+'<small>模型</small></b>'+metrics(model.frt)+'</div>').join('');
      return node(JSON.stringify([channel.channel_id,group.group]),esc(group.group||'未记录实际分组')+'<small>实际服务分组</small>',group.frt,models,'group');
    }).join('');
    return node(JSON.stringify([channel.channel_id]),esc(channel.name)+'<small>#'+esc(channel.channel_id)+'</small>',channel.frt,groups,'channel');
  }).join('')||'<div class="frt-message">'+(report.mean_filter_applied?'没有渠道的平均 FRT 超过所设阈值。':'所选窗口没有可展示的本地 FRT 维度。')+(c.complete?'覆盖已核验；没有样本不表示耗时为 0。':'覆盖未完整，不能据此判断没有请求或耗时正常。')+esc(excluded)+'</div>';
}
})();
