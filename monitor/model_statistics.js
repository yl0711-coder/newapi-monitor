(function(){
'use strict';

const state={inited:false,loading:false,window:'24h',from:'',to:'',range:'',dates:null,generation:0,abort:null,report:null,expanded:new Set(),expandedGroups:new Set(),refreshTimer:null,refreshHandlersInited:false};
const $=id=>document.getElementById(id);
const esc=value=>String(value==null?'':value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=value=>(Number(value)||0).toLocaleString('zh-CN');
const pct=(value,total)=>total>0?((value*100)/total).toFixed(1)+'%':'—';
const WINDOW_LABEL={'24h':'近24小时','3d':'近3天','7d':'近7天'};

window.modelStatisticsActivate=function(){
  if(!state.inited)init();
  window.ModelReportDates?.restore(state,window.monitorNavigationContext?.()||{});
  load();
  scheduleRefresh();
};
window.modelStatisticsDeactivate=function(){
  if(!state.inited)return;
  state.generation++;
  state.abort?.abort();
  state.abort=null;
  state.loading=false;
  if(state.refreshTimer){clearTimeout(state.refreshTimer);state.refreshTimer=null}
};

function scheduleRefresh(){
  if(state.refreshTimer)clearTimeout(state.refreshTimer);
  state.refreshTimer=null;
  const pane=$('tab-model-statistics');
  if(document.hidden||pane?.hidden)return;
  state.refreshTimer=setTimeout(()=>{
    state.refreshTimer=null;
    if(!document.hidden&&!pane?.hidden){
      const frt=window.monitorNavigationContext?.().view==='frt';
      if(frt)window.modelFRTRefresh?.();else load();
    }
    scheduleRefresh();
  },30000);
}
window.modelStatisticsScheduleRefresh= scheduleRefresh;

window.modelStatisticsComparison=function(){
  const r=state.report;
  if(state.loading||!r||!(Number(r.to_ts)>0))return null;
  if(!(Number(r.from_ts)>0)||Number(r.to_ts)<=Number(r.from_ts)||r.range?.has_window===false)return null;
  return {from:new Date(Number(r.from_ts)*1000).toISOString(),to:new Date(Number(r.to_ts)*1000).toISOString(),range:'exact'};
};

window.modelStatisticsNavigationContext=()=>window.ModelReportDates?.context(state)||{window:state.window};
function selectRange(selection){
  window.ModelReportDates.apply(state,selection);
  if(window.monitorNavigate)window.monitorNavigate('model-statistics',window.modelStatisticsNavigationContext());else load();
}

function init(){
  state.inited=true;
  state.dates=window.ModelReportDates?.mount('ms',()=>state,selectRange);
  $('msRefresh')?.addEventListener('click',()=>load());
  document.querySelectorAll('[data-ms-window]').forEach(button=>button.addEventListener('click',()=>{
    const key=button.getAttribute('data-ms-window');
    if(WINDOW_LABEL[key])selectRange({window:key});
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
  ensureRefreshHandlers();
}

function ensureRefreshHandlers(){
  if(state.refreshHandlersInited)return;
  state.refreshHandlersInited=true;
  const refreshIfVisible=()=>{
    const pane=$('tab-model-statistics');
    if(document.hidden||pane?.hidden)return;
    const frt=window.monitorNavigationContext?.().view==='frt';
    if(frt)window.modelFRTRefresh?.();else load();
    scheduleRefresh();
  };
  window.addEventListener('focus',refreshIfVisible);
  document.addEventListener('visibilitychange',()=>{
    if(document.hidden){if(state.refreshTimer){clearTimeout(state.refreshTimer);state.refreshTimer=null}}
    else refreshIfVisible();
  });
}
window.modelStatisticsEnsureRefreshHandlers=ensureRefreshHandlers;

async function load(windowKey){
  if(windowKey!==undefined){if(!WINDOW_LABEL[windowKey])return;state.window=windowKey;state.from='';state.to='';state.range=''}
  state.dates?.sync(state);
  state.abort?.abort();
  const controller=new AbortController();
  state.abort=controller;
  const generation=++state.generation;
  state.loading=true;
  if(!state.report){
    if($('msModels'))$('msModels').innerHTML='<div class="ms-empty">正在读取所选范围…</div>';
    if($('msCoverage'))$('msCoverage').textContent='正在确认所选范围及覆盖…';
  }
  if($('msError'))$('msError').hidden=true;
  setActiveWindow();
  setStatus(state.report?'正在更新，当前仍显示上次结果…':'正在读取本地模型事实…','');
  try{
    const query=new URLSearchParams(window.ModelReportDates?.context(state)||{window:state.window});
    const response=await fetch('/model-statistics/report?'+query,{
      headers:{'Accept':'application/json'},cache:'no-store',signal:controller.signal
    });
    if(generation!==state.generation)return;
    if(response.status===401){location.href='/login';return}
    const body=await response.text();
    let data={};try{data=body?JSON.parse(body):{}}catch(_){throw new Error('接口返回了无效 JSON')}
    if(!response.ok)throw new Error(data.error||'模型统计读取失败');
    if(generation!==state.generation)return;
    state.report=data;
    render(data);
    state.dates?.sync(state,data);
    // Keep the wall-clock generation time and the actual closed right edge
    // visible; lag is a data-watermark signal, not a browser refresh hint.
    const generated=Number(data.generated_at||0),right=Number(data.actual_through_ts??data.to_ts??0),lag=Number(data.lag_seconds||0);
    setStatus('响应生成于 '+new Date(generated*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'})+
      (right>0?' · 水位 '+new Date(right*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):' · 水位未知')+
      (lag>0?' · 延迟 '+Math.ceil(lag/60)+' 分钟':'')+
      (data.collection_failed?' · 最近一次采集失败':''),lag>0||data.collection_failed?'':'ok');
    if($('msCoverage'))$('msCoverage').textContent+=' '+(window.MonitorView?.timing(data)||'');
  }catch(error){
    if(error?.name==='AbortError')return;
    if(generation!==state.generation)return;
    renderError(error?.message||'模型统计读取失败');
    setStatus(state.report?'刷新失败，保留上次成功结果；当前覆盖未确认':'读取失败，可刷新重试','');
    if(!state.report&&$('msCoverage'))$('msCoverage').textContent='本次读取未成功，无法确认所选日期的数据覆盖。';
  }finally{
    if(generation===state.generation){state.loading=false;state.abort=null}
  }
}

function setActiveWindow(){
  document.querySelectorAll('[data-ms-window]').forEach(button=>button.classList.toggle('active',!state.from&&!state.to&&button.getAttribute('data-ms-window')===state.window));
}
function setStatus(text,kind){
  const target=$('msStatus');if(!target)return;
  target.textContent=text;target.className='ms-status '+(kind||'');
}
function renderError(message){
  const error=$('msError');if(error){error.hidden=false;error.textContent=message}
  if(state.report)return;
  const summary=$('msSummary');if(summary)summary.innerHTML='';
  const models=$('msModels');if(models)models.innerHTML='<div class="ms-empty">暂时无法读取模型统计。</div>';
}
function render(report){
  if(window.MonitorView)return window.MonitorView.preserve($('msRequestsView'),()=>renderContent(report));
  renderContent(report);
}
function renderContent(report){
  const error=$('msError');if(error){error.hidden=true;error.textContent=''}
  const models=Array.isArray(report.models)?report.models:[];
  const total=models.reduce((sum,item)=>sum+(Number(item.requests)||0),0);
  const unavailable=models.reduce((sum,item)=>sum+(Number(item.unavailable_channel_requests)||0),0);
  const noProof=!!report.range&&(report.range.has_window===false||models.length===0&&report.source?.requests_complete!==true);
  if($('msSummary'))$('msSummary').innerHTML=noProof?'<div class="ms-empty">所选范围覆盖未确认，不能据此确认零请求。</div>':[
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
    const pending=!complete&&report.source?.coverage_status==='pending';
    const note=String(report.source?.note||'已路由请求与前置无可用渠道请求按互斥事实合并统计。').split('；').filter(part=>!/FRT|TTFT|首字耗时|首个数据事件延迟/.test(part)).join('；');
    const boundary=coverage=>{
      if(!coverage?.through_ts)return '';
      return new Date(Number(coverage.through_ts)*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'});
    };
    const routed=boundary(report.source?.routed_coverage),rejected=boundary(report.source?.unavailable_coverage);
    const watermarks=[routed?'已路由水位 '+routed:'',rejected?'拒绝日志水位 '+rejected:''].filter(Boolean).join('；');
    const rejectionSource=report.source?.unavailable_coverage?.source||'';
    const sourceNote={mixed:'拒绝来源：CloudWatch 与旧采集器混合，覆盖不完整',collector:'拒绝来源：旧采集器（无连续水位证明）',cloudwatch:'拒绝来源：CloudWatch 直采'}[rejectionSource]||'';
    const actualFrom=Number(report.from_ts)||0,actualTo=Number(report.actual_through_ts??report.to_ts)||0;
    const actualWindow=actualFrom>0&&actualTo>actualFrom?'统计区间 '+new Date(actualFrom*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'})+' 至 '+new Date(actualTo*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'统计区间未知';
    // Coverage describes this closed window; freshness describes the tail that
    // is not yet in it. A collector lag must not invalidate complete history,
    // and older responses without this contract must not invent a freshness.
    const sync=window.MonitorView?.freshness(report);
    const freshness='数据截至 '+(actualTo>0?new Date(actualTo*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'未知')+'。'+(window.MonitorView?.timing(report)||sync?.text||'');
    const stateText='请求事实：'+(requestsComplete?'完整':'未完整')+'。';
    $('msCoverage').textContent=(window.ModelReportDates?.describe(report.range)||'')+freshness+(complete?'数据覆盖已确认：':pending?'历史覆盖已核验，实时尾段待定稿：':'历史覆盖尚未全部确认：')+stateText+actualWindow+'；'+note+(sourceNote?'；'+sourceNote:'')+(watermarks?' '+watermarks:'');
    $('msCoverage').classList.toggle('incomplete',(!complete&&!pending)||!!sync?.warning||!!report.collection_failed);
  }
  if(!$('msModels'))return;
  $('msModels').innerHTML=models.length?renderModels(models):'<div class="ms-empty">'+(noProof?'所选范围覆盖未完整，不能据此确认没有请求。':'当前时间范围没有可用的模型请求记录。')+'</div>';
}
function summaryCard(title,value,note){
  return '<div class="ms-summary-card"><small>'+esc(title)+'</small><b>'+num(value)+'</b><span>'+esc(note)+'</span></div>';
}
function renderModels(models){
  const total=models.reduce((sum,item)=>sum+(Number(item.requests)||0),0);
  let rows='';
  models.forEach(model=>{
    const name=model.model||'未标注模型';
    const open=state.expanded.has(name);
    const groups=Array.isArray(model.groups)?model.groups:[];
    const requests=Math.max(Number(model.requests)||0,0);
    const unavailable=Math.max(Number(model.unavailable_channel_requests)||0,0);
    // Only this model's unavailable-channel share controls row highlighting.
    // Exactly 35% is not highlighted.
    const highUnavailable=requests>0&&unavailable/requests>0.35;
    rows+='<tr class="ms-model-row'+(open?' is-open':'')+(highUnavailable?' ms-model-row-high-unavailable':'')+'"'+(highUnavailable?' title="无可用渠道请求次数占该模型总请求次数超过35%"':'')+'><td class="ms-model-name"><button type="button" class="ms-expand" data-ms-expand="'+esc(name)+'" aria-expanded="'+String(open)+'" aria-label="'+(open?'收起':'展开')+esc(name)+'">'+(open?'▾':'▸')+'</button><strong>'+esc(name)+'</strong></td><td>'+num(model.requests)+'</td><td>'+num(model.routed_requests)+'</td><td>'+num(model.unavailable_channel_requests)+'</td><td>'+pct(Number(model.requests)||0,total)+'</td></tr>';
    if(open){
      const groupRows=groups.map(group=>renderGroup(name,group,Number(model.requests)||0)).join('');
      rows+='<tr class="ms-detail-row" data-ms-section-key="'+esc(JSON.stringify([name]))+'"><td colspan="5"><div class="ms-detail"><div class="ms-detail-title">'+esc(name)+' 的分组明细（点击分组查看渠道和客户）</div><table><thead><tr><th>分组</th><th>请求次数</th><th>已进入渠道</th><th>无可用渠道</th><th>占该模型</th></tr></thead><tbody>'+groupRows+'</tbody></table></div></td></tr>';
    }
  });
  return '<div class="ms-model-table-wrap"><table><thead><tr><th>模型（点击展开分组）</th><th>请求次数</th><th>已进入渠道</th><th>无可用渠道</th><th>占全部模型请求</th></tr></thead><tbody>'+rows+'</tbody></table></div>';
}
function renderGroup(modelName,group,modelTotal){
  const groupName=group.group||'未标注分组';
  const key=JSON.stringify([modelName,groupName]);
  const open=state.expandedGroups.has(key);
  const customers=Array.isArray(group.customers)?group.customers:[];
  let html='<tr class="ms-group-row'+(open?' is-open':'')+'" data-ms-group-key="'+esc(key)+'"><td><button type="button" class="ms-expand ms-group-expand" data-ms-group-key="'+esc(key)+'" aria-expanded="'+String(open)+'" aria-label="'+(open?'收起':'展开')+esc(groupName)+'的渠道和客户明细">'+(open?'▾':'▸')+'</button><strong>'+esc(groupName)+'</strong></td><td>'+num(group.requests)+'</td><td>'+num(group.routed_requests)+'</td><td>'+num(group.unavailable_channel_requests)+'</td><td>'+pct(Number(group.requests)||0,modelTotal)+'</td></tr>';
  if(!open)return html;
  const channels=Array.isArray(group.channels)?group.channels:[];
  const channelRows=channels.map(channel=>renderChannel(channel)).join('');
  const customerRows=customers.map(customer=>{
    const customerID=Number(customer.customer_id)||0;
    return '<tr><td>'+(customerID>0?num(customerID):'无法识别')+'</td><td>'+esc(customer.customer_name||'未知客户')+'</td><td>'+num(customer.requests)+'</td><td>'+pct(Number(customer.requests)||0,Number(group.requests)||0)+'</td></tr>';
  }).join('');
  const channelHTML='<div class="ms-subtitle">'+esc(groupName)+' 的渠道请求统计</div><table class="ms-channel-table"><thead><tr><th>渠道</th><th>请求次数</th></tr></thead><tbody>'+(channelRows||'<tr><td colspan="2" class="ms-customer-empty">暂无渠道请求数据</td></tr>')+'</tbody></table>';
  html+='<tr class="ms-customer-detail-row" data-ms-section-key="'+esc(key)+'"><td colspan="5"><div class="ms-customer-detail">'+channelHTML+'<div class="ms-subtitle">'+esc(groupName)+' 的客户明细</div><table><thead><tr><th>客户 ID</th><th>客户名</th><th>请求次数</th><th>占该分组</th></tr></thead><tbody>'+(customerRows||'<tr><td colspan="4" class="ms-customer-empty">暂无可识别的客户明细</td></tr>')+'</tbody></table></div></td></tr>';
  return html;
}

function numericField(item,names){
  for(const name of names){
    if(item&&item[name]!==undefined&&item[name]!==null&&item[name]!==''&&Number.isFinite(Number(item[name])))return Number(item[name]);
  }
  return null;
}
function renderChannel(channel){
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
  return '<tr class="ms-channel-row" data-ms-channel-key="'+esc(identity)+'" data-ms-channel-kind="'+esc(resolvedKind)+'" data-ms-channel-id="'+String(id)+'"><td title="'+esc(identityTitle)+'">'+esc(label)+'</td><td>'+num(channel.requests)+'</td></tr>';
}
})();
