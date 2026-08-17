(function(){
'use strict';

const ST_HEADERS={
  sync:{title:'数据同步状态',subtitle:'全历史、实时 Tail、渠道、稳定性、备份与本地存储的统一健康视图',icon:'sync'},
  usage:{title:'用户用量',subtitle:'客户余额、每日消费、成员矩阵与用量明细',icon:'users'},
  stability:{title:'稳定性报表',subtitle:'用户交付 · 分组 / 渠道 / 模型 · 历史趋势 · 问题分析',icon:'shield'},
  channels:{title:'渠道管理',subtitle:'主域名归并 · 厂商 / 实际渠道 / 服务分组 · 使用排行',icon:'globe'},
  model:{title:'模型监控',subtitle:'分钟级实时状态、告警、SLO、稳定性与响应耗时',icon:'activity'},
  server:{title:'服务端监控',subtitle:'实例、数据库、负载均衡、域名探活与证书',icon:'chart'}
};
const ST_ICONS={
  sync:'<svg viewBox="0 0 24 24"><path d="M20 7h-6V1"/><path d="M4 17h6v6"/><path d="M20 7a8 8 0 0 0-13.7-3.6L4 5.7M4 17a8 8 0 0 0 13.7 3.6l2.3-2.3"/></svg>',
  users:'<svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
  shield:'<svg viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="m9 12 2 2 4-4"/></svg>',
  globe:'<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3a15 15 0 0 1 0 18M12 3a15 15 0 0 0 0 18"/></svg>',
  activity:'<svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
  chart:'<svg viewBox="0 0 24 24"><path d="M3 3v18h18"/><path d="m7 16 4-5 4 3 5-7"/></svg>'
};

window.monitorShellSetTab=function(name){
  const h=ST_HEADERS[name]||ST_HEADERS.usage;
  const title=document.getElementById('monitorPageTitle');
  const sub=document.getElementById('monitorPageSubtitle');
  const icon=document.getElementById('monitorPageIcon');
  if(title)title.textContent=h.title;
  if(sub)sub.textContent=h.subtitle;
  if(icon)icon.innerHTML=ST_ICONS[h.icon]||'';
};

// 桌面侧栏只改变 Monitor 的可用内容宽度，不参与任何页面的数据状态。
// 记住管理员自己的显示偏好；移动端仍使用既有的顶部 Tab。
(function initMonitorSidebar(){
  const shell=document.querySelector('.monitor-shell');
  const button=document.getElementById('monitorSidebarToggle');
  if(!shell||!button)return;
  let saved=false;
  try{saved=localStorage.getItem('nexusapi-monitor-sidebar-collapsed')==='1'}catch(e){}
  const apply=collapsed=>{
    shell.classList.toggle('sidebar-collapsed',collapsed);
    button.setAttribute('aria-expanded',String(!collapsed));
    button.setAttribute('aria-label',collapsed?'展开侧边栏':'收起侧边栏');
    button.title=collapsed?'展开侧边栏':'收起侧边栏';
    const label=button.querySelector('span');if(label)label.textContent=collapsed?'展开侧边栏':'收起侧边栏';
    try{localStorage.setItem('nexusapi-monitor-sidebar-collapsed',collapsed?'1':'0')}catch(e){}
    // 等 CSS 宽度过渡结束后统一通知 ECharts 及表格重新测量。
    setTimeout(()=>window.dispatchEvent(new Event('resize')),220);
  };
  apply(saved);
  button.addEventListener('click',()=>apply(!shell.classList.contains('sidebar-collapsed')));
})();

const st={inited:false,loaded:false,view:'history',layer:'delivery',days:7,custom:null,preset:'',filters:{vendor:'',group:'',channel:'',model:''},allFilters:null,report:null,abort:null,problemAbort:null,drawerAbort:null,edgeAbort:null,edgeReport:null,generation:0,detailPromises:new Map(),detailControllers:new Map(),detailLoading:new Set(),expanded:new Set(),chart:null,drawerChart:null,edgeChart:null,drawer:null,drawerTab:'run',lastFocus:null};
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const nfmt=n=>(+n||0).toLocaleString('zh-CN');
const compact=n=>{n=+n||0;const a=Math.abs(n);for(const [d,u] of [[1e9,'B'],[1e6,'M'],[1e3,'k']])if(a>=d)return (n/d>=100?(n/d).toFixed(0):(n/d).toFixed(1)).replace(/\.0$/,'')+u;return nfmt(n)};
const pct=v=>v==null?'—':(+v).toFixed(2)+'%';
const usd=v=>'$'+(+v||0).toFixed(2);
const bytes=v=>{v=+v||0;for(const [d,u] of [[1073741824,'GB'],[1048576,'MB'],[1024,'KB']])if(v>=d)return (v/d).toFixed(v/d>=100?0:1)+' '+u;return nfmt(v)+' B'};
const dateTime=ts=>ts?new Date(ts*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'—';
const age=sec=>sec==null||sec<0?'—':sec<90?Math.round(sec)+' 秒':sec<5400?Math.round(sec/60)+' 分钟':(sec/3600).toFixed(1)+' 小时';
const health=m=>m&&m.health||'nosample';
const delta=v=>v==null?'环比 —':`环比 ${v>=0?'+':''}${(+v).toFixed(2)} pp`;
const deltaClass=v=>v==null?'':v>=0?'up':'down';
const bucketLabel=sec=>sec>=86400?`${sec/86400} 天/格`:sec>=3600?`${sec/3600} 小时/格`:`${Math.round(sec/60)} 分钟/格`;
const encodeNav=value=>btoa(unescape(encodeURIComponent(JSON.stringify(value))));
const cstDate=ts=>{const p=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date(ts*1000));const v=t=>p.find(x=>x.type===t)?.value||'';return `${v('year')}-${v('month')}-${v('day')}`};
const stPresetRange=(preset,now=Date.now())=>{
  const ts=Math.floor(now/1000),today=cstDate(ts);
  if(preset==='today')return {from:today,to:today};
  if(preset==='yesterday'){const day=cstDate(ts-86400);return {from:day,to:day}}
  const weekday=new Intl.DateTimeFormat('en-US',{timeZone:'Asia/Shanghai',weekday:'short'}).format(new Date(now));
  const sinceMonday=({Mon:0,Tue:1,Wed:2,Thu:3,Fri:4,Sat:5,Sun:6})[weekday]??0;
  return {from:cstDate(ts-sinceMonday*86400),to:today};
};

function queryParams(extra){
  const q=new URLSearchParams();
  if(st.custom){q.set('from',st.custom.from);q.set('to',st.custom.to)}else q.set('days',String(st.days));
  for(const [k,v] of Object.entries(st.filters))if(v)q.set(k,v);
  if(extra)for(const [k,v] of Object.entries(extra))if(v!=null&&v!=='')q.set(k,String(v));
  return q;
}
function loading(el,text){if(el)el.innerHTML=`<div class="stability-loading"><i class="stability-spinner"></i><span>${esc(text||'正在读取本地稳定性汇总…')}</span></div>`}
function errorBox(el,message){if(el)el.innerHTML=`<div class="stability-empty"><b>数据暂时无法读取</b><p>${esc(message||'请稍后重试。其他 Monitor 页面不受影响。')}</p></div>`}

window.stabilityActivate=function(){
  if(!st.inited)init();
  const changed=applyNavigationContext();
  if(!st.loaded||changed)loadReport();
  probeEdge();
  setTimeout(resize,80);
};
window.stabilityOpen=function(context){window.monitorNavigate?.('stability',context||{})};
function applyNavigationContext(){
  const c=window.monitorNavigationContext?.()||{};let changed=false;
  if(!Object.keys(c).length)return false;
  const next={vendor:c.vendor||'',group:c.group||'',channel:c.channel||'',model:c.model||''};
  for(const key of Object.keys(next))if(st.filters[key]!==next[key]){st.filters[key]=next[key];changed=true}
  if(c.from&&c.to){const custom={from:c.from,to:c.to};if(!st.custom||st.custom.from!==custom.from||st.custom.to!==custom.to||st.preset!=='custom'){st.custom=custom;st.preset='custom';changed=true}}
  else if(+c.days>0&&(st.days!==+c.days||st.custom||st.preset)){st.days=+c.days;st.custom=null;st.preset='';changed=true}
  syncRange();return changed;
}
function resize(){if(st.chart)st.chart.resize();if(st.drawerChart)st.drawerChart.resize();if(st.edgeChart)st.edgeChart.resize()}

function init(){
  st.inited=true;
  document.querySelectorAll('[data-stability-view]').forEach(b=>b.addEventListener('click',()=>setView(b.dataset.stabilityView)));
  document.querySelectorAll('[data-stability-days]').forEach(b=>b.addEventListener('click',()=>{st.days=+b.dataset.stabilityDays;st.custom=null;st.preset='';$('stCustomRange')?.classList.remove('show');syncRange();reloadActiveLayer()}));
  document.querySelectorAll('[data-stability-preset]').forEach(b=>b.addEventListener('click',()=>{st.preset=b.dataset.stabilityPreset;st.custom=stPresetRange(st.preset);$('stCustomRange')?.classList.remove('show');syncRange();reloadActiveLayer()}));
  $('stCustomToggle')?.addEventListener('click',()=>{$('stCustomRange')?.classList.toggle('show')});
  $('stCustomApply')?.addEventListener('click',()=>{const from=$('stCustomFrom')?.value,to=$('stCustomTo')?.value;if(!from||!to||from>to){alert('请选择正确的开始和结束日期');return}st.custom={from,to};st.preset='custom';syncRange();reloadActiveLayer()});
  document.querySelectorAll('[data-stability-layer]').forEach(b=>b.addEventListener('click',()=>setLayer(b.dataset.stabilityLayer)));
  for(const id of ['stVendor','stGroup','stChannel','stModel'])$(id)?.addEventListener('change',()=>{readFilters();loadReport()});
  $('stFilterReset')?.addEventListener('click',()=>{st.filters={vendor:'',group:'',channel:'',model:''};renderFilterOptions();loadReport()});
  $('stRefresh')?.addEventListener('click',loadReport);
  $('stProblemRefresh')?.addEventListener('click',loadProblems);
  // 分组列表由 renderReport 动态创建，事件必须委托到稳定存在的父容器。
  $('stDeliveryBody')?.addEventListener('click',groupClick);
  $('stDeliveryBody')?.addEventListener('keydown',e=>{if((e.key==='Enter'||e.key===' ')&&e.target.matches('[data-st-group]')){e.preventDefault();groupClick(e)}});
  $('stDrawerMask')?.addEventListener('click',closeDrawer);
  $('stDrawerClose')?.addEventListener('click',closeDrawer);
  document.querySelectorAll('[data-st-drawer-tab]').forEach(b=>b.addEventListener('click',()=>{st.drawerTab=b.dataset.stDrawerTab;renderDrawer()}));
  document.addEventListener('keydown',e=>{if(e.key==='Escape'&&st.drawer)closeDrawer()});
  window.addEventListener('resize',resize);
  window.addEventListener('monitor:navigate',e=>{if(e.detail?.tab==='stability'&&applyNavigationContext())loadReport()});
  syncRange();setLayer('delivery');setView('history');
}
function setView(view){
  if(st.drawer)closeDrawer();
  st.view=view;
  document.querySelectorAll('[data-stability-view]').forEach(b=>b.classList.toggle('active',b.dataset.stabilityView===view));
  if($('stHistoryView'))$('stHistoryView').hidden=view!=='history';
  if($('stProblemView'))$('stProblemView').hidden=view!=='problems';
  if(view==='problems')loadProblems();else setTimeout(resize,60);
}
function setLayer(layer){
  const button=document.querySelector(`[data-stability-layer="${layer}"]`);if(button?.disabled)return;
  st.layer=layer;
  document.querySelectorAll('[data-stability-layer]').forEach(b=>b.classList.toggle('active',b.dataset.stabilityLayer===layer));
  if($('stDeliveryLayer'))$('stDeliveryLayer').hidden=layer!=='delivery';
  if($('stEdgeLayer'))$('stEdgeLayer').hidden=layer!=='edge';
  if(layer==='delivery')setTimeout(resize,60);else loadEdge();
}
function syncRange(){document.querySelectorAll('[data-stability-days]').forEach(b=>b.classList.toggle('active',!st.custom&&+b.dataset.stabilityDays===st.days));document.querySelectorAll('[data-stability-preset]').forEach(b=>b.classList.toggle('active',b.dataset.stabilityPreset===st.preset));$('stCustomToggle')?.classList.toggle('active',st.preset==='custom')}
function readFilters(){st.filters={vendor:$('stVendor')?.value||'',group:$('stGroup')?.value||'',channel:$('stChannel')?.value||'',model:$('stModel')?.value||''}}
function reloadActiveLayer(){if(st.layer==='edge')loadEdge();else loadReport()}

async function probeEdge(){
  const button=document.querySelector('[data-stability-layer="edge"]');if(!button)return;
  try{
    const res=await fetch('/stability/edge?'+queryParams(),{headers:{Accept:'application/json'}});if(!res.ok)return;
    const d=await res.json();
    button.disabled=!d.enabled;
    button.title=d.enabled?'查看 Nginx 入口层客观汇总':'Nginx 旁路聚合未启用';
    const title=button.querySelector('b'),small=button.querySelector('small');
    if(title)title.textContent=d.enabled?'入口与平台':'入口与平台（尚未接入）';
    if(small)small.textContent=d.enabled?'Nginx access · 节点 / HTTP / 耗时':'Nginx access · 默认关闭';
    if(d.enabled)st.edgeReport=d;
  }catch(e){}
}

async function loadEdge(){
  if(st.edgeAbort)st.edgeAbort.abort();st.edgeAbort=new AbortController();loading($('stEdgeLayer'),'正在读取本地 Nginx 分钟聚合…');
  try{
    const res=await fetch('/stability/edge?'+queryParams(),{headers:{Accept:'application/json'},signal:st.edgeAbort.signal});
    if(res.status===401){location.href='/login';return}const d=await res.json();if(!res.ok)throw new Error(d.error||`HTTP ${res.status}`);
    st.edgeReport=d;renderEdge(d);
  }catch(e){if(e.name!=='AbortError')errorBox($('stEdgeLayer'),e.message)}
}

function edgeRows(rows){return (rows||[]).map(r=>`<div class="stability-model-row"><b>${esc(r.name||'—')}</b><span>${nfmt(r.requests)} 请求</span><small>4xx ${nfmt(r.status_4xx)} · 5xx ${nfmt(r.status_5xx)}</small><small>平均 ${(+r.avg_ms||0).toFixed(0)} ms</small></div>`).join('')||'<div class="stability-empty"><p>当前范围无数据</p></div>'}
function renderEdge(d){
  const el=$('stEdgeLayer');if(!el)return;if(!d.enabled){el.innerHTML='<div class="stability-panel"><div class="stability-empty"><b>Nginx 旁路聚合未启用</b><p>该能力默认关闭，不影响其他 Monitor 功能。</p></div></div>';return}
  const s=d.summary||{},sources=d.sources||[];
  el.innerHTML=`<div class="stability-advice-pending"><b>口径边界：</b>仅展示 Nginx 专用 access log 在节点侧脱敏后的分钟聚合，当前保留 ${nfmt(d.retention_days||7)} 天；超出留存期的查询按页面所示实际起止日期展示。Request ID 只统计携带率，不保存原值、不宣称已与使用日志关联；当前不采集 error log 原文。</div>`
    +`<section class="stability-kpis"><article class="stability-kpi"><small>入口请求</small><b>${nfmt(s.requests)}</b><em>${esc(d.from||'—')} 至 ${esc(d.to||'—')}</em></article><article class="stability-kpi"><small>HTTP 4xx / 5xx</small><b class="${s.status_5xx?'bad':''}">${nfmt(s.status_4xx)} / ${nfmt(s.status_5xx)}</b><em>客观状态码，不自动归因</em></article><article class="stability-kpi"><small>入口平均 / 最大耗时</small><b>${(+s.avg_request_ms||0).toFixed(0)} / ${nfmt(s.max_request_ms)} ms</b><em>upstream 平均 ${(+s.avg_upstream_ms||0).toFixed(0)} ms</em></article><article class="stability-kpi"><small>Request ID 携带率</small><b>${pct(s.request_id_coverage)}</b><em>仅“存在”，不是关联成功率 · ${bytes(s.bytes_sent)}</em></article></section>`
    +`<section class="stability-panel"><div class="stability-panel-head"><div><h3>每日入口状态变化</h3><p>折线为 HTTP 5xx 占比，柱形为入口请求量</p></div></div><div id="stEdgeChart" class="stability-chart"></div></section>`
    +`<section class="stability-ranking-grid"><article class="stability-panel"><div class="stability-panel-head"><div><h3>路径汇总</h3><p>路径已在节点侧归一化，不含 query</p></div></div>${edgeRows(d.routes)}</article><article class="stability-panel"><div class="stability-panel-head"><div><h3>节点汇总</h3><p>用于判断异常是否集中在单一入口节点</p></div></div>${edgeRows(d.nodes)}</article></section>`
    +`<section class="stability-panel"><div class="stability-panel-head"><div><h3>采集器状态</h3><p>只表示聚合数据是否持续送达，不等于业务可用性；“游标不连续”是客观采集异常，不自动推断丢失量。</p></div></div>${sources.map(x=>`<div class="stability-model-row"><b>${esc(x.node)}</b><span>${x.status==='ok'?'正常':x.status==='warn'?'延迟':'中断'}</span><small>最新事件 ${dateTime(x.last_event_ts)} · 送达 ${age(x.age_sec)} 前</small><small>${x.backlog_known?'待读 '+bytes(x.backlog_bytes):'待读未知'} · ${x.cursor_discontinuities?`游标不连续 ${nfmt(x.cursor_discontinuities)} 次，最近 ${dateTime(x.last_cursor_discontinuity_at)}`:'游标连续'} · ${x.discarded_lines?`跳过无效或超窗日志 ${nfmt(x.discarded_lines)} 行，最近 ${dateTime(x.last_discarded_at)}`:'未跳过日志'}</small></div>`).join('')||'<div class="stability-empty"><p>已启用但尚未收到采集器数据</p></div>'}</section>`;
  const chart=$('stEdgeChart');if(!chart||!window.echarts)return;if(st.edgeChart)st.edgeChart.dispose();st.edgeChart=echarts.init(chart);const rows=d.daily||[];
  st.edgeChart.setOption({animation:false,grid:{left:50,right:55,top:28,bottom:38},tooltip:{trigger:'axis'},xAxis:{type:'category',data:rows.map(x=>x.date.slice(5)),axisLabel:{color:'#7e8a9f'},axisLine:{lineStyle:{color:'#354055'}}},yAxis:[{type:'value',axisLabel:{color:'#7e8a9f',formatter:'{value}%'},splitLine:{lineStyle:{color:'#283143'}}},{type:'value',axisLabel:{color:'#657188'},splitLine:{show:false}}],series:[{name:'5xx占比',type:'line',smooth:.2,data:rows.map(x=>x.requests?+(x.status_5xx/x.requests*100).toFixed(3):null),lineStyle:{width:2,color:'#e45b69'},itemStyle:{color:'#e45b69'}},{name:'请求量',type:'bar',yAxisIndex:1,data:rows.map(x=>x.requests),barMaxWidth:20,itemStyle:{color:'rgba(79,153,229,.28)'}}]});
}

async function loadReport(){
  if(st.drawer)closeDrawer();
  const generation=++st.generation;
  for(const controller of st.detailControllers.values())controller.abort();
  st.detailControllers.clear();st.detailPromises.clear();st.detailLoading.clear();
  if(st.abort)st.abort.abort();st.abort=new AbortController();loading($('stDeliveryBody'));
  try{
    const res=await fetch('/stability/report?'+queryParams(),{headers:{Accept:'application/json'},signal:st.abort.signal});
    if(res.status===401){location.href='/login';return}
    const d=await res.json();if(!res.ok)throw new Error(d.error||`HTTP ${res.status}`);
    if(d.enabled===false){errorBox($('stDeliveryBody'),'稳定性报表已关闭（MONITOR_STABILITY_ENABLED=false）。');return}
    if(generation!==st.generation)return;
    st.report=d;st.loaded=true;
    // 未筛选响应才能作为完整候选集；切换 7/30/90 天时同步更新，
    // 避免只在新日期范围出现的渠道/模型永久无法选中。
    if(!Object.values(st.filters).some(Boolean))st.allFilters=d.filters;
    if(!st.allFilters)st.allFilters=d.filters;
    renderReport();
  }catch(e){if(e.name!=='AbortError'&&generation===st.generation)errorBox($('stDeliveryBody'),e.message)}
}

function renderFilterOptions(){
  const f=st.allFilters||st.report?.filters||{vendors:[],groups:[],channels:[],models:[]};
  setOptions('stVendor',f.vendors||[],st.filters.vendor,'全部厂商',v=>v,v=>v);
  setOptions('stGroup',f.groups||[],st.filters.group,'全部分组',v=>v,v=>v);
  setOptions('stChannel',f.channels||[],st.filters.channel,'全部渠道',v=>String(v.id),v=>`#${v.id} ${v.name}`);
  setOptions('stModel',f.models||[],st.filters.model,'全部模型',v=>v,v=>v);
}
function setOptions(id,rows,value,allLabel,val,label){const el=$(id);if(!el)return;el.innerHTML=`<option value="">${esc(allLabel)}</option>`+rows.map(r=>`<option value="${esc(val(r))}">${esc(label(r))}</option>`).join('');el.value=value;if(el.value!==value){el.value='';const key=id==='stVendor'?'vendor':id==='stGroup'?'group':id==='stChannel'?'channel':'model';st.filters[key]=''}}

function renderReport(){
  const d=st.report;if(!d)return;renderFilterOptions();renderSources(d.meta);const body=$('stDeliveryBody');if(!body)return;
  const cov=d.meta?.data_coverage||{},hasCoverage=typeof cov.complete==='boolean';
  if(!d.summary?.requests){if(st.chart){st.chart.dispose();st.chart=null}const detail=hasCoverage&&cov.latest_hour_pending?'最新完整小时正在汇总，完成后会自动显示。':hasCoverage&&!cov.complete?`当前有 ${nfmt(cov.missing_hours)} 个历史小时待补，也可能是所选范围确实没有流量。`:'所选范围内没有真实用户请求。';body.innerHTML=`<div class="stability-empty"><b>当前范围没有稳定性数据</b><p>${detail}</p></div>`;return}
  body.innerHTML=`<section class="stability-kpis" id="stKpis"></section>
    <section class="stability-panel"><div class="stability-panel-head"><div><h3>每日稳定性变化</h3><p>成功交付 / 真实用户请求；无请求日期断线，柱形为每日请求量</p></div><span class="muted">${esc(d.meta.from)} 至 ${esc(d.meta.to)}</span></div><div id="stTrend" class="stability-chart"></div></section>
    <section class="stability-panel"><div class="stability-panel-head"><div><h3>服务分组稳定性</h3><p>服务分组代表用户体验；展开查看实际承载渠道，未路由请求只计入分组</p></div><span class="muted">${nfmt(d.groups?.length||0)} 个有流量分组</span></div><div class="stability-group-head"><span>服务分组 / 渠道</span><span>区间稳定性 / 环比</span><span>时间窄条 · ${bucketLabel(d.meta.timeline_bucket_sec||3600)}</span><span>请求 / 占比</span><span>问题 / 问题率</span><span>渠道 / 操作</span></div><div id="stGroupList"></div></section>
    <section class="stability-ranking-grid"><article class="stability-panel"><div class="stability-panel-head"><div><h3>分组与渠道使用量排行</h3><p>按真实请求数排序，识别优先保障对象</p></div></div><div id="stUsageRank" class="stability-rank-list"></div></article><article class="stability-panel"><div class="stability-panel-head"><div><h3>渠道问题排行</h3><p>按问题数排序；数量分布不等于问题归因</p></div></div><div id="stProblemRank" class="stability-rank-list"></div></article></section>`;
  renderKpis(d);renderTrend(d.groups||[]);renderGroups();renderRankings();
  if(d.meta.rows_truncated){body.insertAdjacentHTML('afterbegin','<div class="alert">当前维度数量超过页面安全上限，列表已截断；汇总口径仍以接口返回范围为准。</div>')}
}
function renderSources(meta){
  const s=meta?.sources||{},cov=meta?.data_coverage||{},hasCoverage=typeof cov.complete==='boolean';const el=$('stSourceMini');if(!el)return;
  const problemPending=+s.problem_pending_minutes||0,problemCoverage=+s.problem_coverage_to||0;
  const problemState=problemPending?'wait':problemCoverage?'ok':'wait';
  const problemLabel=problemPending?`积压 ${nfmt(problemPending)} 分钟`:problemCoverage?`至 ${dateTime(problemCoverage)}`:'积累中';
  const migration=s.problem_migration||{},migrationStatus=migration.status||'disabled';
  const migrationVisible=!['disabled','not_required'].includes(migrationStatus);
  const migrationState=migrationStatus==='complete'?'ok':['paused','paused_disabled','error','stalled'].includes(migrationStatus)?'bad':'wait';
  const migrationETA=migration.estimate_status==='observed'&&migration.estimated_seconds!=null?` · 预计剩余 ${age(+migration.estimated_seconds)}`:
    migration.estimate_status==='backoff'?' · 退避中':migration.estimate_status==='blocked'?' · 已暂停':'';
  const migrationLabel=migrationStatus==='complete'?'错误历史重签完成':`错误历史重签 ${(+migration.percent||0).toFixed(1)}%${migrationETA}`;
  const migrationTitle=`原始错误 v5 冷历史迁移；实时错误采集使用独立高优先水位，不受该进度阻塞${migration.last_error?'；最近错误：'+migration.last_error:''}`;
  const nginxStatus=s.nginx_status||(s.nginx_connected?'ok':s.nginx_enabled?'degraded':'disabled');
  const nginxClass=nginxStatus==='ok'?'ok':nginxStatus==='degraded'?'bad':'wait';
  const nginxLabel=nginxStatus==='ok'?`${nfmt(s.nginx_healthy_sources||s.nginx_source_count)}/${nfmt(s.nginx_source_count)} 正常`:nginxStatus==='degraded'?`${nfmt(s.nginx_healthy_sources)}/${nfmt(s.nginx_source_count)} 异常`:'未启用';
  const coverageClass=!hasCoverage||cov.latest_hour_pending?'':cov.complete?'ok':'wait';
  const coverageLabel=!hasCoverage?'数据状态未知':cov.complete?`数据 ${nfmt(cov.completed_hours)}/${nfmt(cov.expected_hours)}`:cov.latest_hour_pending?`数据 ${nfmt(cov.completed_hours)}/${nfmt(cov.expected_hours)} · 汇总中`:`数据 ${nfmt(cov.completed_hours)}/${nfmt(cov.expected_hours)} · ${nfmt(cov.missing_hours)} 小时待补`;
  const pendingTime=+cov.pending_hour_ts?` ${dateTime(cov.pending_hour_ts)}`:'';
  const coverageTitle=!hasCoverage?'暂未获取小时数据状态':cov.latest_hour_pending?`最新小时${pendingTime} 正常汇总中`:cov.complete?'所选范围小时数据已完整':'存在历史小时待补，当前统计可能偏低';
  el.innerHTML=`<span><i></i>${esc(meta?.from||'—')}～${esc(meta?.to||'—')}</span><span title="${coverageTitle}"><i class="${coverageClass}"></i>${coverageLabel}</span><span title="NewAPI 本地采样新鲜度"><i class="${s.newapi_last_ts?'ok':'wait'}"></i>NewAPI ${s.newapi_last_ts?age(s.newapi_data_age_sec):'无数据'}</span><span title="原始错误采集完整覆盖时间；存在积压时问题排行暂不包含未完成分钟"><i class="${problemState}"></i>错误 ${problemLabel}</span>${migrationVisible?`<span title="${esc(migrationTitle)}"><i class="${migrationState}"></i>${esc(migrationLabel)}</span>`:''}<span title="Nginx 允许节点健康数 / 配置节点数"><i class="${nginxClass}"></i>Nginx ${nginxLabel}</span>`;
}
function renderKpis(d){const s=d.summary,p=d.previous,pc=d.meta?.comparison_coverage||{};const k=$('stKpis');if(!k)return;k.innerHTML=[
  ['区间稳定性',pct(s.stability),delta(d.delta_pp),health(s)],
  ['真实用户请求',nfmt(s.requests),`成功 ${nfmt(s.success)} · 问题 ${nfmt(s.problems)}`,''],
  ['问题请求',nfmt(s.problems),`异常 ${nfmt(s.anomaly)} · 错误 ${nfmt(s.failed)} · 未路由 ${nfmt(s.rejected)}`,s.problems?'bad':'good'],
  ['上一周期',d.meta.comparison_available?pct(p.stability):'—',d.meta.comparison_available?`${nfmt(p.requests)} 次请求`:`历史小时待补 ${nfmt(pc.missing_hours)} 个`,'']
].map(x=>`<article class="stability-kpi"><small>${x[0]}</small><b class="${x[3]||''}">${x[1]}</b><em>${x[2]}</em></article>`).join('')}
function aggregateDaily(groups){const by={};for(const g of groups)for(const d of g.daily||[]){const v=by[d.date]||(by[d.date]={date:d.date,success:0,anomaly:0,failed:0,rejected:0,requests:0});v.success+=d.success||0;v.anomaly+=d.anomaly||0;v.failed+=d.failed||0;v.rejected+=d.rejected||0;v.requests+=d.requests||0}return Object.values(by).sort((a,b)=>a.date.localeCompare(b.date)).map(v=>({...v,stability:v.requests?v.success/v.requests*100:null}))}
function renderTrend(groups){const el=$('stTrend');if(!el||!window.echarts)return;const rows=aggregateDaily(groups);if(st.chart)st.chart.dispose();st.chart=echarts.init(el);st.chart.setOption({animation:false,grid:{left:50,right:55,top:28,bottom:38},tooltip:{trigger:'axis',backgroundColor:'#151b27',borderColor:'#39445a',textStyle:{color:'#e5ebf5'},formatter:p=>{const r=rows[p[0]?.dataIndex];return r?`${esc(r.date)}<br>稳定性 ${pct(r.stability)}<br>请求 ${nfmt(r.requests)}<br>问题 ${nfmt(r.anomaly+r.failed+r.rejected)}`:''}},xAxis:{type:'category',data:rows.map(r=>r.date.slice(5)),axisLabel:{color:'#7e8a9f'},axisLine:{lineStyle:{color:'#354055'}}},yAxis:[{type:'value',min:v=>Math.max(0,Math.floor(v.min-2)),max:100,axisLabel:{color:'#7e8a9f',formatter:'{value}%'},splitLine:{lineStyle:{color:'#283143'}}},{type:'value',axisLabel:{color:'#657188'},splitLine:{show:false}}],series:[{name:'稳定性',type:'line',connectNulls:false,smooth:.25,symbol:'circle',symbolSize:5,data:rows.map(r=>r.stability==null?null:+r.stability.toFixed(3)),lineStyle:{width:2,color:'#8177ff'},itemStyle:{color:'#8177ff'},areaStyle:{color:'rgba(129,119,255,.08)'}},{name:'请求量',type:'bar',yAxisIndex:1,data:rows.map(r=>r.requests),barMaxWidth:20,itemStyle:{color:'rgba(79,153,229,.28)',borderRadius:[3,3,0,0]}}]})}

function dayStrip(points){const rows=points||[];const dense=rows.length>60?' dense':'';const unit=bucketLabel(st.report?.meta?.timeline_bucket_sec||3600);return `<div class="day-strip${dense}">${rows.map(d=>{let c='empty';if(d.requests)c=d.stability>=99?'good':d.stability>=95?'warn':'bad';return `<i class="${c}" title="${esc(dateTime(d.ts))} · ${esc(unit)} · ${d.requests?nfmt(d.requests)+' 次 · '+pct(d.stability):'无请求'}"></i>`}).join('')}</div>`}
function groupRow(g){return `<div class="stability-group-row" role="button" tabindex="0" data-st-group="${esc(g.name)}"><span class="stability-title"><i class="${health(g)}"></i><span><b>${st.expanded.has(g.name)?'▾':'▸'} ${esc(g.name)}</b><small>${esc(g.vendor||'未标记')} · ${nfmt(g.channels?.length||0)} 个实际渠道 · ${nfmt(g.model_count||g.models?.length||0)} 个模型</small></span></span><span class="stability-rate"><b>${pct(g.stability)}</b><small class="${deltaClass(g.delta_pp)}">${delta(g.delta_pp)}</small></span>${dayStrip(g.timeline)}<span class="stability-cell-num"><b>${nfmt(g.requests)}</b><small>${(+g.share_pct||0).toFixed(1)}% 全站</small></span><span class="stability-cell-num"><b>${nfmt(g.problems)}</b><small>${pct(g.problem_rate)}${g.rejected?` · 未路由 ${nfmt(g.rejected)}`:''}</small></span><span><button type="button" class="stability-open-detail" data-st-detail="group" data-group="${esc(g.name)}">完整详情 →</button></span></div>`}
function channelRow(g,ch){const state=ch.current===false?'历史渠道':ch.status===1?'当前启用':ch.status?'当前停用':'状态未知';return `<div class="stability-channel-row"><span class="stability-title"><i class="${health(ch)}"></i><span><b>#${ch.id} ${esc(ch.name)}</b><small>${esc(ch.vendor||'未标记')} · ${state} · ${nfmt(ch.model_count||ch.models?.length||0)} 个模型</small></span></span><span class="stability-rate"><b>${pct(ch.stability)}</b><small class="${deltaClass(ch.delta_pp)}">${delta(ch.delta_pp)}</small></span>${dayStrip(ch.timeline)}<span class="stability-cell-num"><b>${nfmt(ch.requests)}</b><small>${(+ch.share_pct||0).toFixed(1)}% 分组</small></span><span class="stability-cell-num"><b>${nfmt(ch.problems)}</b><small>${pct(ch.problem_rate)} · 贡献 ${(+ch.problem_share_pct||0).toFixed(1)}%</small></span><span><button type="button" class="stability-open-detail" data-st-detail="channel" data-group="${esc(g.name)}" data-channel="${ch.id}">完整详情 →</button></span></div>`}
function renderGroups(){const el=$('stGroupList');if(!el)return;const groups=st.report?.groups||[];el.innerHTML=groups.map(g=>{let detail='';if(st.expanded.has(g.name)){detail=st.detailLoading.has(g.name)?'<div class="stability-loading"><i class="stability-spinner"></i><span>正在加载该分组详情…</span></div>':g._detail_loaded?(g.channels||[]).map(ch=>channelRow(g,ch)).join('')+(g.rejected?`<div class="stability-channel-row"><span class="stability-title"><i class="bad"></i><span><b>未归属请求（明确记录）</b><small>选择渠道前结束，不属于任何真实渠道</small></span></span><span class="stability-rate"><b>—</b><small>无渠道稳定性</small></span><span></span><span class="stability-cell-num"><b>${nfmt(g.rejected)}</b><small>${g.requests?(g.rejected/g.requests*100).toFixed(2):'0.00'}% 分组请求</small></span><span class="stability-cell-num"><b>${nfmt(g.rejected)}</b><small>全部计入分组问题</small></span><span></span></div>`:''):'<div class="stability-empty"><p>详情尚未加载</p></div>'}return groupRow(g)+detail}).join('')||'<div class="stability-empty"><b>当前筛选没有有流量分组</b></div>'}
async function ensureGroupDetail(name){const current=(st.report?.groups||[]).find(g=>g.name===name);if(!current||current._detail_loaded)return current;if(st.detailPromises.has(name))return st.detailPromises.get(name);const generation=st.generation,controller=new AbortController();st.detailControllers.set(name,controller);st.detailLoading.add(name);renderGroups();const promise=(async()=>{const res=await fetch('/stability/detail?'+queryParams({group:name}),{headers:{Accept:'application/json'},signal:controller.signal});if(res.status===401){location.href='/login';return null}const data=await res.json();if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);if(generation!==st.generation)return null;data.group.share_pct=current.share_pct;data.group._detail_loaded=true;const index=(st.report?.groups||[]).findIndex(g=>g.name===name);if(index>=0)st.report.groups[index]=data.group;return data.group})().finally(()=>{if(st.detailControllers.get(name)===controller)st.detailControllers.delete(name);if(st.detailPromises.get(name)===promise)st.detailPromises.delete(name);if(generation===st.generation){st.detailLoading.delete(name);renderGroups()}});st.detailPromises.set(name,promise);return promise}
async function groupClick(e){const detail=e.target.closest('[data-st-detail]');if(detail){e.preventDefault();e.stopPropagation();await openDrawer(detail.dataset.group,+detail.dataset.channel||0);return}const row=e.target.closest('[data-st-group]');if(!row)return;const name=row.dataset.stGroup;if(st.expanded.has(name)){st.expanded.delete(name);renderGroups();return}st.expanded.add(name);renderGroups();try{await ensureGroupDetail(name)}catch(error){if(error.name==='AbortError')return;st.expanded.delete(name);renderGroups();alert(error.message||'分组详情加载失败')}}

function rankRows(rows,problem){const max=Math.max(1,...rows.map(r=>problem?r.problems:r.requests));return rows.slice(0,8).map((r,i)=>`<div class="stability-rank-row"><i>${i+1}</i><b title="${esc((r.id?'#'+r.id+' ':'')+r.name)}">${esc((r.id?'#'+r.id+' ':'')+r.name)}</b><span class="rank-bar"><i style="width:${Math.max(2,(problem?r.problems:r.requests)/max*100)}%;${problem?'background:#e45b69':''}"></i></span><strong>${nfmt(problem?r.problems:r.requests)}</strong><small>${problem?pct(r.problem_rate):(+r.share_pct||0).toFixed(1)+'%'}</small></div>`).join('')||'<div class="stability-empty"><p>当前范围暂无数据</p></div>'}
function renderRankings(){const r=st.report?.rankings||{};$('stUsageRank').innerHTML=`<div class="stability-rank-label">服务分组</div>${rankRows(r.groups||[],false)}<div class="stability-rank-label">实际渠道</div>${rankRows(r.channels||[],false)}`;$('stProblemRank').innerHTML=rankRows([...(r.channels||[])].sort((a,b)=>b.problems-a.problems),true)}

function findEntity(groupName,channelID){const g=(st.report?.groups||[]).find(x=>x.name===groupName);if(!g)return null;if(channelID)return {kind:'channel',group:g,entity:(g.channels||[]).find(x=>x.id===channelID)};return {kind:'group',group:g,entity:g}}
async function openDrawer(groupName,channelID){try{await ensureGroupDetail(groupName)}catch(error){if(error.name==='AbortError')return;alert(error.message||'详情加载失败');return}const found=findEntity(groupName,channelID);if(!found?.entity)return;st.lastFocus=document.activeElement;st.drawer=found;st.drawerTab='run';$('stDrawerMask')?.classList.add('open');$('stDrawer')?.classList.add('open');$('stDrawer')?.setAttribute('aria-hidden','false');document.body.style.overflow='hidden';renderDrawer();$('stDrawerClose')?.focus()}
function closeDrawer(){if(st.drawerAbort){st.drawerAbort.abort();st.drawerAbort=null}st.drawer=null;$('stDrawerMask')?.classList.remove('open');$('stDrawer')?.classList.remove('open');$('stDrawer')?.setAttribute('aria-hidden','true');document.body.style.overflow='';if(st.drawerChart){st.drawerChart.dispose();st.drawerChart=null}if(st.lastFocus?.focus)st.lastFocus.focus();st.lastFocus=null}
function renderDrawer(){if(!st.drawer)return;const x=st.drawer.entity;const title=st.drawer.kind==='channel'?`#${x.id} ${x.name}`:x.name;$('stDrawerTitle').textContent=title;$('stDrawerSubtitle').textContent=`${st.drawer.kind==='channel'?'上游渠道 · '+st.drawer.group.name:'服务分组'} · ${st.report.meta.from} 至 ${st.report.meta.to}`;document.querySelectorAll('[data-st-drawer-tab]').forEach(b=>b.classList.toggle('active',b.dataset.stDrawerTab===st.drawerTab));const body=$('stDrawerBody');if(st.drawerTab==='problems'){renderDrawerProblems();return}const nav={days:st.days,group:st.drawer.group.name};if(st.custom){nav.from=st.custom.from;nav.to=st.custom.to;delete nav.days}if(st.drawer.kind==='channel')nav.channel=x.id;const finance=st.drawer.kind==='channel'?`<div class="monitor-cross-actions"><button type="button" onclick="monitorOpenEncoded('channels','${encodeNav(nav)}')">查看使用与倍率配置 →</button></div>`:'';body.innerHTML=`${finance}<section class="stability-drawer-kpis"><div><small>区间稳定性</small><b>${pct(x.stability)}</b></div><div><small>真实用户请求</small><b>${nfmt(x.requests)}</b></div><div><small>问题请求</small><b>${nfmt(x.problems)}</b></div><div><small>环比变化</small><b class="${deltaClass(x.delta_pp)}">${x.delta_pp==null?'—':(x.delta_pp>=0?'+':'')+x.delta_pp.toFixed(2)+' pp'}</b></div></section><section class="stability-panel-head" style="border:1px solid #30394b;border-bottom:0;border-radius:10px 10px 0 0"><div><h3>每日稳定性曲线</h3><p>仅还原当前所选对象的每日事实</p></div></section><div id="stDrawerChart" class="stability-chart"></div><section class="stability-model-list"><div class="stability-panel-head"><div><h3>${st.drawer.kind==='group'?'模型表现':'承载模型'}</h3><p>按真实请求量排序</p></div></div>${(x.models||[]).map(m=>`<div class="stability-model-row"><b>${esc(m.name)}</b><span>${pct(m.stability)}</span><small>${nfmt(m.requests)} 请求</small><small>${nfmt(m.problems)} 问题</small></div>`).join('')||'<div class="stability-empty"><p>当前范围无模型数据</p></div>'}</section>`;renderDrawerChart(x.daily||[])}
function renderDrawerChart(days){const el=$('stDrawerChart');if(!el||!window.echarts)return;if(st.drawerChart)st.drawerChart.dispose();st.drawerChart=echarts.init(el);st.drawerChart.setOption({animation:false,grid:{left:48,right:22,top:26,bottom:35},tooltip:{trigger:'axis'},xAxis:{type:'category',data:days.map(d=>d.date.slice(5)),axisLabel:{color:'#778399'},axisLine:{lineStyle:{color:'#354055'}}},yAxis:{type:'value',min:v=>Math.max(0,Math.floor(v.min-2)),max:100,axisLabel:{color:'#778399',formatter:'{value}%'},splitLine:{lineStyle:{color:'#283143'}}},series:[{type:'line',smooth:.25,connectNulls:false,data:days.map(d=>d.stability),lineStyle:{color:'#8177ff',width:2},itemStyle:{color:'#8177ff'},areaStyle:{color:'rgba(129,119,255,.08)'}}]})}
async function renderDrawerProblems(){const body=$('stDrawerBody');if(!st.drawer)return;if(st.drawerAbort)st.drawerAbort.abort();st.drawerAbort=new AbortController();const current=st.drawer;loading(body,'正在读取该对象的原始错误分布…');const extra={group:current.group.name};if(current.kind==='channel')extra.channel=current.entity.id;try{const res=await fetch('/stability/problems?'+queryParams(extra),{headers:{Accept:'application/json'},signal:st.drawerAbort.signal});const d=await res.json();if(!res.ok)throw new Error(d.error||`HTTP ${res.status}`);if(st.drawer===current)body.innerHTML=problemTable(d,true)}catch(e){if(e.name!=='AbortError'&&st.drawer===current)errorBox(body,e.message)}}

async function loadProblems(){if(st.view!=='problems')return;if(st.problemAbort)st.problemAbort.abort();st.problemAbort=new AbortController();loading($('stProblemBody'),'正在聚合原始错误签名…');try{const res=await fetch('/stability/problems?'+queryParams(),{headers:{Accept:'application/json'},signal:st.problemAbort.signal});if(res.status===401){location.href='/login';return}const d=await res.json();if(!res.ok)throw new Error(d.error||`HTTP ${res.status}`);$('stProblemBody').innerHTML=problemTable(d,false)}catch(e){if(e.name!=='AbortError')errorBox($('stProblemBody'),e.message)}}
function problemTable(d,compactMode){const rows=d.problems||[];let coverage='';if(+d.pending_minutes>0)coverage+=`<div class="alert">原始错误采集仍有 ${nfmt(d.pending_minutes)} 个分钟待处理；下列排行只包含已完整采集的分钟，不会把部分数据当成完整结果。</div>`;if(+d.uncovered_minutes>0)coverage+=`<div class="alert">当前日期范围有 ${nfmt(d.uncovered_minutes)} 个分钟尚无原始错误采集覆盖；该功能从部署后增量积累，不能据此判断未覆盖时段没有错误。</div>`;if(!rows.length)return `${coverage}<div class="stability-empty"><b>当前范围没有已采集的原始错误</b><p>问题签名从新版本部署后开始增量积累；这不表示历史范围一定从未发生过错误。</p></div>`;return `${coverage}${!compactMode?`<div class="stability-advice-pending"><b>分析边界：</b>下面直接展示 error code 和 error message 原文聚合，不自动归类、不判断责任方。“可能原因”和“沟通与验证”知识库尚待人工确认，因此当前不生成建议。</div>`:''}<div class="stability-problem-list"><div class="stability-problem-head"><span>来源 / error code</span><span>error message 原文</span><span>影响范围</span><span style="text-align:right">数量</span><span>最后出现</span></div>${rows.map(p=>`<div class="stability-problem-row"><span><b class="code">${esc(p.code||'无明确 code')}</b><small>${esc(p.source)}</small></span><code>${esc(p.message||'(空)')}${p.truncated?' …（原文过长，已明确截断）':''}</code><span><small>${esc((p.groups||[]).slice(0,3).join('、')||'—')}<br>${(p.channel_ids||[]).slice(0,4).map(id=>'#'+id).join('、')||'未归属渠道'}</small></span><span class="count">${nfmt(p.count)}</span><small>${dateTime(p.last_ts)}</small></div>`).join('')}</div>${d.truncated?'<div class="alert">问题类型超过接口安全上限，仅显示数量最高的部分。</div>':''}`}

document.addEventListener('DOMContentLoaded',()=>{if($('tab-stability')&&!st.inited)init()});
})();
