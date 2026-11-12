(function(){
'use strict';
const state={inited:false,active:false,report:null,open:new Set(),users:new Map(),userQueries:new Map(),abort:null,generation:0,timer:null,refreshError:'',order:new Map()};
const $=id=>document.getElementById(id);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number=(n,d=0)=>typeof n==='number'&&Number.isFinite(n)?n.toLocaleString('zh-CN',{maximumFractionDigits:d}):'—';
const seconds=n=>typeof n==='number'?number(n/1000,3)+' 秒':'不可计算';
const percent=n=>typeof n==='number'?number(n,1)+'%':'不可计算';
const dt=ts=>+ts?new Date(+ts*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'未确认';
const labels={pause:'建议暂停',increase:'建议增加渠道',attention:'继续观察',no_requests:'近期没有请求',low_sample:'有效样本不足',stale:'采集落后',pending:'暂无法判断',unchanged:'不用调整'};
const ruleLabels={frt_attention:'FRT 一般关注',frt_severe:'FRT 显著偏慢',fault_attention:'渠道故障关注',pressure:'请求压力上升',unavailable:'明确不可用',capacity_limit:'明确容量限制',group_slow:'整组共同变慢',shared_failure:'共同故障来源'};
const badge=(status,text)=>`<span class="gg-badge ${esc(status)}">${esc(text||labels[status]||status)}</span>`;
const key=(...parts)=>JSON.stringify(parts);
const domID=k=>'gg-'+Array.from(new TextEncoder().encode(k),v=>v.toString(16).padStart(2,'0')).join('');
const disclosure=(k,title,body,cls='')=>`<details id="${domID(k)}" data-gg-key="${esc(k)}" class="${cls}" ${state.open.has(k)?'open':''}><summary>${title}</summary>${state.open.has(k)?body():''}</details>`;

window.groupGovernanceActivate=function(){if(!state.inited)init();state.active=true;load();schedule()};
window.groupGovernanceDeactivate=function(){state.active=false;++state.generation;state.abort?.abort();clearTimeout(state.timer);state.timer=null};
function schedule(){clearTimeout(state.timer);if(state.active&&!document.hidden)state.timer=setTimeout(()=>{load();schedule()},30000)}
function resume(){if(state.active&&!document.hidden){load();schedule()}}
function init(){
 state.inited=true;$('ggRefresh')?.addEventListener('click',load);
 for(const id of ['ggSearch','ggStatus','ggRatioName','ggNoChannel','ggNoRequest'])$(id)?.addEventListener(id==='ggSearch'?'input':'change',render);
 window.addEventListener('focus',resume);document.addEventListener('visibilitychange',()=>{if(document.hidden)clearTimeout(state.timer);else resume()});
 $('ggRows').addEventListener('toggle',e=>{const el=e.target,k=el.dataset?.ggKey;if(!k||!el.isConnected||state.open.has(k)===el.open)return;if(el.open)state.open.add(k);else state.open.delete(k);render()},true);
 $('ggRows').addEventListener('click',e=>{
  const request=e.target.closest('[data-gg-request]');if(request){e.preventDefault();openTroubleshooting(JSON.parse(request.dataset.ggRequest));return}
  const users=e.target.closest('[data-gg-users]');if(users){e.preventDefault();loadUsers(users.dataset.ggUsers,+(users.dataset.offset||0))}
 });
}
async function load(){
 if(!state.active)return;state.abort?.abort();const controller=new AbortController(),generation=++state.generation;state.abort=controller;
 $('ggRows').setAttribute('aria-busy','true');
 if(!state.report)$('ggRows').innerHTML='<p class="gg-empty">正在读取本地指标与配置快照…</p>';
 try{
  const response=await fetch('/group-governance/report',{headers:{Accept:'application/json'},cache:'no-store',signal:controller.signal});
  if(generation!==state.generation||!state.active)return;
  if(response.status===401){location.href='/login';return}
  const data=await response.json();if(generation!==state.generation||!state.active)return;if(!response.ok)throw Error(data.error||`HTTP ${response.status}`);
  state.report=data;state.refreshError='';render(true);
 }catch(e){if(e.name!=='AbortError'&&generation===state.generation&&state.active)preserveView(()=>{state.refreshError=e.message;renderHeader()})}
 finally{if(generation===state.generation)$('ggRows').setAttribute('aria-busy','false')}
}
function renderHeader(){
 const r=state.report||{},s=r.state||{},rt=r.realtime||{},enabled=r.enabled!==false;
 $('ggTotal').textContent=s.last_success_at?number(s.current_group_count):'—';
 const ready=adviceKnown();
 $('ggAdjust').textContent=ready?number(rt.adjustment_count):'暂不可判断';$('ggPause').textContent=ready?number(rt.pause_count):'—';$('ggIncrease').textContent=ready?number(rt.increase_count):'—';
 $('ggUpdated').textContent=`指标截止 ${dt(rt.metrics_through_ts)} · 配置 ${dt(rt.config_snapshot_at||s.last_success_at)} · 结果 ${dt(rt.generated_at)}`;
 $('ggCoverage').textContent=`最近 15 个完整分钟，低样本渠道扩大至 30 分钟；采集定稿等待 ${number(rt.finalize_delay_seconds)} 秒，数据截止与当前时间相差 ${rt.metrics_through_ts?number(Math.max(0,Date.now()/1000-rt.metrics_through_ts)):'—'} 秒。页面可见时每 30 秒读本地结果。`;
 $('ggExport').setAttribute('aria-disabled',String(!s.last_success_at));$('ggExport').style.pointerEvents=s.last_success_at?'':'none';
 const notes=[];if(!enabled)notes.push('分组治理尚未启用，当前不会生成调整建议。');else if(!s.last_success_at)notes.push('当前配置尚未同步；分组总数不可确认。');
 if(state.refreshError)notes.push('刷新失败，保留上次结果：'+state.refreshError);else if(ready&&rt.adjustment_count===0)notes.push('当前已完成判断，暂无需要执行的调整事项；仍需观察或缺证的渠道在下方单列，不代表全部正常。');else if(!ready)notes.push('建议暂不可判断；已有指标和故障仍保留，请查看配置及覆盖状态。');
 if(rt.config_ready===false)notes.push('渠道配置尚未更新，暂不能确认调整范围。');
 if(s.last_error)notes.push(s.last_error);if(rt.note)notes.push(rt.note);if(!rt.generated_at)notes.push('近期指标等待后台首轮计算。');
 $('ggAlert').hidden=!notes.length;$('ggAlert').className='gg-alert'+(state.refreshError?' bad':'');$('ggAlert').textContent=notes.join(' ');
}
function adviceKnown(){const rt=state.report?.realtime;return !!rt?.generated_at&&rt.status==='ready'&&rt.advice_ready!==false&&!state.refreshError}
function groups(){
 const by=new Map((state.report?.groups||[]).map(g=>[g.group,{...g,metrics:[],actions:[]}])) ;
 for(const m of state.report?.realtime?.channels||[]){if(!by.has(m.group))by.set(m.group,{group:m.group,current:false,historical_only:true,channels:[],metrics:[],actions:[]});by.get(m.group).metrics.push(m)}
 for(const g of by.values())g.actions=(state.report?.realtime?.actions||[]).filter(a=>a.group===g.group||(a.scope||[]).some(s=>s.startsWith(g.group+' / ')));
 for(const g of by.values())if(!state.order.has(g.group))state.order.set(g.group,state.order.size);
 return Array.from(by.values()).sort((a,b)=>priority(a.metrics,a.actions)-priority(b.metrics,b.actions)||state.order.get(a.group)-state.order.get(b.group));
}
function priority(metrics,actions=[]){
 if(actions.some(a=>a.kind==='pause')||metrics.some(m=>m.status==='pause'))return 0;
 if(actions.some(a=>a.kind==='increase')||metrics.some(m=>m.status==='increase'))return 1;
 if((!metrics.length&&!state.report?.realtime?.requests_complete)||metrics.some(m=>['attention','pending','stale','low_sample'].includes(m.status)||!m.requests_complete))return 2;
 if(metrics.some(m=>m.requests>0))return 3;
 return 4;
}
function idleText(metrics){const windows=[...new Set(metrics.map(m=>m.window_minutes).filter(Boolean))];return windows.length===1?`最近${number(windows[0])}分钟无请求`:'当前完整采集窗口无请求'}
function groupStatus(g){const rank=priority(g.metrics,g.actions);if(rank===0)return 'pause';if(rank===1)return 'increase';if(rank===2)return g.metrics.some(m=>m.status==='attention')?'attention':'pending';if(rank===3)return 'unchanged';return (g.metrics.length&&g.metrics.every(m=>m.requests_complete)||!g.metrics.length&&state.report?.realtime?.requests_complete)?'no_requests':'pending'}
function filtered(){const q=($('ggSearch')?.value||'').trim().toLowerCase(),status=$('ggStatus')?.value||'';return groups().filter(g=>(!q||[g.group,g.display_name,...g.metrics.map(m=>`${m.channel_name} ${m.channel_id} ${(m.models||[]).join(' ')}`)].join(' ').toLowerCase().includes(q))&&(!status||groupStatus(g)===status||g.metrics.some(m=>m.status===status))&&(!$('ggRatioName')?.checked||g.name_has_ratio)&&(!$('ggNoChannel')?.checked||g.enabled_channels===0)&&(!$('ggNoRequest')?.checked||g.metrics.length>0&&g.metrics.every(m=>m.requests_complete&&m.requests===0)))}
function preserveView(update){if(window.MonitorView)window.MonitorView.preserve($('ggRows'),update);else update()}
function render(refreshHeader=false){if(!state.report)return;const update=()=>{if(refreshHeader===true)renderHeader();const rows=filtered();$('ggVisible').textContent=`显示 ${number(rows.length)} / ${number(groups().length)} 个分组`;$('ggRows').innerHTML=rows.length?rows.map(groupHTML).join(''):'<p class="gg-empty">没有符合当前筛选的分组；配置与数据未就绪时不视为正常。</p>';for(const [g]of state.users)renderUsers(g)};preserveView(update)}
function groupHTML(g){
 const status=groupStatus(g),k=key('group',g.group),title=`<span class="gg-title"><b>${esc(g.group||'未记录分组')}</b><small>${esc(g.display_name||'')}${g.historical_only?' · 历史请求分组':''}</small></span><span>${number(new Set(g.metrics.map(m=>m.channel_id)).size||g.channels?.length||0)} 个渠道</span>${badge(status,status==='no_requests'?idleText(g.metrics):'')}<span class="gg-muted">${adviceKnown()?number(g.actions.length)+' 项关联调整':'调整数暂不可判断'}</span>`;
 return disclosure(k,title,()=>{
  const channels=new Map((g.channels||[]).map(c=>[c.id,{...c,metric:null}]));for(const m of g.metrics){channels.set(m.channel_id,{id:m.channel_id,name:m.channel_name,status:m.channel_status,metric:m})}
  const rank=c=>c.metric?priority([c.metric]):2;
  return `<div class="gg-channels">${Array.from(channels.values()).sort((a,b)=>rank(a)-rank(b)||a.id-b.id).map(c=>channelHTML(g,c)).join('')||'<p class="gg-empty">当前配置没有关联渠道。</p>'}${auditHTML(g)}</div>`;
 },'gg-group-row');
}
function channelHTML(g,c){
 const m=c.metric,actions=g.actions.filter(a=>m?.actions?.includes(a.id)).sort((a,b)=>(a.kind==='pause'?0:1)-(b.kind==='pause'?0:1)||a.id.localeCompare(b.id));
 const stateBadges=actions.length?Array.from(new Set(actions.map(a=>a.kind))).map(s=>badge(s)).join(' '):badge(m?.status||'pending',m?.status==='no_requests'?idleText([m]):'');
 return disclosure(key('channel',g.group,c.id),`<span class="gg-title"><b>#${number(c.id)} ${esc(c.name||'未命名渠道')}</b><small>${+c.status===1?'启用':+c.status===3?'自动禁用':'停用/未知'} · ${m?`最近${number(m.window_minutes)}分钟 ${number(m.requests)} 次尝试 · ${number(m.frt?.observed)} 个有效首字样本`:'近期指标尚未就绪'} · ${adviceKnown()?number(actions.length)+' 项关联调整':'调整数暂不可判断'}</small></span><span class="gg-right">${stateBadges}</span>`,()=>m?channelDetails(m):'<p class="gg-empty">本地指标尚未包含此渠道，暂不能确认请求量和服务质量。</p>','gg-channel-row');
}
function metric(name,value,note=''){return `<div class="gg-metric"><small>${esc(name)}</small><b>${esc(value)}</b>${note?`<span>${esc(note)}</span>`:''}</div>`}
function actionHTML(a){return `<article class="gg-action ${esc(a.kind)}"><h4>${badge(a.kind)} ${esc((a.subtypes||[a.subtype]).map(s=>({capacity:'补充容量',backup:'增加独立备用',channel:'暂停整个渠道'}[s]||s)).join('、'))}</h4><p>${esc(a.reason)}</p><p><b>调整范围：</b>${esc((a.scope||[]).join('；'))}。${esc(a.execution||'由管理员在中转站手动操作')}</p>${(a.triggers||[]).map(t=>`<p class="gg-trigger"><b>#${number(t.channel_id)} ${esc(t.channel_name)} · ${esc(t.model)}</b>：${esc(t.rule?.description||t.rule?.reason||'触发依据尚未更新')}<small>${dt(t.from_ts)} 至 ${dt(t.to_ts)}</small></p>`).join('')}<small>事项 ${esc(a.id)}；各关联位置展示同一事项，顶部只计一次。</small></article>`}
function channelDetails(m){
 const f=m.frt||{},d=m.decision||{},gaps=d.gaps||[];
 return `<div class="gg-channel-body">
 <p class="gg-window">${dt(m.from_ts)} 至 ${dt(m.to_ts)}（右边界不含） · 实际数据截止 ${dt(m.to_ts)} · 本分组请求尝试，包含失败重试。</p>
 ${m.window_reason?`<p class="gg-window">${esc(m.window_reason)}</p>`:''}
 <div class="gg-core-grid">
  <section class="gg-core">${metric('首字耗时',seconds(f.mean_ms))}<p>${esc(d.frt||'首字样本或同类参照尚未就绪，暂不能比较速度')}</p></section>
  <section class="gg-core">${metric('已确认渠道／上游错误',number(m.attributed_errors)+' 次 · '+percent(m.fault_rate_pct))}<p>${esc(d.errors||'错误归责尚未就绪，已发现错误仍保留')}</p></section>
  <section class="gg-core">${metric('请求量',`最近${number(m.window_minutes)}分钟 ${number(m.requests)} 次`)}<p>${esc(d.traffic||'尚无足够正常数据，暂不能计算增长')}</p></section>
 </div>
 <p class="gg-advice ${esc(m.status)}"><strong>${esc(d.label||labels[m.status]||'暂无法判断')}：</strong>${esc(d.reason||'判断依据尚未完整，暂不调整渠道')}</p>
 ${gaps.length?`<p class="gg-gaps"><b>影响判断的缺项：</b>${gaps.map(esc).join('；')}。</p>`:''}
 ${disclosure(key('basis',m.key),'<span>查看详细依据</span>',()=>channelEvidence(m),'gg-evidence')}
 </div>`;
}
function channelEvidence(m){
 const f=m.frt||{},b=m.baseline||{},actions=(state.report?.realtime?.actions||[]).filter(a=>m.actions?.includes(a.id));
 const errors=(m.error_reasons||[]).map(r=>`<tr><td>${esc(r.reason)}<small class="gg-sub">${esc(r.rule_id)} · ${esc(r.code||'未记录错误码')}</small></td><td>${esc(r.responsibility)}</td><td>${number(r.count)}${r.pending?'（待判）':''}</td><td>${(r.samples||[]).map(s=>s.request_id?`<button type="button" class="gg-link" data-gg-request="${esc(JSON.stringify({...s,channel_id:m.channel_id,model:s.model||'',group:m.group}))}">${esc(s.request_id)}</button><small class="gg-sub">log #${number(s.log_id)} · ${dt(s.created_at)}</small>`:`<small>log #${number(s.log_id)} · 未记录 Request ID</small>`).join('')}</td></tr>`).join('');
 return `<div class="gg-detail-box">
 <section class="gg-detail-card gg-wide"><h3>调整事项与影响范围</h3>${actions.map(actionHTML).join('')||'<p>当前没有可执行的调整事项，已知故障和缺项见当前说明。</p>'}<p>当前分组 ${esc(m.group)}，渠道 #${number(m.channel_id)} ${esc(m.channel_name)}；关联能力：${esc((m.models||[]).join('、')||'未记录')}。实际暂停范围为整个渠道。</p></section>
 <section class="gg-detail-card gg-wide"><h3>渠道汇总与正常基准</h3><div class="gg-metrics">${metric('请求尝试总数',number(m.requests)+' 次')}${metric('平均请求强度','每分钟 '+number(m.rpm,2)+' 次')}${metric('峰值请求强度','每分钟 '+number(m.peak_rpm)+' 次','同一分钟跨模型先合计，再取最大值')}${metric('待判错误',number(m.pending_errors)+' 次')}${metric('其他责任错误',number(m.other_errors)+' 次')}${metric('本分组流量占比',percent(m.share_pct))}${metric('有效首字样本',number(f.observed)+' 次')}${metric('同模型可比较样本',number(m.compared_samples)+' 次')}${metric('正常基准',b.rpm==null?'尚无足够正常数据':'每分钟 '+number(b.rpm,2)+' 次')}</div><p>基准 ${dt(b.from_ts)} 至 ${dt(b.to_ts)}，${number(b.minutes)} 个正常分钟；${esc(b.source||'尚未建立')}。</p>${b.frozen?`<p>基准冻结：${esc(b.freeze_reason)}。${esc(b.resume_condition)}</p>`:'<p>正常分钟继续更新基准；流量增长本身不会冻结。</p>'}</section>
 <section class="gg-detail-card gg-wide"><h3>同模型比较与连续评估</h3><p>后台按同模型比较，再按当前渠道的有效样本构成汇总参照；不直接比较不同模型构成的渠道均值。规则版本 ${esc(state.report?.realtime?.rule_version)}。</p>${(m.evidence||[]).map(e=>`<div class="gg-rule"><h4>${esc(e.model)}</h4><p>实际平均 ${seconds(e.mean_ms)}，有效样本 ${number(e.observed)}；同模型可比渠道 ${number(e.comparison?.count)} 条。渠道平均耗时的 P50 ${seconds(e.comparison?.p50_ms)}、P80 ${seconds(e.comparison?.p80_ms)}、P90 ${seconds(e.comparison?.p90_ms)}。</p>${(e.rules||[]).map(r=>`<p><b>${esc(ruleLabels[r.id]||r.id)}：</b>${esc(r.description||r.reason)}</p>`).join('')}<small>${dt(e.from_ts)} 至 ${dt(e.to_ts)}</small></div>`).join('')||'<p>近期没有有效请求可进行同模型比较。</p>'}<p>渠道内单次请求耗时 P50 ${seconds(f.p50_ms)}、P95 ${seconds(f.p95_ms)}（直方图估算），不是渠道平均耗时的分位线。超过3秒 ${number(f.over_3s)}次（${percent(f.over_3s_pct)}），超过5秒 ${number(m.over_5s)}次（${percent(m.over_5s_pct)}），超过10秒 ${number(m.over_10s)}次（${percent(m.over_10s_pct)}）。</p></section>
 <section class="gg-detail-card gg-wide"><h3>具体错误与排障样本</h3><p>${m.errors_complete?'错误数量与同窗请求尝试已核对':'归责覆盖未完成，已发现错误保留，完整故障率暂不可计算'}。</p><div class="gg-scroll"><table><thead><tr><th>原因／错误码</th><th>责任方</th><th>次数</th><th>留存样本，点击仅预填排障</th></tr></thead><tbody>${errors||'<tr><td colspan="4">未取得具体错误，是否完整以覆盖状态为准。</td></tr>'}</tbody></table></div></section>
 <section class="gg-detail-card gg-wide"><h3>替代渠道与承接检查</h3>${(m.evidence||[]).filter(e=>(e.alternatives||[]).length||(e.checks||[]).length).map(e=>`<p><b>${esc(e.model)}：</b>${(e.alternatives||[]).map(a=>`#${number(a.channel_id)} ${esc(a.name)}，近期成功 ${number(a.success)} 次，平均 ${seconds(a.mean_ms)}；当前每分钟 ${number(a.rpm,2)} 次，保守承接线 ${number(a.proven_rpm,2)} 次，余量 ${number(a.headroom_rpm,2)} 次。${esc(a.basis)}`).join('；')||'暂无已确认可承接的渠道'}。${(e.checks||[]).map(esc).join('；')}</p>`).join('')||'<p>当前没有完成承接核验的替代方案，不能仅凭渠道启用就建议暂停。</p>'}</section>
 <section class="gg-detail-card gg-wide"><h3>分钟趋势</h3><div class="gg-scroll"><table><thead><tr><th>分钟</th><th>请求尝试</th><th>平均首字</th><th>有效样本</th><th>已确认错误</th><th>待判</th></tr></thead><tbody>${(m.trend||[]).map(p=>`<tr><td>${dt(p.at)}</td><td>${number(p.requests)}</td><td>${seconds(p.mean_ms)}</td><td>${number(p.observed)}</td><td>${number(p.errors)}</td><td>${number(p.pending)}</td></tr>`).join('')}</tbody></table></div></section>
 <section class="gg-detail-card gg-wide"><h3>数据状态与来源</h3><p>请求截止 ${dt(m.requests_through_ts)}（${m.requests_complete?'覆盖完整':'覆盖不足'}）；首字来源截止 ${dt(m.frt_through_ts)}（${m.frt_source_complete?'采集覆盖完整':'覆盖不足'}）；错误归责截止 ${dt(m.errors_through_ts)}，错误投影采集于 ${dt(m.error_captured_at)}；配置快照 ${dt(m.config_at)}。</p><p>请求及首字来自本地 capacity_user_minute_samples；归责来自同批日志的客户排障规则投影。无首字样本与采集缺口分别判断；当前配置只用于确认操作范围。</p></section>
 </div>`;
}

function auditHTML(g){return disclosure(key('audit',g.group),'<span>原有配置审计与用户引用</span>',()=>`<div class="gg-detail-card"><p>配置出处：${esc((g.config_sources||[]).join('、')||'未确认')}。${esc((g.issues||[]).join('；'))}</p><p>当前倍率：${g.ratio_configured?number(g.ratio,6):'未确认'}；用户 ${state.report?.config_only?'未采集':number(g.user_count)}；显式令牌 ${state.report?.config_only?'未采集':number(g.explicit_token_count)}；7 / 30 天请求 ${state.report?.config_only?'未采集':number(g.requests_7d)} / ${state.report?.config_only?'未采集':number(g.requests_30d)}。</p><p>${esc((state.report?.source_errors||[]).join('；'))}</p><div class="gg-user-tools"><input id="${userID(g.group,'q')}" type="search" aria-label="搜索关联用户" placeholder="用户 ID、用户名或显示名"><button class="gg-btn" data-gg-users="${esc(g.group)}" ${state.report?.config_only?'disabled title="当前来源模式未采集关联用户"':''}>读取本地关联用户</button></div><div id="${userID(g.group,'body')}"></div><p>${(g.subscriptions||[]).map(p=>`#${number(p.id)} ${esc(p.title)}（${p.enabled?'在售':'停用'}）`).join('；')||'订阅引用未发现或未核验，见配置状态。'}</p></div>`,'gg-audit')}
function userID(g,s){return domID(key('users',g,s))}
async function loadUsers(group,offset){if(state.report?.config_only)return;const q=$(userID(group,'q'))?.value||'',seq=(state.userQueries.get(group)||0)+1;state.userQueries.set(group,seq);try{const res=await fetch('/group-governance/users?'+new URLSearchParams({group,q,offset:String(offset),limit:'20'}),{headers:{Accept:'application/json'}});const data=await res.json();if(state.userQueries.get(group)!==seq)return;if(!res.ok)throw Error(data.error||`HTTP ${res.status}`);state.users.set(group,data);renderUsers(group)}catch(e){const body=$(userID(group,'body'));if(body&&state.userQueries.get(group)===seq)body.textContent='读取失败：'+e.message}}
function renderUsers(g){const data=state.users.get(g),body=$(userID(g,'body'));if(!body||!data)return;body.innerHTML=(data.users||[]).map(u=>`<div class="gg-list-item">#${number(u.user_id)} ${esc(u.username)} ${esc(u.display_name)} · ${+u.status===1?'启用':'禁用'}</div>`).join('')+`<div class="gg-pages">${number(data.offset)} / ${number(data.total)}${data.offset>0?`<button class="gg-btn" data-gg-users="${esc(g)}" data-offset="${Math.max(0,data.offset-data.limit)}">上一页</button>`:''}${data.offset+(data.users||[]).length<data.total?`<button class="gg-btn" data-gg-users="${esc(g)}" data-offset="${data.offset+data.limit}">下一页</button>`:''}</div>`}
function openTroubleshooting(s){const parts=new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false}).format(new Date(s.created_at*1000)).split(' ');window.monitorNavigate?.('logchain',{preset:'group_governance',request_id:s.request_id,channel_id:String(s.channel_id),model:s.model,group:s.group,date:parts[0],to_date:parts[0],from_time:parts[1],to_time:parts[1],scope:'err_anom'})}
})();
