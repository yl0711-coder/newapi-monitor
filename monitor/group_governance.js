(function(){
'use strict';
const state={inited:false,loaded:false,report:null,expanded:new Set(),users:new Map(),abort:null};
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=n=>(+n||0).toLocaleString('zh-CN');
const dt=ts=>+ts?new Date(+ts*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'—';
const statusText={high:'高风险',pending:'待处理',observe:'观察',normal:'正常'};
const userStatus=s=>+s===1?'启用':'禁用';
const userRole=r=>+r>=100?'超级管理员':+r>=10?'管理员':'普通用户';
const sourceText=s=>({
  'GroupRatio':'倍率配置','UserUsableGroups':'用户可选配置','AutoGroups':'自动分组',
  'GroupGroupRatio:user':'用户分组倍率（用户组）','GroupGroupRatio:using':'用户分组倍率（使用组）',
  'channels.group':'渠道分组','users.group':'用户主分组','tokens.group':'显式令牌分组',
  'subscription_plans.upgrade_group':'订阅套餐目标分组','TopupGroupRatio':'充值分组倍率',
  'ModelRequestRateLimitGroup':'分组模型限流'
}[s]||s.replace('group_ratio_setting.group_special_usable_group','特殊可用分组'));

window.groupGovernanceActivate=function(){if(!state.inited)init();if(!state.loaded)load()};
window.groupGovernanceDeactivate=function(){if(state.abort)state.abort.abort()};

function init(){
  state.inited=true;
  $('ggRefresh')?.addEventListener('click',load);
  for(const id of ['ggSearch','ggStatus','ggRatioName','ggNoChannel','ggNoRequest'])$(id)?.addEventListener(id==='ggSearch'?'input':'change',render);
  $('ggRows')?.addEventListener('click',e=>{const row=e.target.closest('[data-gg-group]');if(!row)return;toggle(row.dataset.ggGroup)});
  $('ggRows')?.addEventListener('keydown',e=>{if((e.key==='Enter'||e.key===' ')&&e.target.matches('[data-gg-group]')){e.preventDefault();toggle(e.target.dataset.ggGroup)}});
}

async function load(){
  if(state.abort)state.abort.abort();state.abort=new AbortController();
  $('ggRows').innerHTML='<tr><td colspan="10" class="gg-empty">正在读取 Monitor 本地分组快照…</td></tr>';
  try{
    const res=await fetch('/group-governance/report',{headers:{Accept:'application/json'},signal:state.abort.signal});
    if(res.status===401){location.href='/login';return}const data=await res.json();if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    state.report=data;state.loaded=true;renderHeader();render();
  }catch(e){if(e.name!=='AbortError')$('ggRows').innerHTML=`<tr><td colspan="10" class="gg-empty">读取失败：${esc(e.message)}</td></tr>`}
}

function renderHeader(){
  const r=state.report||{},s=r.state||{},enabled=r.enabled!==false;
  const canExport=enabled&&!!s.last_success_at;$('ggExport').setAttribute('aria-disabled',String(!canExport));$('ggExport').style.pointerEvents=canExport?'':'none';if(canExport)$('ggExport').href='/group-governance/export.csv';else $('ggExport').removeAttribute('href');
  $('ggUpdated').textContent=s.last_success_at?`数据更新于 ${dt(s.last_success_at)}${s.complete?'':' · 数据可能过期'}`:'尚未产生可靠快照';
  const alert=$('ggAlert');
  if(!enabled){alert.hidden=false;alert.className='gg-alert';alert.innerHTML='功能开关当前关闭。本页不会启动新的生产库同步；灰度验收时设置 <code>MONITOR_GROUP_GOVERNANCE_ENABLED=true</code>。'}
  else if(!s.last_success_at){alert.hidden=false;alert.className='gg-alert bad';alert.textContent=s.last_error||'后台尚未完成首次同步，暂无可靠审计结果。'}
  else if(!s.complete){const errs=(r.source_errors||[]).map(esc).join('；');alert.hidden=false;alert.className='gg-alert';alert.innerHTML=`数据不完整，已保留可用快照：${errs||esc(s.last_error||'部分来源待核验')}。未知不会按 0 处理。`}
  else alert.hidden=true;
  $('ggTotal').textContent=num(s.current_group_count);$('ggHigh').textContent=num(s.high_risk_count);$('ggNoChannelCount').textContent=num(s.no_enabled_channel_count);$('ggCleanup').textContent=num(s.cleanup_candidate_count);
  const coverage=s.coverage_start_at?`历史覆盖 ${dt(s.coverage_start_at)} 至 ${dt(s.coverage_end_at)}${s.history_complete?'':'（不足 30 天）'}`:'历史覆盖尚未核验';$('ggCoverage').textContent=`${coverage} · 无法归属前置拒绝 7d/30d：${num(s.unattributed_rejections_7d)} / ${num(s.unattributed_rejections_30d)}`;
}

function filtered(){
  const rows=state.report?.groups||[],q=($('ggSearch')?.value||'').trim().toLowerCase(),status=$('ggStatus')?.value||'';
  return rows.filter(g=>(!q||g.group.toLowerCase().includes(q)||(g.display_name||'').toLowerCase().includes(q))&&(!status||g.status===status)&&(!$('ggRatioName')?.checked||g.name_has_ratio)&&(!$('ggNoChannel')?.checked||g.enabled_channels===0)&&(!$('ggNoRequest')?.checked||g.requests_30d===0));
}
function render(){
  if(!state.report)return;const rows=filtered();$('ggVisible').textContent=`显示 ${num(rows.length)} / ${num(state.report.groups?.length||0)} 个分组`;
  $('ggRows').innerHTML=rows.length?rows.map(g=>rowHTML(g)+(state.expanded.has(g.group)?detailHTML(g):'')).join(''):'<tr><td colspan="10" class="gg-empty">没有符合当前条件的分组</td></tr>';
  for(const group of state.expanded){
    if(!rows.some(g=>g.group===group))continue;
    if(state.users.has(group))renderUsers(group);
    bindUser(group);
  }
}
function rowHTML(g){
  const flags=[];if(g.user_count>0)flags.push('<span class="gg-badge user">用户分组</span>');if(g.user_selectable)flags.push('<span class="gg-badge selectable">用户可选</span>');if(g.historical_only)flags.push('<span class="gg-badge observe">历史遗留</span>');
  const issues=(g.issues||[]).slice(0,3).map(x=>`<span class="gg-badge ${g.status}">${esc(x)}</span>`).join('');
  const ratio=g.ratio_configured?num(g.ratio):(g.group==='auto'?'<span class="gg-muted">不适用</span>':'<span class="gg-badge high">缺失</span>');
  return `<tr class="gg-row" tabindex="0" role="button" aria-expanded="${state.expanded.has(g.group)}" data-gg-group="${esc(g.group)}"><td><span class="gg-badge ${esc(g.status)}">${statusText[g.status]||esc(g.status)}</span></td><td><span class="gg-group">${esc(g.group)}</span><span class="gg-sub">${flags.join(' ')||'当前配置分组'}</span></td><td>${g.display_name?esc(g.display_name):'<span class="gg-muted">未配置</span>'}</td><td class="gg-num">${ratio}</td><td class="gg-num">${num(g.enabled_channels)} / <span class="gg-muted">${num(g.disabled_channels)}</span></td><td class="gg-num"><b>${num(g.user_count)}</b><span class="gg-sub">启用 ${num(g.enabled_user_count)} · 禁用 ${num(g.disabled_user_count)}</span></td><td class="gg-num">${num(g.explicit_token_count)}<span class="gg-sub">有效 ${num(g.enabled_token_count)} · 过期 ${num(g.expired_token_count)}</span></td><td class="gg-num">${num(g.requests_7d)} / ${num(g.requests_30d)}<span class="gg-sub">含前置拒绝 ${num(g.pre_route_rejections_30d)}</span></td><td><div class="gg-badges">${issues||'<span class="gg-muted">未发现</span>'}</div></td><td class="gg-muted">${recommend(g)}</td></tr>`;
}
function recommend(g){if(g.cleanup_candidate)return '清理候选，需人工核对';if(g.status==='high')return '优先补齐或恢复渠道';if(g.name_has_ratio)return '纳入命名规范化';if(g.historical_only)return '保留观察';if(g.status==='pending')return '核对配置';return '无需处理'}
function detailHTML(g){
  const channels=(g.channels||[]).map(c=>`<div class="gg-list-item"><span>#${num(c.id)} ${esc(c.name||'未命名')}</span><span class="gg-badge ${+c.status===1?'normal':'observe'}">${+c.status===1?'启用':'禁用'}</span></div>`).join('')||'<p>当前未关联渠道。</p>';
  const sources=(g.config_sources||[]).map(x=>`<span class="gg-badge">${esc(sourceText(x))}</span>`).join(' ')||'<span class="gg-muted">仅在近期历史中出现</span>';
  const plans=(g.subscriptions||[]).map(p=>`<div class="gg-list-item"><span>#${num(p.id)} ${esc(p.title)}</span><span class="gg-badge ${p.enabled?'normal':'observe'}">${p.enabled?'在售':'停用'}</span></div>`).join('')||`<p>${state.report?.state?.subscription_verified?'未发现订阅套餐引用。':'订阅套餐引用未核验。'}</p>`;
  return `<tr class="gg-detail"><td colspan="10"><div class="gg-detail-box"><section class="gg-detail-card"><h3>渠道引用</h3><div class="gg-list">${channels}</div></section><section class="gg-detail-card"><h3>配置出处</h3><div class="gg-badges">${sources}</div><p>最近观测时间：${dt(g.last_observed_at)}；7/30 天请求均包含可归属到该分组的前置拒绝。</p></section><section class="gg-detail-card"><h3>实际关联用户 · ${num(g.user_count)}</h3>${g.user_count>0?`<div class="gg-user-tools"><input type="search" id="${userID(g.group,'q')}" placeholder="搜索用户 ID、用户名或显示名"><button class="gg-btn" data-gg-user-search="${esc(g.group)}">搜索</button></div><div id="${userID(g.group,'body')}" class="gg-list"><p>正在读取本地用户快照…</p></div>`:'<p>当前没有 <code>users.group</code> 等于该分组的未删除用户。</p>'}</section><section class="gg-detail-card"><h3>订阅与判定依据</h3><div class="gg-list">${plans}</div><div class="gg-badges" style="margin-top:9px">${(g.issues||[]).map(x=>`<span class="gg-badge ${g.status}">${esc(x)}</span>`).join('')||'<span class="gg-badge normal">已检查范围内未发现问题</span>'}</div><p>历史任务与长期未使用令牌本期不做全量核验，清理候选不等于可安全删除。</p></section></div></td></tr>`;
}
function userID(group,suffix){let h=2166136261;for(const c of group){h^=c.codePointAt(0);h=Math.imul(h,16777619)}return `ggu-${h>>>0}-${suffix}`}
function toggle(group){if(state.expanded.has(group)){state.expanded.delete(group);render();return}state.expanded.add(group);render();const g=state.report.groups.find(x=>x.group===group);if(g?.user_count>0){bindUser(group);loadUsers(group,0)}}
function bindUser(group){const btn=document.querySelector(`[data-gg-user-search="${CSS.escape(group)}"]`),input=$(userID(group,'q'));if(!btn||btn.dataset.bound)return;btn.dataset.bound='1';btn.addEventListener('click',()=>loadUsers(group,0));input?.addEventListener('keydown',e=>{if(e.key==='Enter')loadUsers(group,0)})}
async function loadUsers(group,offset){
  const input=$(userID(group,'q')),q=input?.value||'',body=$(userID(group,'body'));if(body)body.innerHTML='<p>正在读取 Monitor 本地用户快照…</p>';
  try{const params=new URLSearchParams({group,q,offset:String(offset),limit:'20'}),res=await fetch('/group-governance/users?'+params,{headers:{Accept:'application/json'}});const data=await res.json();if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);state.users.set(group,data);renderUsers(group);bindUser(group)}catch(e){if(body)body.innerHTML=`<p>读取失败：${esc(e.message)}</p>`}
}
function renderUsers(group){const data=state.users.get(group),body=$(userID(group,'body'));if(!data||!body)return;const list=data.users||[],users=list.map(u=>`<div class="gg-list-item"><span><b>#${num(u.user_id)} ${esc(u.username)}</b>${u.display_name&&u.display_name!==u.username?` · ${esc(u.display_name)}`:''}<small class="gg-sub">${userRole(u.role)}</small></span><span class="gg-badge ${+u.status===1?'normal':'observe'}">${userStatus(u.status)}</span></div>`).join('')||'<p>没有符合搜索条件的用户。</p>';const prev=data.offset>0?`<button class="gg-btn" data-user-page="${Math.max(0,data.offset-data.limit)}">上一页</button>`:'',next=data.offset+list.length<data.total?`<button class="gg-btn" data-user-page="${data.offset+data.limit}">下一页</button>`:'',range=data.total?`${num(data.offset+1)}–${num(Math.min(data.offset+list.length,data.total))} / ${num(data.total)}`:'0 / 0';body.innerHTML=users+`<div class="gg-pages"><span>${range}</span>${prev}${next}</div>`;body.querySelectorAll('[data-user-page]').forEach(b=>b.addEventListener('click',()=>loadUsers(group,+b.dataset.userPage)))}
})();
