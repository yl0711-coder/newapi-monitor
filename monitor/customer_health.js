(function(){
'use strict';
// 客户维护：盯住已加入名单的公司，看它们今天用得顺不顺。
//
// ★ 必须整体包在 IIFE 里 ★
// page.html 的内联脚本已在全局声明了 const esc（见 page.html 的 esc 定义）。
// 本文件若在顶层再声明同名 const，同一全局作用域两次 const 声明会抛
// SyntaxError，**整个页面的 JS 全部不执行**——表现为所有页签点了都没反应。
// 2026-09-15 实测踩过这个坑。logchain.js 等同类文件同样用 IIFE 隔离。
//
// 本页报表只读 /customer-health/report（本地事实，不打生产库）；
// 增删改走 /customer-health 下的独立名单接口；用户用量名单保持不变。
//
// 与客户排障的分工：本页不展示任何请求日志正文。要看某一条请求发生了什么，
// 点「去排障」跳过去。这个边界必须守住——本页一旦开始列日志，就会变成
// 客户排障的第二份实现，两边判据迟早漂移。

const ch={
  inited:false,
  loading:false,
  saving:false,
  // generation 用于作废离开页面前发出的旧请求：旧响应回来时若代次已变，
  // 无权改写页面。与 logchain.js 同一套做法。
  generation:0,
  abort:null,
  refreshTimer:null,
  report:null,
  error:'',
  // expanded 按 group_id 记展开状态。行会被 render 整体重建，
  // 状态挂在行上会连同 DOM 一起丢。
  expanded:new Set(),
  // editing 记正在编辑的公司 group_id；null = 没有在编辑。
  editing:null,
};

const $=id=>document.getElementById(id);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=n=>(+n||0).toLocaleString('zh-CN');

// pct 格式化稳定率。null/undefined 一律显示「—」而不是 0%：
// 「今天还没有日志记录」和「全部失败」是完全不同的结论，显示成 0% 会造成误判。
const pct=v=>(v===null||v===undefined)?'—':(+v).toFixed(1)+'%';

// 责任方与置信度的中文标签。取值与后端 faultUpstream 等常量一一对应，
// 前端不做任何推断，只做翻译。
const FAULT_LABEL={upstream:'上游问题',ours:'我方问题',downstream:'客户问题',unknown:'待判'};
const CONF_LABEL={high:'判据明确',mid:'判据合理',low:'样本不足',none:'证据不足'};

// 责任方色块沿用客户排障的 .lc-fault-tag（描边而非实心底色）：那一列是推断，
// 不该和事实拥有同等的视觉确定性。后端取值与 lc 的类名后缀不同名，在此显式映射，
// 不用字符串拼接凑——拼错了会静默掉样式。
const FAULT_CLS={upstream:'lc-fault-up',ours:'lc-fault-ours',downstream:'lc-fault-down',unknown:'lc-fault-unknown'};

// money 把美元金额格式化。★ null/undefined 必须返回 null ★
// 返回 0 或 '$0.00' 会把"取不到"显示成"没花钱"，那是两个完全不同的结论。
// 由调用方决定取不到时显示什么（当前显示「—」+ 原因）。
const money=v=>(v===null||v===undefined)?null:'$'+(+v).toFixed(2);

// 与后端 customerHealthSpend* 常量一一对应。写成常量而不是字面量散落各处，
// 免得改了后端取值前端还在比对旧字符串。
const SPEND_STATE_KEY_FINALIZED='finalized';
// partial 的标签必须说"部分成员缺"而不是笼统的"不可知"：
// 前者告诉人"数据大体在、缺几个人"，后者会被理解成"整个功能没数据"。
const SPEND_STATE_LABEL={live:'实时',finalized:'仅封口部分',sampled:'本地采集',unavailable:'不可知',partial:'部分成员缺'};
const SCOPE_LABEL={
  only_this_customer:'更像该公司问题',
  all_customers:'更像渠道问题',
  sole_user:'该渠道只有这家在用',
  no_problem:'今天无问题',
  unknown:'无法判断',
};

window.customerHealthActivate=function(){
  if(!ch.inited)init();
  // 每次重新进入都立即校准一次；只靠 60 秒定时器会让离开很久后返回的用户
  // 先看到最多一分钟的旧数据。
  load();
  scheduleRefresh();
};

window.customerHealthDeactivate=function(){
  if(!ch.inited)return;
  ++ch.generation;
  ch.abort?.abort();
  ch.abort=null;
  ch.loading=false;
  if(ch.refreshTimer){clearTimeout(ch.refreshTimer);ch.refreshTimer=null}
  render();
};

function scheduleRefresh(){
  if(ch.refreshTimer)clearTimeout(ch.refreshTimer);
  ch.refreshTimer=null;
  if(document.hidden||$('tab-customer-health')?.hidden)return;
  ch.refreshTimer=setTimeout(async()=>{
    ch.refreshTimer=null;
    if(!document.hidden&&!$('tab-customer-health')?.hidden)await load();
    scheduleRefresh();
  },60000);
}

function init(){
  ch.inited=true;
  $('chRefresh')?.addEventListener('click',()=>load());
  $('chAddForm')?.addEventListener('submit',e=>{e.preventDefault();addCustomer()});
  document.addEventListener('visibilitychange',()=>{
    if(document.hidden){
      if(ch.refreshTimer){clearTimeout(ch.refreshTimer);ch.refreshTimer=null}
      return;
    }
    if(!$('tab-customer-health')?.hidden){load();scheduleRefresh()}
  });
  // 事件委托：行按钮由 render 整体重建，不能逐个绑定，也不用内联 onclick。
  $('chRows')?.addEventListener('click',onRowClick);
}

// ═══════════ 取数 ═══════════

async function load(){
  if(ch.loading)return;
  ch.loading=true;
  ch.error='';
  const gen=ch.generation;
  ch.abort?.abort();
  ch.abort=new AbortController();
  render();
  try{
    // cache:'no-store' 与后端的 noStoreSensitive 配对：响应含公司名、用户名、
    // user_id、消耗金额与故障归因，不得进浏览器磁盘缓存。
    // 两边都要：响应头管中间层与后续请求，这里管本次请求不去读旧缓存。
    const r=await fetch('/customer-health/report',{signal:ch.abort.signal,cache:'no-store',
      headers:{'Accept':'application/json'}});
    const data=await r.json().catch(()=>({}));
    if(gen!==ch.generation)return; // 已离开本页，无权改写状态
    if(!r.ok){ch.error=data.error||('取数失败 HTTP '+r.status);}
    else{ch.report=data;}
  }catch(e){
    if(gen!==ch.generation)return;
    if(e.name!=='AbortError')ch.error='取数失败：'+e.message;
  }finally{
    if(gen===ch.generation){ch.loading=false;render();}
  }
}

// ═══════════ 增删改（独立 /customer-health/* 写接口）═══════════

// addCustomer 由服务端在一个 SQLite 事务里“复用/创建公司 + 加入首个成员”。
// 任一步失败都整体回滚，不留下客户维护页不可见、但本地客户维护表仍残留的空公司。
async function addCustomer(){
  if(ch.saving)return;
  const username=String($('chUsername')?.value||'').trim();
  const company=String($('chCompany')?.value||'').trim();
  if(!username||!company){setFormError('用户ID（或已同步用户名）和公司名都要填');return}
  ch.saving=true;setFormError('');render();
  try{
    const {res,data}=await chMutate('/customer-health/customers',{input:username,company},
      `ch-customer-add:${company}:${username}`);
    if(!res.ok||data?.ok===false){
      setFormError((data&&data.error)||('添加用户失败 HTTP '+res.status));
      return;
    }
    $('chUsername').value='';
    $('chCompany').value='';
    await load();
  }catch(e){
    setFormError('添加失败：'+e.message+'；重试会复用同一幂等键，不会重复添加');
  }finally{
    ch.saving=false;render();
  }
}

function setFormError(msg){ch.formError=msg}

// onRowClick 事件委托。按钮判定必须排在整行展开之前：
// 否则点「删除」会同时触发展开，页面在确认框弹出时抖一下。
function onRowClick(e){
  const jump=e.target.closest('[data-ch-jump]');
  // 客户维护是一个明确的排障入口，不能继承排障页上一次遗留的日期、渠道、
  // 模型、令牌等筛选。preset 由排障页统一恢复“今天全部问题”的干净视图。
  if(jump){window.monitorNavigate?.('logchain',{preset:'customer_diagnosis',user_id:+jump.dataset.chJump||0});return}
  const del=e.target.closest('[data-ch-del]');
  if(del){deleteCompany(+del.dataset.chDel);return}
  const delUser=e.target.closest('[data-ch-deluser]');
  if(delUser){removeMember(+delUser.dataset.chDeluser);return}
  const edit=e.target.closest('[data-ch-edit]');
  if(edit){ch.editing=+edit.dataset.chEdit;render();return}
  const cancel=e.target.closest('[data-ch-cancel]');
  if(cancel){ch.editing=null;render();return}
  const save=e.target.closest('[data-ch-save]');
  if(save){saveCompany(+save.dataset.chSave);return}
  const addUser=e.target.closest('[data-ch-adduser]');
  if(addUser){addMember(+addUser.dataset.chAdduser);return}
  const row=e.target.closest('[data-ch-row]');
  if(!row)return;
  const id=+row.dataset.chRow;
  if(ch.expanded.has(id))ch.expanded.delete(id);else ch.expanded.add(id);
  render();
}

function deleteControlHTML(r){
  const n=r.members.length;
  return `<button type="button" class="ch-mini ch-danger" data-ch-del="${r.group_id}" `+
    `title="一次性把 ${n} 个成员从客户维护名单删除并解散公司；用户用量不受影响">`+
    `解散公司（连 ${n} 人一起移除）</button>`;
}

// 解散由后端在一个事务里完成，任何一步失败都会连同公司和成员删除一起回滚，
// 浏览器只发送一次请求。
async function deleteCompany(groupID){
  const row=(ch.report?.rows||[]).find(r=>r.group_id===groupID);
  if(!row)return;
  const n=row.members.length;
  if(!confirm('解散「'+row.company+'」？\n\n'+
    '将一次性把 '+n+' 个成员从客户维护名单删除，并解散该公司。\n'+
    '任何一步失败都会整体撤销，不会留下删了一半的状态。\n\n'+
    '客户维护这边的人和公司一起消失；用户用量里的客户分组和用户名单不受影响。\n'+
    '不影响主站账号，也不删除历史用量事实或请求日志。'))return;
  if(ch.saving)return;
  ch.saving=true;setFormError('');render();
  try{
    const {res,data}=await chMutate('/customer-health/groups/dissolve',{id:groupID},`ch-group-dissolve:${groupID}`);
    if(!res.ok||data?.ok===false){
      setFormError('解散公司失败，成员和公司均未删除：'+(data?.error||('HTTP '+res.status)));
      return;
    }
    await load();
  }catch(e){
    setFormError('解散请求的结果暂时未知：'+e.message+'；重试会复用同一幂等键，不会重复执行。');
  }finally{
    ch.saving=false;render();
  }
}

// saveCompany 改公司名。
// actionKey 带上目标名：改成 A 失败后改成 B，是两个不同的动作，不该共用幂等键。
async function saveCompany(groupID){
  const name=String($('chEditName-'+groupID)?.value||'').trim();
  if(!name){setFormError('公司名不能为空');render();return}
  const ok=await post('/customer-health/groups/update',{id:groupID,name},'改名失败',
    `ch-group-rename:${groupID}:${name}`);
  if(ok)ch.editing=null;
}

// addMember 给公司加用户名。
async function addMember(groupID){
  const input=String($('chEditUser-'+groupID)?.value||'').trim();
  if(!input){setFormError('要添加的用户ID或已同步用户名不能为空');render();return}
  const ok=await post('/customer-health/members',{input,group_id:groupID},'添加用户失败',
    `ch-member-add:${input}:${groupID}`);
  if(ok&&$('chEditUser-'+groupID))$('chEditUser-'+groupID).value='';
}

// removeMember 只删除 Monitor 维护名单记录，不删除主站账号或历史数据。
async function removeMember(userID){
  if(!confirm('从 Monitor 客户维护名单删除该用户？\n\n仅删除维护名单记录，不影响主站账号，也不删除历史用量事实或请求日志。'))return;
  await post('/customer-health/members/delete',{user_id:userID},'删除用户失败',`ch-member-del:${userID}`);
}

// chMutate 所有写操作的唯一出口。
//
// ★ 必须走 window.usageMutationPost，不许自己 fetch ★
// 那个封装做了三件本页也需要的事：带稳定的 Idempotency-Key + 请求体
// request_id、等完整 JSON 体读完才算"已明确应答"、5xx/网络失败时**保留**
// 幂等键供下次重试；独立接口按独立名单的当前状态处理重复请求，不会改动用户用量。
//
// actionKey 必须对"同一个动作"稳定：它和 path 一起构成页面重试键的槽位。
// 拿时间戳或随机数当 actionKey 等于每次重试都是新动作，页面无法复用同一请求键。
async function chMutate(path,payload,actionKey){
  const impl=window.usageMutationPost;
  if(typeof impl!=='function'){
    // 宁可明确失败，也不退回裸 fetch：那会静默丢掉幂等保证。
    throw new Error('幂等提交封装未就绪（usageMutationPost 未加载）');
  }
  return impl(path,payload,actionKey);
}

// post 是简单写操作的公共外壳：统一错误提示 + 成功后重新取数。
// 不各写一遍，避免有的分支忘了刷新，页面显示与库里不一致。
async function post(url,body,failLabel,actionKey){
  if(ch.saving)return false;
  ch.saving=true;setFormError('');render();
  try{
    const {res,data}=await chMutate(url,body,actionKey);
    if(!res.ok||data?.ok===false){
      setFormError((data&&data.error)||(failLabel+' HTTP '+res.status));
      return false;
    }
    await load();
    return true;
  }catch(e){
    setFormError(failLabel+'：'+e.message+'；重试会复用同一幂等键，不会重复执行');
    return false;
  }finally{
    ch.saving=false;render();
  }
}

// ═══════════ 渲染 ═══════════

function render(){
  const box=$('chRows');
  if(!box)return;
  $('chFormError')&&($('chFormError').textContent=ch.formError||'');
  $('chConfirm')&&($('chConfirm').disabled=!!ch.saving);
  const meta=$('chMeta');
  if(meta){
    const collection=ch.report?.collection;
    meta.textContent=ch.report
      ? '统计日 '+ch.report.day+'（'+ch.report.time_zone+'）· 出报于 '+
        new Date((ch.report.generated_at||0)*1000).toLocaleTimeString('zh-CN')+
        (collection?.note?' · '+collection.note:'')
      : '';
  }
  // 空/错/加载三态都得是 <tr>：本页表格化后 tbody 里放 div 会被浏览器
  // 提到表格外面渲染，出现"提示文字飘在表格上方"。
  // colspan 必须与 page.html 表头列数一致（7）。
  if(ch.error){box.innerHTML='<tr><td colspan="7" class="lc-empty">'+esc(ch.error)+'</td></tr>';return}
  if(ch.loading&&!ch.report){box.innerHTML='<tr><td colspan="7" class="lc-empty">加载中…</td></tr>';return}
  const rows=ch.report?.rows||[];
  if(!rows.length){
    box.innerHTML='<tr><td colspan="7" class="lc-empty">名单是空的。用上面两个输入框把要盯的客户加进来。</td></tr>';
    return;
  }
  box.innerHTML=rows.map(rowHTML).join('');
}

// rowHTML 一家公司一行。结构与客户排障的 rowHTML 同构：
// 主行 <tr> + 展开时紧跟一条 colspan 明细行，整行可点。
function rowHTML(r){
  const open=ch.expanded.has(r.group_id);
  const threshold=ch.report?.red_threshold??90;
  // 标红条件：有稳定率且低于阈值。没有稳定率（今天无日志记录）不标红——
  // 那是"没数据"，标红会让人以为出了故障。
  const bad=r.stability_pct!==null&&r.stability_pct!==undefined&&r.stability_pct<threshold;
  // 复用客户排障的行状态类：lc-err = 红底（这里表示低于阈值），lc-open = 展开态。
  const cls=[bad?'lc-err':'',open?'lc-open':''].filter(Boolean).join(' ');
  const metricsReady=r.metrics_ready!==false;
  const metric=v=>metricsReady?num(v):'<span class="ch-unknown">—</span>';
  const tds=[
    `<td class="ch-company"><span class="ch-arrow">${open?'▾':'▸'}</span>${esc(r.company)}</td>`,
    `<td>${num(r.members.length)}</td>`,
    `<td><div><b>${metric(r.total)}</b></div><div class="ch-mix">`+
      `<span class="ok">正常 ${metric(r.success)}</span>`+
      `<span class="anom">异常 ${metric(r.anomaly)}</span>`+
      `<span class="fail">错误 ${metric(r.failed)}</span></div>`+
      (metricsReady?`<span class="ch-impact">计入稳定性：异常 ${num(r.stability_anomaly)} · 错误 ${num(r.stability_failed)}</span>`:'')+
      `</td>`,
    `<td class="ch-stability"><b>${pct(r.stability_pct)}</b></td>`,
    `<td>${primaryModelsHTML(r)}</td>`,
    spendCell(r),
    `<td class="lc-fault">${faultCell(r)}</td>`,
  ].join('');
  let html=`<tr data-ch-row="${r.group_id}" class="${cls}">${tds}</tr>`;
  if(open)html+=`<tr class="lc-detail"><td colspan="7">${detailHTML(r)}</td></tr>`;
  return html;
}

// 主要模型由后端按公司全部成员合并后计算；前端只展示，不自行重算口径。
// 默认展示使用最多的一个；仅当两个模型都严格超过 40% 时展示两个。
function primaryModelsHTML(r){
  if(r.metrics_ready===false)return '<div class="ch-primary-model ch-none"><b>—</b><span>指标回算中</span></div>';
  if((+r.total||0)<=0)return '<div class="ch-primary-model ch-none"><b>—</b><span>今日无记录</span></div>';
  const models=Array.isArray(r.primary_models)?r.primary_models:[];
  if(!models.length)return '<div class="ch-primary-model ch-none"><b>暂无可识别模型</b></div>';
  return models.map(m=>`<div class="ch-primary-model" title="${esc(m.name)} · ${num(m.requests)} 条日志记录">`+
    `<b>${esc(m.name)}</b><span>${(+m.share_pct||0).toFixed(1)}% · ${num(m.requests)} 条</span></div>`).join('');
}

// spendCell 今日消耗格。
//
// ★ 取不到时显示「—」，绝不显示 $0.00 ★
// 后端 today_spend_usd 为 null 即取不到（事实层未启用、水位未就绪等），
// 显示 0 会被读成"这家今天没花钱"。原因挂在 title 上，不占表格宽度。
function spendCell(r){
  const amount=money(r.today_spend_usd);
  const note=r.spend_note||'';
  if(amount===null){
    // partial 与 unavailable 都没有合计，但原因完全不同：
    // partial 是"缺几个成员的数"，unavailable 是"整个来源不可用"。
    // 标签必须区分，否则前者会被误读成后者，让人去排查数据源。
    const label=SPEND_STATE_LABEL[r.spend_state]||'不可知';
    return `<td class="ch-spend"><b class="ch-unknown" title="${esc(note)}">—</b>`+
      `<span class="ch-spend-state" title="${esc(note)}">${esc(label)}</span></td>`;
  }
  // finalized 必须在格子里就标出来，不能只写在展开区：
  // 那是"只统计到某个小时"的金额，看成全天会低估。
  const state=r.spend_state===SPEND_STATE_KEY_FINALIZED
    ? `<span class="ch-spend-state" title="${esc(note)}">${esc(SPEND_STATE_LABEL.finalized)}</span>`
    : `<span class="ch-spend-state" title="${esc(note)}">${esc(SPEND_STATE_LABEL[r.spend_state]||'')}</span>`;
  return `<td class="ch-spend"><b title="${esc(note)}">${esc(amount)}</b>${state}</td>`;
}

// faultCell 责任方格。沿用客户排障的描边色块 + 低可信度降权。
function faultCell(r){
  if(!r.fault)return '<span class="lc-sub">—</span>';
  const cls=FAULT_CLS[r.fault]||'lc-fault-unknown';
  // 本页归因一律降过一档（后端 customerHealthDowngradeConfidence），
  // low/none 再整体降权，与客户排障同一套视觉规则。
  const dim=(r.fault_confidence==='low'||r.fault_confidence==='none')?' lc-fault-dim':'';
  const conf=r.fault_confidence?CONF_LABEL[r.fault_confidence]||r.fault_confidence:'';
  const tip=(r.reason||'')+(conf?'（'+conf+'）':'');
  return `<span class="lc-fault-tag ${cls}${dim}" title="${esc(tip)}">${esc(FAULT_LABEL[r.fault]||r.fault)}</span>`;
}

function detailHTML(r){
  const editing=ch.editing===r.group_id;
  const spend=money(r.today_spend_usd);
  const metricsReady=r.metrics_ready!==false;
  const metric=v=>metricsReady?num(v):'—';
  // 指标区。金额与日志记录数并排：这两个是本页最常一起看的数
  //（"花了这么多钱，稳定性却这么差"）。
  // 金额取不到时给「—」并把原因写在下面一行，不给 0。
  const stats=`<div class="ch-stats">
      <div><b${metricsReady?'':' class="ch-unknown"'}>${metric(r.total)}</b><span>今日日志记录</span></div>
      <div><b${metricsReady?'':' class="ch-unknown"'}>${metric(r.success)}</b><span>正常记录</span></div>
      <div><b${metricsReady?'':' class="ch-unknown"'}>${metric(r.anomaly)}</b><span>异常记录</span></div>
      <div><b${metricsReady?'':' class="ch-unknown"'}>${metric(r.failed)}</b><span>错误记录</span></div>
      <div><b${metricsReady?'':' class="ch-unknown"'}>${metric(r.stability_anomaly)}</b><span>计入稳定性的异常</span></div>
      <div><b${metricsReady?'':' class="ch-unknown"'}>${metric(r.stability_failed)}</b><span>计入稳定性的错误</span></div>
      <div><b>${pct(r.stability_pct)}</b><span>记录稳定率</span></div>
      <div><b${spend===null?' class="ch-unknown"':''}>${spend===null?'—':esc(spend)}</b><span>今日消耗（全部用户）</span></div>
    </div>`;
  const primary=`<div class="ch-reason">
      <div class="ch-reason-line"><span class="ch-tag">主要模型</span>${primaryModelsHTML(r)}</div>
    </div>`;
  const metricsNote=r.metrics_note?`<div class="ch-detail-note">${esc(r.metrics_note)}</div>`:'';
  const spendNote=r.spend_note?`<div class="ch-detail-note">${esc(r.spend_note)}</div>`:'';
  const reason=metricsReady?`<div class="ch-reason">
      <div class="ch-reason-line">
        <span class="ch-tag">主要原因</span>
        ${faultCell(r)}
        ${r.fault_confidence?`<span class="ch-conf">${esc(CONF_LABEL[r.fault_confidence]||r.fault_confidence)}</span>`:''}
      </div>
      <div class="ch-reason-why">${esc(r.reason||'—')}</div>
      <div class="ch-reason-line">
        <span class="ch-tag">同渠道其他账号</span>
        <span class="ch-scope ch-scope-${esc(r.channel_scope)}">${esc(SCOPE_LABEL[r.channel_scope]||r.channel_scope)}</span>
      </div>
      <div class="ch-reason-why">${esc(r.scope_note||'')}</div>
    </div>`:'';
  // 成员行带各自今日消耗：公司合计已在上面给出，这里让人看得出是谁花的。
  // 同样地，取不到的成员显示「—」不显示 $0.00。
  const members=`<div class="ch-list">${r.members.map(mb=>{
    const mine=money(mb.today_spend_usd);
    return `<div class="ch-member">
        <span class="ch-mid">#${mb.user_id}</span>
        <span class="ch-mname">${esc(mb.username||'（无用户名）')}</span>
        <span class="ch-spend"><b${mine===null?' class="ch-unknown"':''}>${mine===null?'—':esc(mine)}</b></span>
        <button type="button" class="ch-mini" data-ch-jump="${mb.user_id}">去排障</button>
        ${editing?`<button type="button" class="ch-mini ch-danger" data-ch-deluser="${mb.user_id}">删除用户</button>`:''}
      </div>`}).join('')}</div>`;
  const editBox=editing?`<div class="ch-edit">
      <label>公司名
        <input id="chEditName-${r.group_id}" type="text" maxlength="64" value="${esc(r.company)}">
      </label>
      <button type="button" class="ch-mini" data-ch-save="${r.group_id}">保存公司名</button>
      <label>再加一个用户
        <input id="chEditUser-${r.group_id}" type="text" placeholder="用户ID或已同步用户名">
      </label>
      <button type="button" class="ch-mini" data-ch-adduser="${r.group_id}">加入本公司</button>
      <button type="button" class="ch-mini" data-ch-cancel="${r.group_id}">完成</button>
    </div>`:'';
  const actions=`<div class="ch-actions">
      ${editing?'':`<button type="button" class="ch-mini" data-ch-edit="${r.group_id}">编辑</button>`}
      ${deleteControlHTML(r)}
      <span class="ch-hint">本页不列请求日志，具体内容点「去排障」</span>
    </div>`;
  return `<div class="ch-detail">${stats}${metricsNote}${spendNote}${primary}${reason}${members}${editBox}${actions}</div>`;
}

})();
