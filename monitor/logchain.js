(function(){
'use strict';

// 客户排障（tab-logchain）。回答"某客户某条请求走了哪个上游、上游返回了什么错"。
//
// 只访问两个接口，都在 view 组（管理员）：
//   GET /logchain/requests  逐条明细（查生产 logs，含 type=5）
//   GET /logchain/filters   下拉取值（只读本地 channel_snaps）
//
// 默认只看错误（error_only=true）：本页定位是"当天故障清单"，不是全量请求流水。
// 可切换成显示全部请求，届时错误行仍标红底，并给出"N 条中 M 条错误"计数。

const lc={
  inited:false,
  date:'',            // YYYY-MM-DD（CST），空=今天
  errorOnly:true,
  filters:{group:'',domain:'',channel_id:'',model:'',user:'',keyword:''},
  rows:[],hasMore:false,nextBeforeID:0,
  blindSpots:[],scope:null,note:'',enrichError:'',
  opts:null,           // /logchain/filters 结果，只取一次
  loading:false,abort:null,generation:0,
  expanded:new Set()
};

const $=id=>document.getElementById(id);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const nfmt=n=>(+n||0).toLocaleString('zh-CN');
const usd=n=>{n=+n||0;return '$'+(n===0||Math.abs(n)>=.01?n.toFixed(2):n.toFixed(4))};

// 时间一律按 Asia/Shanghai 呈现，与后端 CST 自然日口径一致，避免跨时区误读。
const hhmm=ts=>ts?new Date(ts*1000).toLocaleTimeString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit'}):'—';
const hhmmss=ts=>ts?new Date(ts*1000).toLocaleTimeString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',second:'2-digit'}):'—';
const fullTime=ts=>ts?new Date(ts*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'—';
const cstToday=()=>{
  const p=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date());
  const v=t=>p.find(x=>x.type===t)?.value||'';
  return `${v('year')}-${v('month')}-${v('day')}`;
};
const shiftDate=(d,delta)=>{
  // 用 UTC 正午做基准做日期加减，避开夏令时/时区把日期推错一天。
  const [y,m,dd]=d.split('-').map(Number);
  const t=new Date(Date.UTC(y,m-1,dd,12,0,0));
  t.setUTCDate(t.getUTCDate()+delta);
  return t.toISOString().slice(0,10);
};
const dur=sec=>{const s=+sec||0;return s<=0?'—':(s<60?s+'s':Math.floor(s/60)+'m'+(s%60)+'s')};

// ═══════════ 激活与跨页上下文 ═══════════

window.logChainActivate=function(){
  if(!lc.inited)init();
  const changed=applyNavigationContext();
  if(!lc.rows.length||changed||!lc.scope)load();
};

// logChainOpen 供其它页跳进来用（如用户用量的客户详情 → 排障）。
window.logChainOpen=function(context){window.monitorNavigate?.('logchain',context||{})};

function applyNavigationContext(){
  const c=window.monitorNavigationContext?.()||{};
  if(!Object.keys(c).length)return false;
  let changed=false;
  // 跨页带 user_id 进来时按客户筛；带 date 时切到那天。
  const user=c.user_id?String(c.user_id):'';
  if(user&&lc.filters.user!==user){lc.filters.user=user;changed=true}
  if(c.date&&lc.date!==c.date){lc.date=c.date;changed=true}
  if(c.domain&&lc.filters.domain!==c.domain){lc.filters.domain=c.domain;changed=true}
  if(c.group&&lc.filters.group!==c.group){lc.filters.group=c.group;changed=true}
  if(c.channel_id&&lc.filters.channel_id!==String(c.channel_id)){lc.filters.channel_id=String(c.channel_id);changed=true}
  // 从别处跳进来排障，通常想看全部而非仅错误；显式传 error_only 才覆盖。
  if(c.error_only!=null){const v=String(c.error_only)==='true';if(lc.errorOnly!==v){lc.errorOnly=v;changed=true}}
  if(changed)syncControls();
  return changed;
}

function init(){
  lc.inited=true;
  lc.date=lc.date||cstToday();

  $('lcPrevDay')?.addEventListener('click',()=>{lc.date=shiftDate(lc.date,-1);syncControls();load()});
  $('lcNextDay')?.addEventListener('click',()=>{
    if(lc.date>=cstToday())return; // 不允许翻到未来
    lc.date=shiftDate(lc.date,1);syncControls();load();
  });
  $('lcToday')?.addEventListener('click',()=>{lc.date=cstToday();syncControls();load()});
  $('lcDate')?.addEventListener('change',()=>{
    const v=$('lcDate').value;
    if(!v)return;
    if(v>cstToday()){showError('不能选择未来日期。');syncControls();return}
    lc.date=v;load();
  });

  $('lcErrorOnly')?.addEventListener('change',()=>{lc.errorOnly=!!$('lcErrorOnly').checked;load()});

  // 只有真正的 <select> 才绑 change（选完立即查）。
  // 文本框绝不能绑 change：它在失焦时触发，用户输入后点"查询"会先 blur 再 click，
  // 于是发两次相同查询。detail 泳道容量只有 1 且与客户 Portal 共用，
  // 白发一次就是让客户多排一次队。文本框统一走回车 / 查询按钮。
  ['lcGroup','lcDomain','lcChannel'].forEach(id=>$(id)?.addEventListener('change',()=>{
    const key={lcGroup:'group',lcDomain:'domain',lcChannel:'channel_id'}[id];
    lc.filters[key]=$(id).value||'';
    // 选了具体渠道就不该再受域名约束（渠道更精确），避免两者冲突筛出空集。
    if(key==='channel_id'&&lc.filters.channel_id){lc.filters.domain='';syncControls()}
    load();
  }));

  const applyText=()=>{
    lc.filters.model=($('lcModel')?.value||'').trim();
    lc.filters.user=($('lcUser')?.value||'').trim();
    lc.filters.keyword=($('lcKeyword')?.value||'').trim();
    load();
  };
  $('lcApply')?.addEventListener('click',applyText);
  ['lcModel','lcUser','lcKeyword'].forEach(id=>$(id)?.addEventListener('keydown',e=>{if(e.key==='Enter')applyText()}));

  $('lcReset')?.addEventListener('click',()=>{
    lc.filters={group:'',domain:'',channel_id:'',model:'',user:'',keyword:''};
    lc.errorOnly=true;lc.date=cstToday();
    syncControls();load();
  });

  $('lcMore')?.addEventListener('click',()=>load(true));

  // 行展开：事件委托，避免每行绑监听。
  //
  // 按钮判断必须排在取行之前：复制/跳转按钮都在展开行(tr.lc-detail)里，
  // 而展开行没有 data-lc-id。先取行就会 closest 返回 null 直接 return，
  // 按钮永远点不动。
  $('lcTableBody')?.addEventListener('click',e=>{
    const copyBtn=e.target.closest('[data-lc-copy]');
    if(copyBtn){copyText(copyBtn.dataset.lcCopy);return}
    const jumpBtn=e.target.closest('[data-lc-jump]');
    if(jumpBtn){window.monitorNavigate?.('channels',{domain:jumpBtn.dataset.lcJump});return}
    const tr=e.target.closest('tr[data-lc-id]');
    if(!tr)return; // 点在展开行的正文/空白上：不收起，便于选中复制文本
    const id=tr.dataset.lcId;
    if(lc.expanded.has(id))lc.expanded.delete(id);else lc.expanded.add(id);
    render();
  });

  syncControls();
  loadFilterOptions();
}

function syncControls(){
  if($('lcDate')){$('lcDate').value=lc.date;$('lcDate').max=cstToday()}
  if($('lcErrorOnly'))$('lcErrorOnly').checked=lc.errorOnly;
  if($('lcUser'))$('lcUser').value=lc.filters.user;
  if($('lcKeyword'))$('lcKeyword').value=lc.filters.keyword;
  ['lcGroup','lcDomain','lcChannel','lcModel'].forEach(id=>{
    const key={lcGroup:'group',lcDomain:'domain',lcChannel:'channel_id',lcModel:'model'}[id];
    if($(id))$(id).value=lc.filters[key]||'';
  });
  const nextBtn=$('lcNextDay');
  if(nextBtn){const atToday=lc.date>=cstToday();nextBtn.disabled=atToday;nextBtn.title=atToday?'已是今天':'后一天'}
  const label=$('lcDateLabel');
  if(label)label.textContent=lc.date===cstToday()?'今天':lc.date;
}

// ═══════════ 取数 ═══════════

async function loadFilterOptions(){
  if(lc.opts)return; // 下拉选项与日期无关，只取一次
  try{
    const r=await fetch('/logchain/filters',{headers:{'Accept':'application/json'}});
    if(r.status===401){location.href='/login';return}
    if(!r.ok)return; // 下拉取不到不阻塞主表，用户仍可用文本框筛
    lc.opts=await r.json();
    populateFilters();
  }catch(e){/* 同上，静默降级 */}
}

function populateFilters(){
  const o=lc.opts;if(!o)return;
  setOptions('lcGroup',(o.groups||[]).map(g=>({v:g,t:g})),lc.filters.group,'全部服务分组');
  setOptions('lcDomain',(o.domains||[]).map(d=>({v:d,t:d})),lc.filters.domain,'全部上游主域名');
  setOptions('lcChannel',(o.channels||[]).map(c=>({
    v:String(c.id),
    t:`${c.name||('#'+c.id)}${c.domain?' · '+c.domain:''}${c.deleted?' (已删除)':''}`
  })),lc.filters.channel_id,'全部渠道');
}

function setOptions(id,items,current,placeholder){
  const el=$(id);if(!el)return;
  el.innerHTML=`<option value="">${esc(placeholder)}</option>`+
    items.map(i=>`<option value="${esc(i.v)}">${esc(i.t)}</option>`).join('');
  el.value=current||'';
}

function buildQuery(more){
  const q=new URLSearchParams();
  // 单日：from=to=当天，后端会把 to 当天整日纳入（左闭右开）。
  q.set('from',lc.date);q.set('to',lc.date);
  if(lc.errorOnly)q.set('error_only','true');
  if(lc.filters.group)q.set('group',lc.filters.group);
  if(lc.filters.domain)q.set('domain',lc.filters.domain);
  if(lc.filters.channel_id)q.set('channel_id',lc.filters.channel_id);
  if(lc.filters.model)q.set('model',lc.filters.model);
  if(lc.filters.keyword)q.set('keyword',lc.filters.keyword);
  // 客户输入：纯数字按 user_id，否则按令牌名模糊查（后端 token_name LIKE）。
  const u=lc.filters.user;
  if(u){ if(/^\d+$/.test(u))q.set('user_id',u); else q.set('token_name',u); }
  q.set('limit','100');
  if(more&&lc.nextBeforeID)q.set('before_id',String(lc.nextBeforeID));
  return q.toString();
}

// load 取一页数据。
//
// 不能用 "if(lc.loading)return" 做互斥:那样在请求进行中改筛选条件会被静默丢弃,
// 表格停在旧结果上,用户以为筛选没生效。改用世代计数——新请求直接中止旧请求,
// 且只有最新世代有权写状态,避免被中止的旧请求把新请求的 loading 标记清掉。
async function load(more){
  const gen=++lc.generation;
  lc.loading=true;
  lc.abort?.abort();
  const ac=new AbortController();lc.abort=ac;
  if(!more){lc.rows=[];lc.expanded.clear();lc.nextBeforeID=0}
  renderStatus(more?'加载更多…':'加载中…');
  try{
    const r=await fetch('/logchain/requests?'+buildQuery(more),{signal:ac.signal,headers:{'Accept':'application/json'}});
    if(r.status===401){location.href='/login';return}
    const text=await r.text();
    if(gen!==lc.generation)return; // 已被更新的请求取代,丢弃本次结果
    let data={};
    try{data=JSON.parse(text)}catch(e){throw new Error(`响应不是 JSON（HTTP ${r.status}）：${text.slice(0,200)}`)}
    if(!r.ok)throw new Error(data.error||`HTTP ${r.status}`);
    lc.rows=more?lc.rows.concat(data.rows||[]):(data.rows||[]);
    lc.hasMore=!!data.has_more;
    lc.nextBeforeID=+data.next_before_id||0;
    lc.blindSpots=data.blind_spots||[];
    lc.scope=data.scope||null;
    lc.note=data.note||'';
    lc.enrichError=data.channel_enrich_error||'';
    render();
  }catch(e){
    if(e.name==='AbortError'||gen!==lc.generation)return;
    lc.rows=more?lc.rows:[];
    render();
    showError(e.message||String(e));
  }finally{
    // 只有最新世代能解锁,否则被中止的旧请求会提前放开 loading。
    if(gen===lc.generation)lc.loading=false;
  }
}

function showError(msg){
  const el=$('lcError');
  if(!el)return;
  el.hidden=false;
  el.innerHTML=`<b>查询失败</b><div style="margin-top:4px">${esc(msg)}</div>`;
}
function clearError(){const el=$('lcError');if(el)el.hidden=true}

async function copyText(t){
  try{await navigator.clipboard.writeText(t);toast('已复制')}
  catch(e){toast('复制失败，请手动选择文本')}
}
function toast(msg){
  const el=$('lcToast');if(!el)return;
  el.textContent=msg;el.hidden=false;
  clearTimeout(el._t);el._t=setTimeout(()=>{el.hidden=true},1600);
}

// ═══════════ 渲染 ═══════════

function renderStatus(text){
  const el=$('lcStatus');
  if(el)el.textContent=text||'';
}

// upstreamCell 渠道 → 上游主域名。三种状态语义完全不同，必须区分显示：
//   正常          渠道名 → 域名
//   快照缺失      渠道 #id（渠道快照缺失）—— 不是"没有上游"
//   channel_id=0  未打到渠道 —— 请求在选渠道前就失败了
function upstreamCell(r){
  if(!r.channel_id){
    return `<span class="badge badge-muted" title="请求在选定渠道前就失败了（如限流、无可用渠道、分组无权限）">未打到渠道</span>`;
  }
  const chan=esc(r.channel_name||('#'+r.channel_id));
  const flags=[];
  if(r.channel_deleted)flags.push('<span class="gtag" title="该渠道已从 NewAPI 删除，快照保留以便查历史">已删除</span>');
  if(r.channel_status===2)flags.push('<span class="gtag" title="手动禁用">已禁用</span>');
  if(r.channel_status===3)flags.push('<span class="gtag" title="自动禁用（熔断）">自动禁用</span>');
  if(r.channel_unresolved){
    return `<div><span title="渠道 ID ${esc(r.channel_id)}">${chan}</span></div>`+
      `<div class="lc-sub" title="本地渠道快照里查不到这个渠道，因此无法得知它的上游主域名。不等于该请求没有上游。">⚠ 渠道快照缺失</div>`;
  }
  const dom=r.upstream_domain
    ? `<b class="lc-domain" title="上游主域名">${esc(r.upstream_domain)}</b>`
    : `<span class="lc-sub">（无主域名）</span>`;
  return `<div>${chan} ${flags.join(' ')}</div><div class="lc-arrow">→ ${dom}</div>`;
}

function modelCell(r){
  let html=`<span>${esc(r.model_name||'—')}</span>`;
  // 模型映射：客户请求的名字与上游实际收到的不同，排障时必须让人看见。
  if(r.is_model_mapped&&r.upstream_model_name){
    html+=`<div class="lc-sub" title="发生模型映射：上游实际收到的模型名">↳ ${esc(r.upstream_model_name)}</div>`;
  }
  return html;
}

// contentCell 错误原文。限高显示，点行展开看全文；原文一字不改。
function contentCell(r){
  const c=r.content||'';
  if(!c)return `<span class="lc-sub">（无内容）</span>`;
  return `<div class="lc-content">${esc(c)}</div>`;
}

function rowHTML(r){
  const id=String(r.id);
  const isErr=r.type===5;
  const open=lc.expanded.has(id);
  const cls=[isErr?'lc-err':'',open?'lc-open':''].filter(Boolean).join(' ');
  const tds=[
    `<td class="lc-cust"><div>${esc(r.member||('#'+r.user_id))}</div><div class="lc-sub">ID ${esc(r.user_id)}</div></td>`,
    `<td>${esc(r.token_name||'—')}</td>`,
    `<td>${r.group?`<span class="gtag">${esc(r.group)}</span>`:'—'}</td>`,
    `<td>${modelCell(r)}</td>`,
    `<td class="lc-up">${upstreamCell(r)}</td>`,
    `<td class="lc-ct">${contentCell(r)}</td>`,
    // 时间放最后一列，精确到时分（用户明确要求）；hover 看到秒与完整日期。
    `<td class="lc-time" title="${esc(fullTime(r.created_at))}"><b>${esc(hhmm(r.created_at))}</b><div class="lc-sub">${esc(hhmmss(r.created_at).slice(-2))}s</div></td>`
  ].join('');
  let html=`<tr data-lc-id="${esc(id)}" class="${cls}">${tds}</tr>`;
  if(open)html+=detailHTML(r);
  return html;
}

function detailHTML(r){
  const kv=[];
  const add=(k,v,title)=>{if(v!==''&&v!=null)kv.push(`<div class="lc-kv"><span${title?` title="${esc(title)}"`:''}>${esc(k)}</span><b>${esc(v)}</b></div>`)};
  add('请求 ID',r.request_id||'—','new-api logs.request_id，可用它跟上游对账');
  add('类型',r.type_name||String(r.type));
  add('发生时间',fullTime(r.created_at));
  add('耗时',dur(r.use_time));
  if(r.is_stream)add('首字延迟',r.first_byte_ms?r.first_byte_ms+'ms':'—');
  add('流式',r.is_stream?'是':'否');
  add('输入 tokens',nfmt(r.prompt_tokens));
  add('输出 tokens',nfmt(r.completion_tokens));
  if(r.cache_read_tokens)add('缓存读 tokens',nfmt(r.cache_read_tokens));
  // 费用仅消费(type=2)有意义：其它类型 quota 恒为 0，显示 $0.00 会误导对账。
  if(r.type===2)add('费用',usd(r.cost_usd));
  if(r.request_path)add('请求路径',r.request_path);
  add('渠道 ID',r.channel_id?String(r.channel_id):'（未打到渠道）');
  if(r.channel_vendor)add('厂商',r.channel_vendor);
  if(r.upstream_domain)add('上游主域名',r.upstream_domain);

  const raw=r.content||'';
  const rawBlock=raw
    ? `<div class="lc-raw-head">
         <span>上游返回原文（未做任何改写）</span>
         <button type="button" class="lc-copy" data-lc-copy="${esc(raw)}">复制</button>
       </div>
       <pre class="lc-raw">${esc(raw)}</pre>`
    : `<div class="lc-sub" style="margin-top:8px">这条记录没有 content。</div>`;

  // 用 data 属性 + 事件委托，不用内联 onclick：
  // 内联写法要把域名插进 HTML 属性里的 JS 字符串字面量，多一层转义面；
  // 且与复制按钮的处理方式不一致，容易漏改。
  const jump=r.upstream_domain
    ? `<button type="button" class="lc-jump" data-lc-jump="${esc(r.upstream_domain)}">在渠道管理中查看 ${esc(r.upstream_domain)}</button>`
    : '';

  return `<tr class="lc-detail"><td colspan="7">
    <div class="lc-kvs">${kv.join('')}</div>
    ${rawBlock}
    ${jump?`<div style="margin-top:10px">${jump}</div>`:''}
  </td></tr>`;
}

function render(){
  clearError();
  const body=$('lcTableBody');
  if(!body)return;

  const rows=lc.rows;
  const errs=rows.filter(r=>r.type===5).length;

  // 计数：只看错误时说"N 条错误"；显示全部时说"N 条中 M 条错误"，
  // 后者能看出错误占比，避免把偶发错误当成系统性故障。
  const counter=$('lcCounter');
  if(counter){
    if(!rows.length)counter.textContent='';
    else if(lc.errorOnly)counter.innerHTML=`本页 <b>${nfmt(rows.length)}</b> 条错误${lc.hasMore?'（还有更多）':''}`;
    else counter.innerHTML=`本页 <b>${nfmt(rows.length)}</b> 条中 <b class="lc-errnum">${nfmt(errs)}</b> 条错误${lc.hasMore?'（还有更多）':''}`;
  }

  if(!rows.length){
    body.innerHTML=`<tr><td colspan="7" class="lc-empty">${emptyText()}</td></tr>`;
  }else{
    // 后端按 id DESC 返回（近似时间倒序）：最新的错误在最上面，
    // 排障时最关心的是"刚刚发生了什么"。
    body.innerHTML=rows.map(rowHTML).join('');
  }

  const more=$('lcMore');
  if(more){more.hidden=!lc.hasMore;more.disabled=lc.loading}

  renderBlindSpots();
  renderNotes();
  renderStatus('');
}

function emptyText(){
  const day=lc.date===cstToday()?'今天':lc.date;
  const filtered=!!(lc.filters.group||lc.filters.domain||lc.filters.channel_id||lc.filters.model||lc.filters.user||lc.filters.keyword);
  if(lc.errorOnly){
    return `<b>${esc(day)}没有查到错误请求。</b>`+
      (filtered?'<div class="lc-sub" style="margin-top:6px">当前有筛选条件，可点"重置"看全部。</div>':'')+
      `<div class="lc-sub" style="margin-top:6px">注意：这不代表没有客户遇到问题 —— 限流/无可用渠道这类前置拒绝不写日志，见下方说明。</div>`;
  }
  return `<b>${esc(day)}没有查到请求。</b>`+
    (filtered?'<div class="lc-sub" style="margin-top:6px">当前有筛选条件，可点"重置"看全部。</div>':'');
}

// renderBlindSpots 盲区常驻显示，不做折叠。
// 这个功能最可能造成的实际损害是：客户说"我请求根本发不出去"，
// 你在这里查不到，于是判断他在瞎说。所以这段话必须一直在眼前。
function renderBlindSpots(){
  const el=$('lcBlind');
  if(!el)return;
  if(!lc.blindSpots.length){el.hidden=true;return}
  el.hidden=false;
  el.innerHTML=`<div class="lc-blind-head">这个页面查不到的情况（重要）</div>`+
    `<ul class="lc-blind-list">${lc.blindSpots.map(s=>`<li>${esc(s)}</li>`).join('')}</ul>`;
}

function renderNotes(){
  const el=$('lcNotes');
  if(!el)return;
  const notes=[];
  if(lc.note)notes.push(esc(lc.note));
  if(lc.enrichError)notes.push(`渠道信息补全失败（明细仍有效）：${esc(lc.enrichError)}`);
  // 后端可能收敛了范围（跨度截断/limit 上限），回显出来，避免以为筛选原样生效。
  if(lc.scope&&lc.scope.from&&lc.scope.to)notes.push(`查询范围：${esc(lc.scope.from)} ～ ${esc(lc.scope.to)}（CST）`);
  if(!notes.length){el.hidden=true;return}
  el.hidden=false;
  el.innerHTML=notes.map(n=>`<div>${n}</div>`).join('');
}

})();
