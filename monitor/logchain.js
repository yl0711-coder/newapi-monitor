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
  // scope 查看范围，互斥单选。默认 error，与本页定位一致（当天故障清单）。
  //   error        上游返回错误（type=5）
  //   stream       流真的出故障：timeout / scanner_error / panic / ping_fail 及未见过的新取值
  //   client_gone  下游客户端主动断连，**独立一档**
  //   billing      扣费未交付，或交付未扣费
  //   anomaly_all  流故障 + 客户断连 + 消费异常
  //   err_anom     错误 + 全部异常（本页能查到的全部问题）
  //
  // client_gone 为什么单独一档：2026-08-24 生产实测当天 1594 条里 **92% 已真交付内容**
  // （平均 324 输出 token），客户拿到部分回答后自己断开，多数不是故障。
  // 与 timeout/panic 混在一档时，25 条真故障会被 1594 条断连彻底淹掉。
  //
  // 没有"全部请求"这一档：本页定位是问题清单，正常请求不看。
  // 要看全量流水去「用户用量」，那才是它的职责。
  scope:'error',
  // asc=false 为时间倒序（最新在上，默认）。排障最常看"刚刚发生了什么"。
  // 切换方向后必须重新从第一页取：游标是沿方向前进的，沿用旧游标会翻到反方向。
  asc:false,
  filters:{group:'',domain:'',channel_id:'',model:'',user:'',keyword:''},
  rows:[],hasMore:false,nextBeforeTs:0,nextBeforeID:0,
  // scopeEcho 是后端回显的生效范围（时间窗、limit 等），与上面的 scope（查看范围）
  // 是两件事。曾经两者同名，后者静默覆盖前者，默认查看范围直接丢失。
  blindSpots:[],scopeEcho:null,note:'',enrichError:'',edgeEvidenceError:'',evidenceMode:'off',evidenceVerified:false,
  // radius 是后端的影响面判读（「看范围」层），**只覆盖单次请求返回的那一页**。
  // radiusStale 在点过「加载更早的记录」后置为 true：那之后 lc.rows 是累积的
  // （第三页时表格 150 行），而 radius 只描述最后一页的 50 行，两个数字摆在
  // 同一屏上会自相矛盾。此时隐藏影响面并说明原因——宁可不给结论，
  // 也不给一个与表格对不上的结论。
  // 不在前端自行重算：判据会变成两份实现，迟早漂。
  // 也不让后端算全筛选范围：那需要另发一条聚合 SQL，多占一次 usageDetailGate
  // （容量 1，与客户 Portal 日志查询共用），排障是内部功能不得挤占客户功能。
  radius:null,radiusStale:false,
  opts:null,           // /logchain/filters 结果，只取一次
  loading:false,abort:null,generation:0,
  expanded:new Set()
};

// 查看范围的合法取值。跨页传入时按此校验，拼错的值不接受（否则会静默落到默认，
// 而人以为自己筛的是别的东西）。前四项映射到后端 anomaly 参数，见 scopeQuery。
const SCOPES=['error','stream','client_gone','billing','anomaly_all','err_anom'];

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
  if(!lc.rows.length||changed||!lc.scopeEcho)load();
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
  // 跨页进来时可指定范围，显式传才覆盖默认。
  if(c.scope&&SCOPES.includes(c.scope)&&lc.scope!==c.scope){lc.scope=c.scope;changed=true}
  else if(c.error_only!=null){ // 兼容旧链接
    // 旧链接的 error_only=false 曾意为"全部请求"，但本页已不提供该档；
    // 落到 err_anom（错误+异常）——它是最接近的语义，且仍是问题清单。
    const v=String(c.error_only)==='true'?'error':'err_anom';
    if(lc.scope!==v){lc.scope=v;changed=true}
  }
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

  document.querySelectorAll('[data-lc-scope]').forEach(btn=>btn.addEventListener('click',()=>{
    const v=btn.dataset.lcScope;
    if(lc.scope===v)return; // 点当前项不重复查库（该泳道与客户 Portal 共用）
    lc.scope=v;
    syncControls();
    load();
  }));

  // 排序：按钮组显式选方向，表头「时间」列点击切换。两处共用 setOrder，
  // 保证按钮高亮、表头箭头、实际查询三者永不脱节。
  document.querySelectorAll('[data-lc-order]').forEach(btn=>btn.addEventListener('click',()=>{
    setOrder(btn.dataset.lcOrder==='asc');
  }));
  const sortTh=$('lcSortTh');
  sortTh?.addEventListener('click',()=>setOrder(!lc.asc));
  sortTh?.addEventListener('keydown',e=>{
    // 表头是 role="button"，键盘可达性要跟上：回车/空格等同点击。
    if(e.key==='Enter'||e.key===' '){e.preventDefault();setOrder(!lc.asc)}
  });

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
    lc.scope='error';lc.date=cstToday();lc.asc=false;
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

// syncDetailHeader 明细列的表头随范围切换。
//
// 为什么必须切：logs.content 在不同日志类型里装的是完全不同的东西——
// type=5 错误里是上游返回的错误原文（排障要看的），
// type=2 消费里是**计费摘要**（"模型倍率 3.00, 分组倍率 1.00"）。
// 而异常行全是 type=2（它们是"成功扣费但有问题"的请求），
// 所以异常行的 content 结构上就不可能是上游错误——上游根本没报错。
// 表头固定写"上游返回原文"，最显眼的位置就会摆着一句无用的计费摘要。
function syncDetailHeader(){
  const th=$('lcDetailTh'), sub=$('lcDetailThSub');
  if(!th)return;
  const anomalyOnly=lc.scope==='stream'||lc.scope==='client_gone'||lc.scope==='billing'||lc.scope==='anomaly_all';
  if(anomalyOnly){
    th.textContent='异常详情';
    if(sub)sub.textContent='流结束原因 / 中断原因 · 点行看全部字段';
  }else if(lc.scope==='err_anom'){
    th.textContent='上游返回 / 异常详情';
    if(sub)sub.textContent='错误行给原文，异常行给结束原因';
  }else{
    th.textContent='上游返回原文';
    if(sub)sub.textContent='未做任何改写 · 点行看全文';
  }
}

// setOrder 切换时间排序方向。
// 必须重新从第一页取：游标是沿排序方向前进的，方向反了还沿用旧游标会翻到反方向、
// 吐出已经看过的行。load() 在 more 为假时会清空 rows 与游标，正好满足这一点。
function setOrder(asc){
  if(lc.asc===asc)return; // 点当前方向不重复查库（那条泳道与客户 Portal 共用）
  lc.asc=asc;
  syncControls();
  load();
}

function syncControls(){
  if($('lcDate')){$('lcDate').value=lc.date;$('lcDate').max=cstToday()}
  document.querySelectorAll('[data-lc-scope]').forEach(btn=>{
    btn.classList.toggle('active',btn.dataset.lcScope===lc.scope);
  });
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
  syncDetailHeader();
  // 排序三处显示保持一致：按钮组高亮、表头箭头、表头副说明。
  document.querySelectorAll('[data-lc-order]').forEach(btn=>{
    btn.classList.toggle('active',(btn.dataset.lcOrder==='asc')===lc.asc);
  });
  const arrow=$('lcSortArrow');
  if(arrow)arrow.textContent=lc.asc?'↑':'↓';
  const hint=$('lcSortHint');
  if(hint)hint.textContent=lc.asc?'最早在上':'最新在上';
  const sortTh=$('lcSortTh');
  if(sortTh)sortTh.setAttribute('aria-label',lc.asc?'时间正序，点击改为倒序':'时间倒序，点击改为正序');
}

// ═══════════ 取数 ═══════════

async function loadFilterOptions(){
  if(lc.opts)return; // 下拉选项与日期无关，只取一次
  try{
    // cache:'no-store' 与后端的 Cache-Control: private, no-store 配套（RB-03）：
    // 响应含渠道名/ID 与上游主域名，不得留在浏览器缓存里被后续会话读到。
    const r=await fetch('/logchain/filters',{cache:'no-store',headers:{'Accept':'application/json'}});
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
  // 查看范围 → 后端参数。error_only 与 anomaly 互斥（后端会拒），只能设其一。
  // err_anom 也走后端（anomaly=err_anom），不在前端滤：
  // 前端过滤会让 limit/has_more/计数三者失准。
  if(lc.scope==='error')q.set('error_only','true');
  else if(lc.scope==='stream')q.set('anomaly','stream');
  else if(lc.scope==='client_gone')q.set('anomaly','client_gone');
  else if(lc.scope==='billing')q.set('anomaly','billing');
  else if(lc.scope==='anomaly_all')q.set('anomaly','all');
  else if(lc.scope==='err_anom')q.set('anomaly','err_anom');
  if(lc.filters.group)q.set('group',lc.filters.group);
  if(lc.filters.domain)q.set('domain',lc.filters.domain);
  if(lc.filters.channel_id)q.set('channel_id',lc.filters.channel_id);
  if(lc.filters.model)q.set('model',lc.filters.model);
  if(lc.filters.keyword)q.set('keyword',lc.filters.keyword);
  // 客户输入：纯数字按 user_id，否则按令牌名模糊查（后端 token_name LIKE）。
  const u=lc.filters.user;
  if(u){ if(/^\d+$/.test(u))q.set('user_id',u); else q.set('token_name',u); }
  if(lc.asc)q.set('order','asc'); // 后端只认 "asc"，缺省即倒序
  q.set('limit','100');
  // 游标必须成对传：排序键是 (created_at, id)，只给 id 定位不到续查位置，
  // 后端会显式拒绝（避免"加载更多"从头再来、出现重复行）。
  if(more&&lc.nextBeforeTs&&lc.nextBeforeID){
    q.set('before_ts',String(lc.nextBeforeTs));
    q.set('before_id',String(lc.nextBeforeID));
  }
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
  if(!more){lc.rows=[];lc.expanded.clear();lc.nextBeforeTs=0;lc.nextBeforeID=0}
  renderStatus(more?'加载更多…':'加载中…');
  try{
    // 同上（RB-03）。这个接口的敏感度更高：含客户标识、令牌名与上游错误原文。
    const r=await fetch('/logchain/requests?'+buildQuery(more),{cache:'no-store',signal:ac.signal,headers:{'Accept':'application/json'}});
    if(r.status===401){location.href='/login';return}
    const text=await r.text();
    if(gen!==lc.generation)return; // 已被更新的请求取代,丢弃本次结果
    let data={};
    try{data=JSON.parse(text)}catch(e){throw new Error(`响应不是 JSON（HTTP ${r.status}）：${text.slice(0,200)}`)}
    if(!r.ok)throw new Error(data.error||`HTTP ${r.status}`);
    lc.rows=more?lc.rows.concat(data.rows||[]):(data.rows||[]);
    lc.hasMore=!!data.has_more;
    lc.nextBeforeTs=+data.next_before_ts||0;
    lc.nextBeforeID=+data.next_before_id||0;
    lc.blindSpots=data.blind_spots||[];
    // more=true 即「加载更早的记录」：本次 radius 只覆盖新取的这一页，
    // 而表格显示的是累积行，故标记为失效而不是用它覆盖。
    if(more){lc.radiusStale=true}
    else{lc.radius=data.blast_radius||null;lc.radiusStale=false}
    lc.scopeEcho=data.scope||null;
    lc.note=data.note||'';
    lc.enrichError=data.channel_enrich_error||'';
    lc.edgeEvidenceError=data.edge_evidence_error||'';
    lc.evidenceMode=data.nginx_evidence_mode||'off';
    lc.evidenceVerified=!!data.nginx_evidence_verified;
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
// contentCell 明细列主内容。按行的性质决定显示什么，不按当前筛选——
// "错误+异常"混排时同一列里两种行并存，各自显示对自己有意义的东西。
//
// 错误行(type=5)：content 是上游返回的错误原文，直接显示。
// 异常行(type=2 且有标签)：content 是计费摘要（"模型倍率 3.00..."），
//   对排障毫无用处；真正的诊断信息是 end_reason / end_error。
//   计费摘要退到展开区（核对计价时才有用），不占主列。
// faultCell 疑似责任方。
//
// ★ 这一列是**推断**，与其它列性质不同 ★
// 其余列都在转述 new-api / 上游写下的事实；这一列是我方规则对事实的解读。
// 因此三件事必须同时做到，缺一件就会让人把推断当结论：
//   1. 依据（fault_why）必须可见 —— 放在 title 里，悬停即见
//   2. 可信度必须显式标出 —— low 的加「?」并降低视觉权重
//   3. 「待判」必须如实显示，不能为了好看凑一个答案
//
// 判错的代价不是"少看到信息"，而是**找错人**：判成我方会让人去改自己的配置，
// 而问题其实在上游（这个错我在评估阶段真犯过，连错三次）。
const FAULT_LABEL={
  upstream:{t:'上游',c:'lc-fault-up'},
  ours:{t:'我方',c:'lc-fault-ours'},
  downstream:{t:'下游',c:'lc-fault-down'},
  unknown:{t:'待判',c:'lc-fault-unknown'}
};

// 上游关联的置信度标签。
//
// ★ 四档的措辞与配色必须让人一眼分出「证据」和「推断」★
// exact 是同一请求的铁证（双方错误原文里嵌着同一个模型商 request id）；
// probable 只是时间窗内唯一，可能认错。把后者当前者用，会照着别的请求的
// 上游日志去解释眼前的故障——比没有关联更糟。
// 所以 exact 用绿（放心用），probable 用黄（留个心眼），ambiguous 用灰（别用）。
const CORRELATE_LABEL={
  exact:{t:'精确匹配',cls:'lc-corr-exact',
    tip:'铁证：双方错误原文里嵌着同一个模型商 request id，是同一次请求的两侧视角'},
  probable:{t:'高置信推断',cls:'lc-corr-probable',
    tip:'推断，非铁证：按模型名 + 状态码 + 时间窗匹配且窗内唯一。实测 10 秒窗内约 82% 唯一，即约两成可能认错'},
  // not_applicable 与 none 必须分开显示。
  // none 会让人去查采集是不是漏了；而这一档是上游自身没有记录，那趟核对必然白跑。
  // 实测依据：我方 08-26 23:52 那条 524，上游 507 条日志里 524 一条都没有——
  // 524 由 Cloudflare 在上游应用之前就返回了，上游从未看到这次请求。
  not_applicable:{t:'上游无此记录（非采集缺失）',cls:'lc-corr-na',
    tip:'该错误由上游前置 CDN 产生，请求未到达上游应用，上游自身不会有对应日志。不必去查采集'},
  none:{t:'未找到对应',cls:'lc-corr-none',
    tip:'上游日志里没有相近时刻的同模型同状态码错误。可能是采集未覆盖该上游、或时钟偏差过大'},
  ambiguous:{t:'无法唯一对应',cls:'lc-corr-ambiguous',
    tip:'上游在相近时刻有多条同模型同状态码的错误。宁可不给结论也不给错结论，故只报候选条数'}
};
function faultCell(r){
  if(!r.fault)return '<span class="lc-sub">—</span>';
  const m=FAULT_LABEL[r.fault];
  // 后端将来新增取值时按原值显示，不吞掉——归因最怕"没见过的情况被藏起来"。
  const label=m?m.t:r.fault;
  const cls=m?m.c:'lc-fault-unknown';
  const why=r.fault_why||'';
  const conf=r.fault_confidence||'';
  // 低可信度加问号并置灰：403/413 这类样本不足的映射不能与 502/503 同等呈现。
  const mark=conf==='low'?'<span class="lc-fault-q" title="可信度低：样本不足或判据本身模糊">?</span>':'';
  const dim=conf==='low'?' lc-fault-dim':'';
  const confText={high:'可信度高',mid:'可信度中',low:'可信度低（样本不足）',none:''}[conf]||'';
  const tip=[why,confText].filter(Boolean).join('\n');
  // 有上游关联时加个小圆点：不展开也能看出这行有上游侧证据可查。
  // 只对 exact / probable 加——ambiguous 没有可看的内容，加了是空跑一趟。
  // 挂在责任方这一列而不是单开一列：上游日志正是复核归因结论的材料，
  // 而表格宽度已经很紧，单开一列要挤掉别的。
  const um=r.upstream_match;
  let dot='';
  if(um&&(um.confidence==='exact'||um.confidence==='probable')){
    const dt=um.confidence==='exact'
      ? {c:'lc-corr-dot-exact',t:'有上游侧日志可对照（精确匹配，铁证）'}
      : {c:'lc-corr-dot-probable',t:'有上游侧日志可对照（高置信推断，约两成可能认错）'};
    dot=`<span class="lc-corr-dot ${dt.c}" title="${esc(dt.t)}"></span>`;
  }
  return `<span class="lc-fault-tag ${cls}${dim}" title="${esc(tip)}">${esc(label)}${mark}</span>${dot}`;
}

function contentCell(r){
  const isAnom=(r.anomaly_tags||[]).length>0;
  if(r.type===2&&isAnom){
    const er=r.end_reason||'';
    const ee=r.end_error||'';
    if(er&&er!=='eof'){
      // 流中断：结束原因是主信息，中断原文补充说明。
      return `<div class="lc-content">${esc(er)}${ee?' · '+esc(ee):''}</div>`;
    }
    if(er==='eof'&&r.stream_error_count>0){
      return `<div class="lc-content">流内出错 ${nfmt(r.stream_error_count)} 次（最终正常结束）</div>`;
    }
    // 纯消费异常（没有流问题）：没有可显示的诊断文本，说清事实即可，
    // 不要把计费摘要摆上来冒充诊断信息。
    return `<div class="lc-sub">计费与交付不一致，详见标签</div>`;
  }
  const c=r.content||'';
  if(!c)return `<span class="lc-sub">（无内容）</span>`;
  return `<div class="lc-content">${esc(c)}</div>`;
}

// anomalyTagsHTML 把后端给的 anomaly_tags 渲染成标签。
// 标签由后端判定，前端不自己算——两处各判一次一旦口径不一致，
// 会出现"筛出来了但没标签"这种自相矛盾的结果。
// client_gone 用中性色而非告警色：它多数不是故障（实测 92% 已真交付内容），
// 用红/黄会让人以为出了问题。真故障(stream)才用告警色。
const TAG_LABEL={
  client_gone:{t:'客户端断连',c:'lc-tag-gone'},
  stream:{t:'流未正常结束',c:'lc-tag-stream'},
  billing_unpaid:{t:'扣费未交付',c:'lc-tag-unpaid'},
  billing_free:{t:'交付未扣费',c:'lc-tag-free'}
};
function anomalyTagsHTML(r){
  const tags=r.anomaly_tags||[];
  if(!tags.length)return '';
  return tags.map(t=>{
    const m=TAG_LABEL[t];
    // 后端将来新增标签时按原值显示，不吞掉——排障最怕没见过的情况被藏起来。
    return m?`<span class="lc-tag ${m.c}">${esc(m.t)}</span>`
            :`<span class="lc-tag lc-tag-stream">${esc(t)}</span>`;
  }).join('');
}

// endReasonHTML 补充行。
//
// 异常行的 end_reason 已由 contentCell 作为主内容显示，这里不再重复；
// 只补 client_gone 的耗时——耗时 45s 的断连大概率是上游拖慢把客户等跑了，
// 耗时 2s 的大概率是客户主动取消。这是区分二者唯一的旁证，值得单独一行。
//
// 错误行(type=5)若也带 end_reason（错误发生在流传输中），照常显示原值。
function endReasonHTML(r){
  const er=r.end_reason||'';
  if(!er||er==='eof')return '';
  const isAnom=(r.anomaly_tags||[]).length>0;
  if(r.type===2&&isAnom){
    // 主内容已含 end_reason，这里只给耗时旁证。
    return er==='client_gone'&&r.use_time>0
      ? `<div class="lc-endreason bad" title="耗时长 → 更可能是上游拖慢；耗时短 → 更可能是客户主动取消">耗时 ${esc(dur(r.use_time))}</div>`
      : '';
  }
  return `<div class="lc-endreason bad" title="流结束原因原值（未归类）">${esc(er)}</div>`;
}

function rowHTML(r){
  const id=String(r.id);
  const isErr=r.type===5;
  const isAnom=(r.anomaly_tags||[]).length>0;
  const open=lc.expanded.has(id);
  // 错误红底、异常黄底。错误优先：一条 type=5 即使带异常标签也按错误显示，
  // 因为它是明确失败，比"成功但有问题"更紧急。
  // 只带 client_gone 标签的行不标黄底：黄底是"成功了但有问题、要核查"的信号，
  // 而客户端断连多数是客户自己的正常行为（实测约 92% 已交付内容）。
  // 全标黄会让真正需要核查的行（流故障、消费异常）失去视觉区分度。
  const tags=r.anomaly_tags||[];
  const onlyClientGone=tags.length>0&&tags.every(t=>t==='client_gone');
  const cls=[isErr?'lc-err':((isAnom&&!onlyClientGone)?'lc-anom':''),open?'lc-open':''].filter(Boolean).join(' ');
  const tds=[
    `<td class="lc-cust"><div>${esc(r.member||('#'+r.user_id))}</div><div class="lc-sub">ID ${esc(r.user_id)}</div></td>`,
    `<td>${esc(r.token_name||'—')}</td>`,
    `<td>${r.group?`<span class="gtag">${esc(r.group)}</span>`:'—'}</td>`,
    `<td>${modelCell(r)}</td>`,
    `<td class="lc-up">${upstreamCell(r)}</td>`,
    `<td class="lc-fault">${faultCell(r)}</td>`,
    `<td class="lc-ct">${contentCell(r)}${endReasonHTML(r)}${anomalyTagsHTML(r)}</td>`,
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
  // addHTML 与 add 的唯一差别：value 不转义。
  // **只允许传本文件内构造的固定片段**（如置信度色块），绝不可传接口返回的字段值——
  // 那些要走 add。上游错误原文里可能带 < > 之类字符，不转义就是 XSS 面。
  const addHTML=(k,html,title)=>{if(html)kv.push(`<div class="lc-kv"><span${title?` title="${esc(title)}"`:''}>${esc(k)}</span><b>${html}</b></div>`)};
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
  // 流状态三项。end_reason 原值直出；eof 也显示（展开区是细节视图，
  // 明确告知"正常结束"比留空更有用）。
  if(r.end_reason)add('流结束原因',r.end_reason,'原值，未归类。eof=正常结束，client_gone=下游客户端断连');
  if(r.stream_error_count)add('流错误计数',nfmt(r.stream_error_count));
  // 归因放在展开区里完整展示依据：表格列受宽度限制只能靠 title，
  // 而依据是复核这条推断的唯一材料，不该只在悬停时才可见。
  if(r.fault){
    const m=FAULT_LABEL[r.fault];
    const confText={high:'可信度高',mid:'可信度中',low:'可信度低（样本不足或判据模糊）',none:'—'}[r.fault_confidence]||'—';
    add('疑似责任方',(m?m.t:r.fault)+'（'+confText+'）',
      '这是我方规则对事实的**推断**，不是 new-api 或上游写下的事实。判断依据见下一行');
    if(r.fault_why)add('归因依据',r.fault_why,'规则据此得出上面的结论，请据此复核');
  }
  // 上游侧视角。紧接归因之后：它是归因的证据来源——上游自己怎么记这次失败，
  // 比我方对状态码和原文的解读更有力。
  //
  // ★ 四档必须视觉上分得开 ★
  // exact 是铁证（双方嵌同一模型商 id），probable 只是推断。
  // 混在一起显示会让人拿推断当证据，照着别的请求的上游日志解释眼前的故障。
  const um=r.upstream_match;
  // 四档全显示，含 none。
  //
  // ★ 原先跳过 none，那是错的 ★
  // 「没找到对应」与「压根没查（采集未开）」在页面上表现一致——都是什么都不显示。
  // 于是看的人无法判断该不该去查采集。显式给出 none 才区分得开，
  // 而 not_applicable 更进一步说明「不必去查」。
  if(um&&um.confidence){
    const c=CORRELATE_LABEL[um.confidence]||{t:um.confidence,cls:''};
    addHTML('上游关联',`<span class="lc-corr ${c.cls}">${esc(c.t)}</span>`,c.tip);
    if(um.why)add('关联依据',um.why,'据此判断该不该相信下面的上游信息');
    if(um.confidence==='ambiguous'){
      // 多义时不给具体某条——给了就是在猜。但「候选涉及几个上游渠道」是真信息：
      // 都落在同一个渠道说明那个渠道在批量出错；散在多个渠道则指向上游整体。
      add('候选条数',nfmt(um.candidate_count)+' 条',
        '上游在相近时刻有多条同模型同状态码的错误，无法唯一对应，故不展示具体内容');
      if(um.candidate_channels)add('候选涉及渠道',um.candidate_channels,
        '这些候选分布在上游的哪几个渠道。集中在一个说明该渠道在批量出错');
    }else{
      if(um.upstream_channel_name)add('上游用的渠道',um.upstream_channel_name,
        '**上游自己**用了它哪个渠道去打——我方日志里没有这个信息');
      // ★★ 状态码 / 错误码 / 错误类型 / 原文只在**与我方不一致**时才显示 ★★
      //
      // 2026-08-28 实测 33 条 exact 匹配：这四项两侧**全部逐字相同**。
      // 机制是上游把返回给我方的响应体原样记进自己的日志，我方也原样记进
      // content——两边记的是同一个字符串。所以无条件显示等于让人把同一句话
      // 读两遍，还会误以为拿到了上游的内部诊断。
      //
      // 但**不一致时必须显示**：那说明上游记的与它告诉我方的不是一回事，
      // 那种矛盾本身就是重要线索，不能因为"通常相同"就藏起来。
      if(um.upstream_status_code&&um.upstream_status_code!==r.upstream_status_code){
        add('上游状态码（与我方不一致）',String(um.upstream_status_code),
          '我方记的是 '+(r.upstream_status_code||'—')+'，上游自己记的是这个');
      }
      if(um.upstream_error_code&&um.upstream_error_code!==(r.upstream_error_code||'')){
        add('上游错误码（与我方不一致）',um.upstream_error_code,
          '我方记的是 '+(r.upstream_error_code||'—'));
      }
      // 上游侧原文不放这里：它是长文本，塞进 kv 行会撑破布局。
      // 改为紧跟「上游返回原文」下方的独立折叠块，见 upstreamRawBlock。
      // 耗时差是这一跳的开销。只在有差值时给——相同就没有信息量。
      const dt=(r.use_time||0)-(um.upstream_use_time||0);
      if(um.upstream_use_time&&dt!==0){
        add('两侧耗时',dur(r.use_time)+' / 上游 '+dur(um.upstream_use_time),
          '差值 '+dur(Math.abs(dt))+'，即我方到上游这一跳的开销');
      }
    }
  }
  add('渠道 ID',r.channel_id?String(r.channel_id):'（未打到渠道）');
  if(r.channel_vendor)add('厂商',r.channel_vendor);
  if(r.upstream_domain)add('上游主域名',r.upstream_domain);

  const edge=r.edge_evidence||null;
  let edgeBlock='';
  if(edge){
    const verified=!!r.edge_evidence_verified;
    const label=verified?'已验证入口证据':'灰度入口证据（关联未验收）';
    const statuses=(()=>{try{return JSON.parse(edge.upstream_statuses||'[]').join(' → ')}catch(_){return ''}})();
    const rows=[
      ['节点',edge.node||'—'],['入口状态',String(edge.status||'—')],
      ['上游状态序列',statuses||String(edge.upstream_status||'—')],
      ['入口总耗时',(edge.request_ms||0)+'ms'],['上游耗时',edge.upstream_present?(edge.upstream_ms||0)+'ms':'—'],
      ['连接/首包',(edge.connect_ms||0)+'ms / '+(edge.header_ms||0)+'ms'],
      ['边缘交付',edge.completion||'—']
    ];
    edgeBlock=`<section class="lc-edge ${verified?'verified':'pilot'}"><div class="lc-raw-head"><span>${esc(label)}</span></div>`+
      `<div class="lc-kvs">${rows.map(x=>`<div class="lc-kv"><span>${esc(x[0])}</span><b>${esc(x[1])}</b></div>`).join('')}</div>`+
      `${verified?'':'<div class="lc-sub">pilot 仅用于核对 Request ID 覆盖率，不能据此自动改变责任归因。</div>'}</section>`;
  }

  const raw=r.content||'';
  // 标题按行的性质给：type=2 的 content 是计费摘要，不是上游返回。
  // 主列已让位给诊断信息，这里如实说明它是什么，避免被误读成上游报错。
  const rawTitle=r.type===2?'计费摘要（logs.content，非上游返回）':'上游返回原文（未做任何改写）';
  const rawBlock=raw
    ? `<div class="lc-raw-head">
         <span>${esc(rawTitle)}</span>
         <button type="button" class="lc-copy" data-lc-copy="${esc(raw)}">复制</button>
       </div>
       <pre class="lc-raw">${esc(raw)}</pre>`
    : `<div class="lc-sub" style="margin-top:8px">这条记录没有 content。</div>`;

  // 上游侧的错误日志原文，紧跟我方原文之后——两者是同一次失败的两侧视角，
  // 挨着放才能一眼对比。
  //
  // ★ 用折叠而不是「相同就隐藏」★
  // 实测 33 条 exact 匹配里两侧原文**逐字相同**，曾据此改成只在不一致时显示。
  // 但那样一行凭空消失，与「压根没拿到上游日志」无法区分——正是
  // docs/aimustkonw.md 那条「缺失绝不显示为零」要防的。
  // 折叠既不占地方，标题又如实说明了核对结果。
  const um2=r.upstream_match;
  const uc=um2&&um2.upstream_content||'';
  const ucSame=uc&&uc.trim()===(r.content||'').trim();
  let upstreamRawBlock='';
  if(uc){
    const head=ucSame
      ? '上游侧错误日志原文（已逐字核对，与我方一致）'
      : '⚠ 上游侧错误日志原文（与我方不一致，值得追查）';
    upstreamRawBlock=`<details class="lc-upraw${ucSame?'':' lc-upraw-diff'}">`+
      `<summary class="lc-upraw-head">${esc(head)}</summary>`+
      `<pre class="lc-raw">${esc(uc)}</pre></details>`;
  }

  // end_error 是流中断的错误原文（如 "context canceled"）。单独一块展示，
  // 不混进上面的 content —— 两者来源不同：content 是 new-api 记的上游返回，
  // end_error 是流传输层的失败原因。
  //
  // **只展示，绝不参与判定**：它是自由文本，可能含 "panic" 等词，
  // 参与判定会误命中（sampler.go 的注释记录了这个坑）。
  const endErrBlock=r.end_error
    ? `<div class="lc-raw-head" style="margin-top:10px">
         <span>流中断原因（仅供参考，不参与异常判定）</span>
         <button type="button" class="lc-copy" data-lc-copy="${esc(r.end_error)}">复制</button>
       </div>
       <pre class="lc-raw">${esc(r.end_error)}</pre>`
    : '';

  // 用 data 属性 + 事件委托，不用内联 onclick：
  // 内联写法要把域名插进 HTML 属性里的 JS 字符串字面量，多一层转义面；
  // 且与复制按钮的处理方式不一致，容易漏改。
  const jump=r.upstream_domain
    ? `<button type="button" class="lc-jump" data-lc-jump="${esc(r.upstream_domain)}">在渠道管理中查看 ${esc(r.upstream_domain)}</button>`
    : '';

  return `<tr class="lc-detail"><td colspan="8">
    <div class="lc-kvs">${kv.join('')}</div>
    ${edgeBlock}
    ${rawBlock}
    ${upstreamRawBlock}
    ${endErrBlock}
    ${jump?`<div style="margin-top:10px">${jump}</div>`:''}
  </td></tr>`;
}

function render(){
  clearError();
  const body=$('lcTableBody');
  if(!body)return;

  const rows=lc.rows;
  const errs=rows.filter(r=>r.type===5).length;
  const anoms=rows.filter(r=>(r.anomaly_tags||[]).length>0).length;

  // 计数按当前范围说人话。err_anom 混排两类，给出各自条数；
  // 筛定单一类时占比无意义，直接报条数。
  const counter=$('lcCounter');
  const more=lc.hasMore?'（还有更多）':'';
  const LABEL={error:'错误',stream:'流故障',client_gone:'客户端断连',billing:'消费异常',anomaly_all:'异常'};
  if(counter){
    if(!rows.length)counter.textContent='';
    else if(lc.scope==='err_anom')counter.innerHTML=`本页 <b>${nfmt(rows.length)}</b> 条问题：`+
      `<b class="lc-errnum">${nfmt(errs)}</b> 条错误、`+
      `<b style="color:var(--yellow)">${nfmt(anoms)}</b> 条异常${more}`;
    else counter.innerHTML=`本页 <b>${nfmt(rows.length)}</b> 条${LABEL[lc.scope]||'记录'}${more}`;
  }

  if(!rows.length){
    body.innerHTML=`<tr><td colspan="8" class="lc-empty">${emptyText()}</td></tr>`;
  }else{
    // 顺序完全由后端 ORDER BY 决定（created_at 为首要键），前端不再排一遍：
    // 两处各排一次一旦口径不一致，翻页拼接就会出现看不懂的乱序。
    body.innerHTML=rows.map(rowHTML).join('');
  }

  const moreBtn=$('lcMore');
  if(moreBtn){
    moreBtn.hidden=!lc.hasMore;
    moreBtn.disabled=lc.loading;
    // 文案必须跟随排序方向：正序翻页取的是更"晚"的记录，
    // 固定写"更早"会与实际行为相反。
    moreBtn.textContent=lc.asc?'加载更晚的记录':'加载更早的记录';
  }

  renderRadius();
  renderBlindSpots();
  renderNotes();
  renderStatus('');
}

function emptyText(){
  const day=lc.date===cstToday()?'今天':lc.date;
  const filtered=!!(lc.filters.group||lc.filters.domain||lc.filters.channel_id||lc.filters.model||lc.filters.user||lc.filters.keyword);
  const what={
    error:'错误请求',
    stream:'流故障请求',
    client_gone:'客户端断连的请求',
    billing:'消费异常请求',
    anomaly_all:'异常请求',
    err_anom:'错误或异常请求'
  }[lc.scope]||'记录';
  const hint=filtered
    ? '<div class="lc-sub" style="margin-top:6px">当前有筛选条件，可点"重置"清掉。</div>'
    : '';
  // 查不到最容易被读成"没发生过"，而前置拒绝根本不写 logs。
  // 本页只有问题清单（没有"全部请求"档），所以任何范围下都该提示。
  const blindHint='<div class="lc-sub" style="margin-top:6px">注意：这不代表没有客户遇到问题 —— 限流/无可用渠道这类前置拒绝不写日志，见下方说明。</div>';
  return `<b>${esc(day)}没有查到${what}。</b>`+hint+blindHint;
}

// renderBlindSpots 盲区提示。默认收起（自用系统，展开占地方不顺眼），
// 但**标题始终可见**——它的价值在于"你没主动去看时也知道它存在"。
// 展开状态记在 localStorage，不必每次点开。
//
// 用 <details> 而非自己写折叠：原生元素自带键盘可达性与 aria 语义。
// 这个功能最可能造成的实际损害是：客户说"我请求根本发不出去"，
// 你在这里查不到，于是判断他在瞎说。所以这段话必须一直在眼前。
const BLIND_OPEN_KEY='nexusapi-monitor-logchain-blind-open';

// SHAPE_LABEL 形状取值的中文说明。取值与 logchain_radius.go 的常量一一对应，
// 那边加了取值这里必须同步，否则页面会显示原始英文枚举。
const SHAPE_LABEL={
  single_channel:'集中在单个渠道',
  single_customer:'集中在单个客户',
  single_domain:'集中在单个上游域名',
  single_model:'集中在单个模型',
  widespread:'分散，无明显集中',
  insufficient:'样本不足，不做判读'
};

// RADIUS_DIM 四个维度的表头。顺序即展示顺序：渠道和客户在前，
// 因为排障最常问的是"是这个渠道坏了，还是这个客户在做异常请求"。
const RADIUS_DIM=[
  ['by_channel','渠道','受影响客户数'],
  ['by_customer','客户','涉及渠道数'],
  ['by_domain','上游域名','受影响客户数'],
  ['by_model','模型','受影响客户数']
];

function renderRadius(){
  const el=$('lcRadius');
  if(!el)return;
  // 翻页后隐藏：radius 只覆盖最后一页，表格是累积的，两者对不上。
  // 明确告知而不是静默留空——静默会让人以为"这次没算出影响面"。
  if(lc.radiusStale){
    el.hidden=false;
    el.innerHTML=`<div class="lc-radius-stale">已加载更早的记录，影响面判读仅覆盖单页，`+
      `与当前表格行数不一致，故不展示。重新查询可再次判读。</div>`;
    return;
  }
  const br=lc.radius;
  if(!br||!br.shape){el.hidden=true;return}
  el.hidden=false;

  const dims=RADIUS_DIM.map(([key,dimName,spreadName])=>{
    const dim=br[key]||{};
    const items=dim.items;
    if(!items||!items.length)return '';
    let rows=items.map(it=>
      `<tr><td class="lc-radius-key">${esc(it.key)}</td>`+
      `<td class="lc-radius-num">${nfmt(it.count)}</td>`+
      `<td class="lc-radius-num">${nfmt(it.spread)}</td></tr>`
    ).join('');
    // 被截断的部分必须显式给出，否则这张表看起来就像"问题只涉及这 5 项"，
    // 而形状结论用的是全量（会说"分散在 12 个渠道"），两者对不上。
    // Spread 一列给"—"而不是数字：Spread 是去重计数，跨项相加会重复计数。
    if(dim.other_items>0){
      rows+=`<tr class="lc-radius-other"><td class="lc-radius-key">`+
        `其余 ${nfmt(dim.other_items)} 项</td>`+
        `<td class="lc-radius-num">${nfmt(dim.other_count)}</td>`+
        `<td class="lc-radius-num" title="去重计数不可跨项相加">—</td></tr>`;
    }
    return `<div class="lc-radius-dim">`+
      `<table class="lc-radius-table"><thead><tr>`+
      `<th>${esc(dimName)}</th><th>问题数</th><th>${esc(spreadName)}</th>`+
      `</tr></thead><tbody>${rows}</tbody></table></div>`;
  }).join('');

  const label=SHAPE_LABEL[br.shape]||br.shape;
  el.innerHTML=`<details class="lc-radius-details">`+
    `<summary class="lc-radius-head">影响面：<b>${esc(label)}</b>`+
    `<span class="lc-radius-sub">仅本页 ${nfmt(br.rows)} 条问题</span></summary>`+
    `<div class="lc-radius-why">${esc(br.shape_why||'')}</div>`+
    `<div class="lc-radius-dims">${dims}</div>`+
    `</details>`;
}

function renderBlindSpots(){
  const el=$('lcBlind');
  if(!el)return;
  if(!lc.blindSpots.length){el.hidden=true;return}
  el.hidden=false;
  let open=false;
  try{open=localStorage.getItem(BLIND_OPEN_KEY)==='1'}catch(e){}
  el.innerHTML=`<details class="lc-blind-details"${open?' open':''}>`+
    `<summary class="lc-blind-head">这个页面查不到的情况（${lc.blindSpots.length} 项）</summary>`+
    `<ul class="lc-blind-list">${lc.blindSpots.map(s=>`<li>${esc(s)}</li>`).join('')}</ul>`+
    `</details>`;
  el.querySelector('details')?.addEventListener('toggle',e=>{
    try{localStorage.setItem(BLIND_OPEN_KEY,e.target.open?'1':'0')}catch(err){}
  });
}

function renderNotes(){
  const el=$('lcNotes');
  if(!el)return;
  const notes=[];
  if(lc.note)notes.push(esc(lc.note));
  if(lc.enrichError)notes.push(`渠道信息补全失败（明细仍有效）：${esc(lc.enrichError)}`);
  if(lc.edgeEvidenceError)notes.push(`Nginx 入口证据补全失败（明细仍有效）：${esc(lc.edgeEvidenceError)}`);
  if(lc.evidenceMode==='pilot')notes.push('Nginx 请求证据处于 pilot：只核对关联覆盖率，尚不作为责任结论。');
  // 后端可能收敛了范围（跨度截断/limit 上限），回显出来，避免以为筛选原样生效。
  if(lc.scopeEcho&&lc.scopeEcho.from&&lc.scopeEcho.to)notes.push(`查询范围：${esc(lc.scopeEcho.from)} ～ ${esc(lc.scopeEcho.to)}（CST）`);
  // 跨度被收窄。警告级：不说清的话，人会以为"这段时间里就这些"，
  // 据此得出"那几天没有请求"的错误结论。
  //
  // 原因可能有多条（跨度硬上限 + 关键词上限叠加），全部列出：
  // 只说最后一条会让人看到"31 天收窄到 3 天"，而他根本没要过 31 天。
  const cap=lc.scopeEcho&&lc.scopeEcho.span_capped;
  if(cap&&cap.requested_days>cap.effective_days){
    const why=(cap.reasons||[]).map(r=>esc(r)).join('；');
    let msg=`<span class="lc-note-warn">查询跨度已从 ${nfmt(cap.requested_days)} 天收窄至 `+
      `${nfmt(cap.effective_days)} 天。</span>`;
    if(why)msg+=`原因：${why}。`;
    msg+=`要查更早的记录，请缩小 to 端后分多次查询。`;
    notes.push(msg);
  }
  // 关键词把口径收窄到错误行（P2-02）。警告级：不说的话，人会以为
  // "消费行里没有匹配的"，而实际是压根没查那一部分。
  const kwScope=lc.scopeEcho&&lc.scopeEcho.keyword_scoped_to_errors;
  if(kwScope){
    notes.push(`<span class="lc-note-warn">关键词搜索已限定为错误行（type=5），`+
      `消费行未纳入本次搜索。</span>原因：${esc(kwScope.reason||'')}。`);
  }
  if(!notes.length){el.hidden=true;return}
  el.hidden=false;
  el.innerHTML=notes.map(n=>`<div>${n}</div>`).join('');
}

})();
