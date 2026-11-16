(function(){
'use strict';

// 使用日志（tab-usage-logs）：按 new-api v1.0.0-rc.48 管理员“使用日志 → 通用日志”的行为
// 重新实现（未拷贝其源码，new-api 为 AGPL），数据来自 /usage-logs/*（只读 new-api 从库）。
// 与原页面一致的要点：默认时间=今天 00:00:00 至“现在+1 小时”；只有点“搜索”/回车才查询；
// 统计忽略类型与请求 ID，RPM/TPM 为最近 60 秒；只有用户名、模型名支持 %；
// 每页默认 100（手机 20），最多 100；金额按站点货币设置显示（≥1 保留 4 位，<1 保留 6 位）。
// 颜色沿用 Monitor 主题变量。

const $=id=>document.getElementById(id);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const TYPE_LABELS={0:'未知',1:'充值',2:'消耗',3:'管理',4:'系统',5:'错误',6:'退款',7:'登录'};
const TYPE_CLASS={0:'grey',1:'cyan',2:'green',3:'orange',4:'purple',5:'red',6:'blue',7:'teal'};
const DISPLAYABLE=new Set([0,2,5,6]);
const TIMING=new Set([2,5]);
const COLUMNS=[['channel','渠道'],['user','用户'],['token','令牌'],['model','模型'],['stream','流'],['tokens','Token'],['cost','费用'],['time','耗时'],['detail','详情']];
const COLS_KEY='usage-logs:common:root:column-visibility';
const PAGE_SIZE_KEY='page-size';
const MOBILE=()=>window.matchMedia('(max-width:640px)').matches;
// 线性图标（自绘 SVG，统一 1.8 线宽，颜色跟随 currentColor）
const svg=(d,cls)=>`<svg class="ul-svg${cls?' '+cls:''}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${d}</svg>`;
const ICON={
  calendar:'<rect x="3" y="5" width="18" height="16" rx="2"/><path d="M16 3v4M8 3v4M3 10h18"/>',
  key:'<circle cx="8" cy="15" r="4"/><path d="M10.8 12.2 20 3M16 7l3 3M14 9l2 2"/>',
  crown:'<path d="M3 8l4 4 5-7 5 7 4-4-2 11H5z"/>',
  wallet:'<path d="M19 7V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-3"/><path d="M21 9h-6a3 3 0 0 0 0 6h6z"/>',
  wrench:'<circle cx="15.5" cy="8.5" r="4.5"/><path d="M12.3 11.7 4 20"/>',
  eye:'<path d="M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/>',
  eyeOff:'<path d="M3 3l18 18M10.6 5.1A10.8 10.8 0 0 1 12 5c6.4 0 10 7 10 7a17 17 0 0 1-3.2 4.1M6.6 6.6C3.9 8.3 2 12 2 12s3.6 7 10 7a9.6 9.6 0 0 0 5.4-1.6M9.9 9.9a3 3 0 0 0 4.2 4.2"/>',
  branch:'<circle cx="6" cy="5" r="2"/><circle cx="6" cy="19" r="2"/><circle cx="18" cy="8" r="2"/><path d="M6 7v10M18 10c0 4-6 3-11.5 7"/>',
  route:'<circle cx="5" cy="18" r="2"/><circle cx="19" cy="6" r="2"/><path d="M7 18h7a3 3 0 0 0 0-6h-4a3 3 0 0 1 0-6h7"/>',
  spark:'<path d="M12 3l1.8 5.2L19 10l-5.2 1.8L12 17l-1.8-5.2L5 10l5.2-1.8z"/>',
  alert:'<path d="M12 3 2 20h20z"/><path d="M12 10v4M12 17h.01"/>',
  chevDown:'<path d="M6 9l6 6 6-6"/>',
  chevUp:'<path d="M6 15l6-6 6 6"/>',
  copy:'<rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/>',
  columns:'<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M9 4v16M15 4v16"/>',
  search:'<circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/>',
};

const st={
  inited:false,meta:null,items:[],total:0,page:1,pageSize:0,loading:false,
  applied:null,draft:null,expanded:false,masked:false,stat:null,seq:0,statSeq:0,
  cols:loadCols(),datePop:false,
  pageCursors:new Map(),
  totalKnown:false,hasMore:false,countTask:null,countError:'',statRequested:false,
  // Exact list total of this search, reused while paging the same filter.
  totalKey:null,totalCached:null,
};

function loadCols(){
  try{const v=JSON.parse(localStorage.getItem(COLS_KEY)||'{}');return v&&typeof v==='object'?v:{}}catch{return{}}
}
function saveCols(){try{localStorage.setItem(COLS_KEY,JSON.stringify(st.cols))}catch{}}
function colOn(k){return st.cols[k]!==false}

// ---------- 时间 ----------
const pad=n=>String(n).padStart(2,'0');
function fmtDT(ms){if(!ms&&ms!==0)return'-';const d=new Date(ms);return`${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`}
function startOfDay(d){const x=new Date(d);x.setHours(0,0,0,0);return x.getTime()}
function endOfDay(d){const x=new Date(d);x.setHours(23,59,59,999);return x.getTime()}
function defaultRange(){const now=Date.now();return{start:startOfDay(now),end:now+3600*1000}}
function toLocalInput(ms){if(!ms)return'';const d=new Date(ms);return`${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`}
function fromLocalInput(v){if(!v)return 0;const t=new Date(v).getTime();return Number.isFinite(t)?t:0}
function fmtSeconds(s){
  if(!(s>=0))return'-';
  if(s<60)return`${(Math.round(s*10)/10).toFixed(1)}s`;
  const m=Math.floor(s/60),r=Math.round(s-m*60);
  return r===60?`${m+1}m 0s`:`${m}m ${r}s`;
}

// ---------- 金额 ----------
function meta(){return st.meta||{quota_per_unit:500000,quota_display_type:'USD',usd_exchange_rate:7.3,custom_currency_symbol:'¤',custom_currency_exchange_rate:1,groups:[],has_enabled_subscription_plans:false}}
function currency(forPrice){
  const m=meta();let t=String(m.quota_display_type||'USD').toUpperCase();
  if(forPrice&&t==='TOKENS')t='USD';
  if(t==='CNY')return{type:t,symbol:'¥',rate:m.usd_exchange_rate>0?m.usd_exchange_rate:1};
  if(t==='CUSTOM')return{type:t,symbol:m.custom_currency_symbol||'¤',rate:m.custom_currency_exchange_rate>0?m.custom_currency_exchange_rate:1};
  if(t==='TOKENS')return{type:t};
  return{type:'USD',symbol:'$',rate:1};
}
function fmtAmount(value,cur){
  const digits=Math.abs(value)>=1?4:6;
  let v=value;const minAbs=Math.pow(10,-digits);
  if(v!==0&&Math.abs(v)<minAbs)v=v<0?-minAbs:minAbs;
  let s=v.toFixed(digits).replace(/\.?0+$/,'');
  if(s==='-0')s='0';
  const neg=s.startsWith('-');if(neg)s=s.slice(1);
  const [i,f]=s.split('.');const grouped=i.replace(/\B(?=(\d{3})+(?!\d))/g,',');
  const num=grouped+(f?'.'+f:'');
  if(cur.type==='CUSTOM')return`${neg?'-':''}${cur.symbol} ${num}`;
  return`${neg?'-':''}${cur.symbol}${num}`;
}
function fmtQuota(q){
  q=Number(q)||0;const cur=currency(false);
  if(cur.type==='TOKENS')return q.toLocaleString('en-US');
  const qpu=meta().quota_per_unit>0?meta().quota_per_unit:500000;
  return fmtAmount(q/qpu*cur.rate,cur);
}
function fmtPrice(usd){const cur=currency(true);return fmtAmount((Number(usd)||0)*cur.rate,cur)}
function fmtQuotaShort(q){
  q=Number(q)||0;const cur=currency(false);
  if(cur.type==='TOKENS')return q.toLocaleString('en-US');
  const qpu=meta().quota_per_unit>0?meta().quota_per_unit:500000;
  const v=q/qpu*cur.rate,a=Math.abs(v);
  let s;
  if(a>=1000)s=(v/1000).toFixed(1).replace(/\.0$/,'')+'k';
  else s=v.toFixed(a>=1?2:4).replace(/\.?0+$/,'');
  return cur.type==='CUSTOM'?`${cur.symbol} ${s}`:`${cur.symbol}${s}`;
}
const fmtInt=n=>(Number(n)||0).toLocaleString('en-US');
function fmtRatio(r){r=Number(r);if(!Number.isFinite(r))return'';return Number.isInteger(r)?String(r):String(Number(r.toFixed(4)))}
function fmtKM(n){n=Number(n)||0;if(n<1000)return String(n);if(n<1e6)return(n/1000).toFixed(1)+'K';return(n/1e6).toFixed(2)+'M'}

// ---------- other ----------
function parseOther(it){if(it._o!==undefined)return it._o;let o=null;try{o=it.other?JSON.parse(it.other):null}catch{o=null}it._o=(o&&typeof o==='object')?o:null;return it._o}
const admin=o=>(o&&typeof o.admin_info==='object'&&o.admin_info)||null;
function cacheWrite(o){if(!o)return 0;const a=Number(o.cache_creation_tokens_5m)||0,b=Number(o.cache_creation_tokens_1h)||0;return(a>0||b>0)?a+b:(Number(o.cache_creation_tokens)||0)}
function effectiveRatio(o){if(!o)return null;if(o.user_group_ratio!=null&&Number(o.user_group_ratio)!==-1)return{kind:'专属倍率',v:Number(o.user_group_ratio)};if(o.group_ratio!=null)return{kind:'分组倍率',v:Number(o.group_ratio)};return null}

// 动态计费：识别标准 tier("名称", p * 单价 + c * 单价 + cr * … ) 形状，单价为每百万 token 美元。
function parseTiers(o){
  if(!o||!o.expr_b64)return null;
  let src='';try{src=decodeURIComponent(escape(atob(o.expr_b64)))}catch{try{src=atob(o.expr_b64)}catch{return null}}
  const tiers=[];const re=/tier\(\s*(["'])((?:\\.|(?!\1).)*)\1\s*,\s*([^()]*(?:\([^()]*\)[^()]*)*)\)/g;let m;
  while((m=re.exec(src))){
    const label=m[2],body=m[3],prices={};let ok=true;
    for(const term of body.split('+')){
      const t=term.trim();if(!t)continue;
      const mm=t.match(/^([a-z][a-z0-9]*)\s*\*\s*([0-9.]+(?:e[-+]?\d+)?)$/i)||t.match(/^([0-9.]+(?:e[-+]?\d+)?)\s*\*\s*([a-z][a-z0-9]*)$/i);
      if(!mm){ok=false;break}
      const [v,n]=/^[a-z]/i.test(mm[1])?[mm[1],mm[2]]:[mm[2],mm[1]];prices[v]=Number(n);
    }
    // 条件：本 tier( 之前、上一个 ':' 之后的 "条件 ?" 文本；没有则为兜底档
    const before=src.slice(0,m.index),colon=before.lastIndexOf(':'),seg=before.slice(colon+1).trim();
    const cond=seg.endsWith('?')?seg.slice(0,-1).trim():'';
    tiers.push({label,prices,ok,cond});
  }
  return tiers.length?tiers:null;
}
const norm=s=>String(s??'').trim().toLowerCase();
function matchedTier(o){const tiers=parseTiers(o);if(!tiers)return null;const want=norm(o.matched_tier);return tiers.find(t=>t.ok&&norm(t.label)===want)||null}
function anyCache(o){return(Number(o.cache_tokens)||0)>0||cacheWrite(o)>0||(Number(o.cache_creation_tokens_12h)||0)>0}

// 详情片段：[{text,tone}] tone: ''|muted|danger
function detailSegments(it){
  const o=parseOther(it),segs=[],a=admin(o);
  if(a&&a.quota_saturation)segs.push({text:'额度已钳制',tone:'danger'});
  const t=it.type;
  if(t===6){segs.push({text:'异步任务退款'});return segs}
  if(t===1||t===3||t===7){const txt=contentText(it);if(txt)segs.push({text:txt});return segs}
  if(t!==2)return segs;
  if(!o)return segs;
  if(o.violation_fee||o.violation_fee_code||o.violation_fee_marker){
    segs.push({text:'违规扣费',tone:'danger'});
    if(o.violation_fee_code)segs.push({text:String(o.violation_fee_code),tone:'muted'});
    segs.push({text:'扣费: '+fmtQuota(o.fee_quota??it.quota),tone:'muted'});
    return segs;
  }
  if(o.billing_mode==='tiered_expr'){
    const tier=matchedTier(o);
    if(!tier){segs.push({text:'动态计费 · '+(o.matched_tier||'特殊计费表达式'),tone:'muted'})}
    else{
      const p=tier.prices,lab=tier.label||'默认';
      if(p.p!=null||p.c!=null)segs.push({text:`${lab} · ${fmtPrice(p.p||0)} / ${fmtPrice(p.c||0)}/M`});
      else segs.push({text:`${lab}`});
      if(anyCache(o)){const parts=['cr','cc','cc1h','cc12h'].filter(k=>p[k]!=null).map(k=>fmtPrice(p[k]));if(parts.length)segs.push({text:'缓存 '+parts.join(' / '),tone:'muted'})}
      const others=Object.keys(p).filter(k=>!['p','c','cr','cc','cc1h','cc12h'].includes(k)).map(k=>`${k} ${fmtPrice(p[k])}/M`);
      if(others.length)segs.push({text:others.join(' · '),tone:'muted'});
    }
  }else if(Number(o.model_price)>0){segs.push({text:'每次调用 · '+fmtPrice(o.model_price)})}
  else if(o.model_ratio!=null){
    const inp=Number(o.model_ratio)*2,out=inp*(o.completion_ratio!=null?Number(o.completion_ratio):1);
    segs.push({text:`标准 · ${fmtPrice(inp)} / ${fmtPrice(out)}/M`});
    if(anyCache(o)){const parts=[];
      if(o.cache_ratio!=null&&Number(o.cache_ratio)!==1)parts.push(fmtPrice(inp*Number(o.cache_ratio)));
      if(o.cache_creation_ratio!=null&&Number(o.cache_creation_ratio)!==1)parts.push(fmtPrice(inp*Number(o.cache_creation_ratio)));
      if(Number(o.cache_creation_ratio_1h))parts.push(fmtPrice(inp*Number(o.cache_creation_ratio_1h)));
      if(Number(o.cache_creation_ratio_12h))parts.push(fmtPrice(inp*Number(o.cache_creation_ratio_12h)));
      if(parts.length)segs.push({text:'缓存 '+parts.join(' / '),tone:'muted'})}
  }else{const r=effectiveRatio(o);if(r)segs.push({text:`${r.kind} ${fmtRatio(r.v)}x`})}
  if(o.is_system_prompt_overwritten)segs.push({text:'系统提示覆盖',tone:'danger'});
  return segs;
}
function contentText(it){
  const o=parseOther(it);
  if(o&&o.op&&o.op.action&&(it.type===1||it.type===3||it.type===7))return String(o.op.action)+(o.op.params?' '+JSON.stringify(o.op.params):'');
  if(o&&Array.isArray(o.content_parts)&&o.content_parts.length)return o.content_parts.map(p=>p&&p.key?String(p.key)+(p.params?' '+JSON.stringify(p.params):''):'').filter(Boolean).join(', ')||it.content||'';
  return it.content||'';
}

// ---------- 颜色/徽章 ----------
function hashStr(s){let h=0;for(const ch of String(s))h=(h*31+ch.codePointAt(0))|0;return Math.abs(h)}
function avatarStyle(name){const h=hashStr(name);return`background:hsl(${h%360},${54+h%8}%,${52+h%8}%)`}
function chanStyle(id){const h=hashStr('ch'+id)%360;return`color:hsl(${h},70%,72%);border-color:hsl(${h},50%,40%);background:hsla(${h},60%,40%,.14)`}
const greenAmberRed=v=>v==='g'?'ul-g':v==='a'?'ul-a':'ul-r';
function frtClass(s){return s<5?'g':s<10?'a':'r'}
function totalClass(it){const t=it.use_time,c=it.completion_tokens;if(c<100||t<=0)return t<10?'g':t<30?'a':'r';const tps=c/t;return tps>=30?'g':tps>=15?'a':'r'}
const mask=v=>st.masked?'••••':esc(v);

// ---------- 渲染 ----------
function shell(){
  const p=$('tab-usage-logs');if(!p)return;
  p.innerHTML=`
  <div class="ul-card ul-filter-card">
    <div class="ul-sc" id="ulShortcuts" aria-label="常用用户"></div>
    <div class="ul-filters">
      <div class="ul-date" id="ulDateWrap"><button type="button" class="ul-input ul-date-btn" id="ulDateBtn">${svg(ICON.calendar,'ul-ico')}<span id="ulDateLabel"></span></button>
        <div class="ul-pop" id="ulDatePop" hidden>
          <label>起始时间<input type="datetime-local" step="1" id="ulDateStart"></label>
          <label>结束时间<input type="datetime-local" step="1" id="ulDateEnd"></label>
          <div class="ul-presets"><button type="button" data-preset="today">今天</button><button type="button" data-preset="7d">7 天</button><button type="button" data-preset="week">本周</button><button type="button" data-preset="30d">30 天</button><button type="button" data-preset="month">本月</button></div>
          <div class="ul-pop-actions"><button type="button" class="ul-btn primary" id="ulDateOk">确认</button></div>
        </div>
      </div>
      <input class="ul-input" id="ulModel" placeholder="模型名称" autocomplete="off">
      <input class="ul-input" id="ulGroup" placeholder="分组" list="ulGroupList" autocomplete="off"><datalist id="ulGroupList"></datalist>
      <select class="ul-input" id="ulType"><option value="0">所有类型</option><option value="1">充值</option><option value="2">消耗</option><option value="3" title="仅用于查询历史日志，新记录请前往审计日志查看。">管理（已弃用）</option><option value="4">系统</option><option value="5">错误</option><option value="6">退款</option><option value="7" title="仅用于查询历史日志，新记录请前往审计日志查看。">登录（已弃用）</option></select>
      <button type="button" class="ul-toggle" id="ulToggle"></button>
    </div>
    <div class="ul-filters ul-adv" id="ulAdv" hidden>
      <input class="ul-input" id="ulToken" placeholder="令牌名称" autocomplete="off">
      <input class="ul-input" id="ulUser" placeholder="用户名" autocomplete="off">
      <input class="ul-input" id="ulChannel" placeholder="渠道 ID" autocomplete="off">
      <input class="ul-input" id="ulReq" placeholder="请求 ID" autocomplete="off">
      <input class="ul-input" id="ulUpReq" placeholder="上游请求 ID" autocomplete="off">
    </div>
    <div class="ul-bar">
      <div class="ul-stats" id="ulStats"></div>
      <div class="ul-actions">
        <button type="button" class="ul-icon-btn" id="ulEye" title="隐藏" aria-label="隐藏"></button>
        <button type="button" class="ul-btn" id="ulReset">重置</button>
        <button type="button" class="ul-btn primary" id="ulSearch"><span class="ul-spin" aria-hidden="true"></span>${svg(ICON.search,'ul-search-ico')}<span>搜索</span></button>
        <button type="button" class="ul-btn" id="ulCancel" hidden>取消查询</button>
        <div class="ul-colmenu"><button type="button" class="ul-btn" id="ulCols">${svg(ICON.columns)}查看</button><div class="ul-pop ul-cols-pop" id="ulColsPop" hidden></div></div>
      </div>
    </div>
  </div>
  <div class="ul-card ul-table-card"><div class="ul-table-wrap" id="ulTableWrap" aria-live="polite"></div><div class="ul-pager" id="ulPager"></div></div>
  <div class="ul-modal" id="ulModal" hidden><div class="ul-dialog" role="dialog" aria-modal="true"><div class="ul-dialog-head"><b id="ulModalTitle"></b><button type="button" class="ul-x" id="ulModalClose" aria-label="关闭">×</button></div><div class="ul-dialog-body" id="ulModalBody"></div><div class="ul-dialog-foot" id="ulModalFoot"></div></div></div>`;
  bind();
}

function readDraftFromInputs(){
  const d=st.draft;
  d.model=$('ulModel').value.trim();d.group=$('ulGroup').value.trim();d.type=$('ulType').value;
  d.token=$('ulToken').value.trim();d.username=$('ulUser').value.trim();d.channel=$('ulChannel').value.trim();
  d.requestId=$('ulReq').value.trim();d.upstreamRequestId=$('ulUpReq').value.trim();
}
function writeInputs(){
  const d=st.draft;
  $('ulModel').value=d.model||'';$('ulGroup').value=d.group||'';$('ulType').value=d.type||'0';
  $('ulToken').value=d.token||'';$('ulUser').value=d.username||'';$('ulChannel').value=d.channel||'';
  $('ulReq').value=d.requestId||'';$('ulUpReq').value=d.upstreamRequestId||'';
  renderDateLabel();renderToggle();renderEye();
}
function advCount(d){return['token','username','channel','requestId','upstreamRequestId'].filter(k=>d[k]).length}
function anyFilter(d){return!!(d.model||d.group||(d.type&&d.type!=='0')||d.token||d.username||d.channel||d.requestId||d.upstreamRequestId)}
function renderToggle(){
  const n=advCount(st.draft),b=$('ulToggle');
  b.innerHTML=`<span>${st.expanded?'收起':'展开'}</span>${n?`<span class="ul-count">${n}</span>`:''}${svg(st.expanded?ICON.chevUp:ICON.chevDown,'ul-chev')}`;
  b.setAttribute('aria-expanded',String(st.expanded));
  b.classList.toggle('active',!st.expanded&&n>0);
  $('ulAdv').hidden=!st.expanded;
  $('ulReset').disabled=!anyFilter(st.draft);
  renderShortcuts();
}
function renderEye(){
  const b=$('ulEye');b.innerHTML=svg(st.masked?ICON.eyeOff:ICON.eye);b.classList.toggle('active',st.masked);b.title=st.masked?'显示':'隐藏';b.setAttribute('aria-label',b.title);
  for(const id of['ulToken','ulUser','ulGroup'])$(id).type=st.masked?'password':'text';
  renderShortcuts();
}
function rangeOf(d){const r=defaultRange();return{start:d.startTime||r.start,end:d.endTime||r.end}}
function renderDateLabel(){
  const d=st.draft;let label='时间范围';
  const r=(d.startTime||d.endTime)?{start:d.startTime,end:d.endTime}:defaultRange();
  if(r.start||r.end){
    label=`${r.start?fmtDT(r.start):'-'} ~ ${r.end?fmtDT(r.end):'-'}`;
    // 窄屏同一天时用紧凑格式：MM/DD HH:mm:ss–HH:mm:ss
    if(MOBILE()&&r.start&&r.end&&fmtDT(r.start).slice(0,10)===fmtDT(r.end).slice(0,10)){const a=fmtDT(r.start),b=fmtDT(r.end);label=`${a.slice(5,7)}/${a.slice(8,10)} ${a.slice(11)}–${b.slice(11)}`}
  }
  $('ulDateLabel').textContent=label;$('ulDateBtn').title=`${r.start?fmtDT(r.start):'-'} ~ ${r.end?fmtDT(r.end):'-'}`;
}
function presetRange(k){
  const now=new Date();let s,e;
  if(k==='today'){s=startOfDay(now);e=endOfDay(now)}
  else if(k==='7d'){s=startOfDay(now.getTime()-6*86400000);e=endOfDay(now)}
  else if(k==='30d'){s=startOfDay(now.getTime()-29*86400000);e=endOfDay(now)}
  else if(k==='week'){const dow=(now.getDay()+6)%7;s=startOfDay(now.getTime()-dow*86400000);e=endOfDay(s+6*86400000)}
  else{const a=new Date(now.getFullYear(),now.getMonth(),1);s=a.getTime();e=endOfDay(new Date(now.getFullYear(),now.getMonth()+1,0))}
  return{start:s,end:e};
}

function bind(){
  const enter=e=>{if(e.key==='Enter'){e.preventDefault();search()}};
  for(const id of['ulModel','ulGroup','ulToken','ulUser','ulChannel','ulReq','ulUpReq']){$(id).addEventListener('keydown',enter);$(id).addEventListener('input',()=>{readDraftFromInputs();renderToggle()})}
  $('ulType').addEventListener('change',()=>{readDraftFromInputs();renderToggle()});
  $('ulToggle').addEventListener('click',()=>{st.expanded=!st.expanded;renderToggle()});
  $('ulSearch').addEventListener('click',search);
  $('ulCancel').addEventListener('click',cancelQueries);
  $('ulStats').addEventListener('click',e=>{if(e.target.closest('[data-quota-calc]'))loadStat(true)});
  $('ulReset').addEventListener('click',reset);
  $('ulEye').addEventListener('click',()=>{st.masked=!st.masked;renderEye();if(st.stat)renderStats();renderTable()});
  $('ulCols').addEventListener('click',e=>{e.stopPropagation();const p=$('ulColsPop');p.hidden=!p.hidden;if(!p.hidden)renderColsPop()});
  $('ulDateBtn').addEventListener('click',e=>{e.stopPropagation();const p=$('ulDatePop');p.hidden=!p.hidden;if(!p.hidden){const r=rangeOf(st.draft);$('ulDateStart').value=toLocalInput(r.start);$('ulDateEnd').value=toLocalInput(r.end)}});
  $('ulDatePop').addEventListener('click',e=>{e.stopPropagation();const b=e.target.closest('[data-preset]');if(!b)return;const r=presetRange(b.dataset.preset);applyDraftRange(r.start,r.end)});
  $('ulDateOk').addEventListener('click',()=>applyDraftRange(fromLocalInput($('ulDateStart').value),fromLocalInput($('ulDateEnd').value)));
  document.addEventListener('click',e=>{if(!e.target.closest('#ulDateWrap'))$('ulDatePop')&&($('ulDatePop').hidden=true);if(!e.target.closest('.ul-colmenu'))$('ulColsPop')&&($('ulColsPop').hidden=true)});
  $('ulShortcuts').addEventListener('click',onShortcutBarClick);
  $('ulModal').addEventListener('click',onShortcutModalClick);
  $('ulModalClose').addEventListener('click',closeModal);
  $('ulModalBody').addEventListener('click',e=>{const b=e.target.closest('[data-copyval]');if(b)copy(b.dataset.copyval)});
  $('ulModal').addEventListener('click',e=>{if(e.target.id==='ulModal')closeModal()});
  document.addEventListener('keydown',e=>{if(e.key==='Escape'&&!$('ulModal').hidden)closeModal()});
  $('ulTableWrap').addEventListener('click',onTableClick);
  $('ulTableWrap').addEventListener('scroll',e=>e.currentTarget.classList.toggle('scrolled',e.currentTarget.scrollLeft>0),{passive:true});
  $('ulPager').addEventListener('click',onPagerClick);
  $('ulPager').addEventListener('change',e=>{if(e.target.id==='ulPageSize'){st.pageSize=Number(e.target.value)||100;try{localStorage.setItem(PAGE_SIZE_KEY,String(st.pageSize))}catch{}st.page=1;st.pageCursors.clear();st.totalKey=null;st.totalCached=null;writeHash();loadList()}});
}
function applyDraftRange(s,e){
  st.draft.startTime=s||0;st.draft.endTime=e||0;$('ulDatePop').hidden=true;renderDateLabel();
  if(MOBILE())search();
}
function renderColsPop(){
  $('ulColsPop').innerHTML='<div class="ul-pop-title">切换列</div>'+COLUMNS.map(([k,l])=>`<label class="ul-check"><input type="checkbox" data-col="${k}" ${colOn(k)?'checked':''}> ${l}</label>`).join('');
  $('ulColsPop').querySelectorAll('[data-col]').forEach(cb=>cb.addEventListener('change',()=>{st.cols[cb.dataset.col]=cb.checked;saveCols();renderTable()}));
}

// ---------- URL 状态（hash） ----------
const HASH_KEYS=['startTime','endTime','type','model','token','channel','group','username','requestId','upstreamRequestId','page','pageSize'];
function readHash(){
  const q=window.monitorHashParams?window.monitorHashParams():new URLSearchParams(location.hash.slice(1));
  const d={};for(const k of HASH_KEYS){const v=q.get(k);if(v!=null&&v!=='')d[k]=v}
  const out={startTime:Number(d.startTime)||0,endTime:Number(d.endTime)||0,type:/^[0-7]$/.test(d.type||'')?d.type:'0',model:d.model||'',token:d.token||'',channel:d.channel||'',group:d.group||'',username:d.username||'',requestId:d.requestId||'',upstreamRequestId:d.upstreamRequestId||''};
  st.page=Math.max(1,Number(d.page)||1);
  const ps=Number(d.pageSize);if([10,20,30,40,50,100].includes(ps))st.pageSize=ps;
  return out;
}
function writeHash(){
  const q=new URLSearchParams();q.set('tab','usage-logs');const a=st.applied;
  for(const k of HASH_KEYS.slice(0,10)){const v=a[k];if(v&&v!=='0')q.set(k,String(v))}
  if(st.page>1)q.set('page',String(st.page));
  if(st.pageSize)q.set('pageSize',String(st.pageSize));
  history.replaceState(null,'',location.pathname+location.search+'#'+q.toString());
}
function queryParams(withPage){
  const a=st.applied,r=rangeOf(a),q=new URLSearchParams();
  q.set('start_timestamp',String(Math.floor(r.start/1000)));q.set('end_timestamp',String(Math.floor(r.end/1000)));
  if(a.type&&a.type!=='0')q.set('type',a.type);
  if(a.model)q.set('model_name',a.model);if(a.token)q.set('token_name',a.token);if(a.group)q.set('group',a.group);
  if(a.username)q.set('username',a.username);if(a.channel)q.set('channel',a.channel);
  if(a.requestId)q.set('request_id',a.requestId);if(a.upstreamRequestId)q.set('upstream_request_id',a.upstreamRequestId);
  if(withPage){q.set('p',String(st.page));q.set('page_size',String(st.pageSize));const cursor=st.pageCursors.get(st.page);if(st.page>1&&cursor){q.set('before_created_at',String(cursor.created_at));q.set('before_id',String(cursor.id))}}
  return q;
}

function prepareSearch(){
  const q=queryParams(false);
  if(st.countTask&&st.countTask.key!==q.toString()){st.countTask.ctrl.abort();st.countTask=null}
  st.countError='';st.totalKnown=false;
  // A matching aggregation already running at search time (including a
  // clicked wide-range SUM) remains the refresh for this search, even if it
  // finishes while COUNT is loading.
  const same=k=>k===statQuery(false).toString()||k===statQuery(true).toString();
  st.statRequested=!(st.statLoading&&same(st.statKey));
  if(!same(st.statKey)){if(st.statAbort)st.statAbort.abort();++st.statSeq;st.statLoading=false}
  st.stat=null;renderStats(true);
}
async function search(){
  const seq=st.activationSeq;readDraftFromInputs();
  // Repeated Enter must not keep killing/restarting the same running SQL.
  // Changed filters still supersede it immediately; completed searches
  // always refresh normally, and the cancel control remains available.
  if(st.loading&&st.page===1&&st.applied&&HASH_KEYS.slice(0,10).every(k=>String(st.applied[k]??'')===String(st.draft[k]??'')))return;
  st.applied={...st.draft};st.page=1;st.pageCursors.clear();st.totalKey=null;st.totalCached=null;prepareSearch();writeHash();
  if(!st.meta&&!await loadMeta())return;if(seq!==st.activationSeq)return;loadList();
}
function reset(){
  const r=defaultRange();
  st.draft={startTime:r.start,endTime:r.end,type:'0',model:'',token:'',channel:'',group:'',username:'',requestId:'',upstreamRequestId:''};
  writeInputs();search();
}

async function getJSON(url,signal){
  const ctrl=new AbortController();let timedOut=false;
  const abort=()=>ctrl.abort();if(signal){signal.addEventListener('abort',abort,{once:true});if(signal.aborted)abort()}
  const timer=setTimeout(()=>{timedOut=true;ctrl.abort()},30000);
  try{
  const res=await fetch(url,{credentials:'same-origin',headers:{Accept:'application/json'},signal:ctrl.signal});
  if(res.status===401){location.href='/login';throw new Error('未登录')}
  const j=await res.json().catch(()=>null);
  if(!j)throw new Error('HTTP '+res.status);
  if(!j.success)throw new Error(j.message||'请求失败');
  return j.data;
  }catch(e){if(timedOut)throw new Error('查询等待超时，请缩小时间范围或增加筛选条件后重试');throw e}
  finally{clearTimeout(timer);if(signal)signal.removeEventListener('abort',abort)}
}
async function loadMeta(){
  try{st.meta=await getJSON('/usage-logs/meta');if(!st.meta)throw new Error('站点设置为空')}
  catch(e){st.meta=null;st.items=[];renderTable();$('ulTableWrap').innerHTML=`<div class="ul-empty"><b class="ul-err">暂时无法读取站点货币设置</b><div class="muted">${esc(e.message)}；请点击搜索重试。</div></div>`;$('ulStats').textContent='统计暂不可用';toast('读取站点设置失败：'+e.message);return false}
  const dl=$('ulGroupList');if(dl)dl.innerHTML=(meta().groups||[]).filter(g=>g!=='auto').map(g=>`<option value="${esc(g)}">`).join('');
  return true;
}
async function loadList(){
  if(st.listAbort)st.listAbort.abort();const ctrl=new AbortController();st.listAbort=ctrl;
  const seq=++st.seq;st.loading=true;$('ulSearch').disabled=true;$('ulSearch').classList.add('loading');
  st.totalKnown=false;st.countError='';updateQueryControls();
  // 重新查询时保留上一批行（变暗），首次加载才显示占位
  if(!st.items.length)$('ulTableWrap').innerHTML='<div class="ul-empty"><span class="ul-spin big"></span><div class="muted">加载中…</div></div>';
  else $('ulTableWrap').classList.add('refreshing');
  try{
    const q=queryParams(true);q.set('defer_total','1');
    const d=await getJSON('/usage-logs/list?'+q,ctrl.signal);
    if(seq!==st.seq)return;
    st.items=d.items||[];st.hasMore=!!d.has_more;st.pageSize=d.page_size||st.pageSize;
    const listKey=queryParams(false).toString();
    if(d.total_pending!==true){st.totalKnown=true;st.total=d.total||0;st.totalKey=listKey;st.totalCached=st.total}
    // Paging the same search reuses its exact total instead of re-running a
    // full COUNT per page; a new search, reset or page-size change refreshes it.
    else if(st.totalKey===listKey&&st.totalCached!=null){st.totalKnown=true;st.total=st.totalCached}
    else{st.totalKnown=false;st.total=0}
    // A successful response supersedes any stale anchors after this page.
    for(const key of st.pageCursors.keys())if(key>st.page)st.pageCursors.delete(key);
    if(d.next_cursor)st.pageCursors.set(st.page+1,d.next_cursor);
    const pages=Math.max(1,Math.ceil(st.total/st.pageSize));
    if(st.totalKnown&&st.page>pages&&st.total>0&&!st.items.length){st.page=pages;writeHash();return loadList()}
    renderTable();renderPager();
    if(!st.totalKnown)loadCount(seq);else runPendingStat(seq);
  }catch(e){if(seq!==st.seq)return;st.items=[];st.total=0;st.hasMore=false;st.countError='暂不可用';$('ulTableWrap').innerHTML=`<div class="ul-empty"><b class="ul-err">加载日志失败</b><div class="muted">${esc(e.message)}</div></div>`;renderPager();runPendingStat(seq)}
  finally{if(seq===st.seq){st.loading=false;$('ulSearch').disabled=false;$('ulSearch').classList.remove('loading');$('ulTableWrap').classList.remove('refreshing');updateQueryControls()}}
}
async function loadCount(seq){
  const q=queryParams(false),key=q.toString();let task=st.countTask;
  if(!task||task.key!==key){
    if(task)task.ctrl.abort();const ctrl=new AbortController();
    task={key,ctrl,promise:getJSON('/usage-logs/count?'+q,ctrl.signal)};st.countTask=task;
  }
  updateQueryControls();
  try{
    const d=await task.promise;if(seq!==st.seq||task.ctrl.signal.aborted)return;
    st.total=d.total;st.totalKnown=true;st.countError='';st.totalKey=key;st.totalCached=d.total;renderPager();
    const pages=Math.max(1,Math.ceil(st.total/st.pageSize));
    if(st.page>pages&&st.total>0&&!st.items.length){st.page=pages;writeHash();loadList()}
  }catch(e){if(seq!==st.seq||task.ctrl.signal.aborted)return;st.countError='暂不可用';renderPager();toast('日志总数查询失败：'+e.message)}
  finally{if(st.countTask===task)st.countTask=null;updateQueryControls();if(seq===st.seq)runPendingStat(seq)}
}
function runPendingStat(seq){if(seq===st.seq&&st.statRequested){st.statRequested=false;loadStat()}}
function updateQueryControls(){const b=$('ulCancel');if(b)b.hidden=!(st.loading||st.countTask||st.statLoading)}
function cancelQueries(){
  const listLoading=st.loading;++st.seq;++st.statSeq;st.statRequested=false;
  for(const k of['listAbort','statAbort'])if(st[k])st[k].abort();
  if(st.countTask)st.countTask.ctrl.abort();st.countTask=null;
  st.loading=false;st.statLoading=false;
  if(listLoading){st.items=[];st.totalKnown=false;st.hasMore=false;$('ulTableWrap').innerHTML='<div class="ul-empty">已取消查询，请重新搜索。</div>'}
  if(!st.totalKnown)st.countError='未完成';renderPager();
  if(!st.stat&&st.rates){st.stat=st.rates;renderStats()}
  else if(!st.stat)$('ulStats').innerHTML='<span class="muted">统计已取消</span>'+(st.applied&&statWide()?'<button type="button" class="ul-btn ul-quota-calc" data-quota-calc>计算用量</button>':'');
  $('ulSearch').disabled=false;$('ulSearch').classList.remove('loading');$('ulTableWrap').classList.remove('refreshing');updateQueryControls();
}
// Over a wide range the quota SUM reads every matching consume row, the main
// slow query of this page. Above this span it waits for an explicit click;
// the default range (today 00:00 to now+1h, at most 25h) still loads by itself.
const STAT_AUTO_SPAN_MS=26*3600*1000;
function statWide(){const r=rangeOf(st.applied||{});return r.end-r.start>STAT_AUTO_SPAN_MS}
function statQuery(force){
  // These three fields are ignored by new-api statistics. Changing only
  // them must not cancel and restart an identical expensive aggregation.
  const q=queryParams(false);for(const key of['type','request_id','upstream_request_id'])q.delete(key);
  if(!force&&statWide())q.set('skip_quota','1');
  return q;
}
async function loadStat(force){
  // Coalesce IN-FLIGHT requests only: a later explicit search still refreshes
  // completed amounts, rather than silently serving an old cached total.
  const q=statQuery(force===true);
  const key=q.toString();if(st.statLoading&&st.statKey===key)return;
  if(st.statAbort)st.statAbort.abort();const ctrl=new AbortController();st.statAbort=ctrl;
  if(force!==true)st.rates=null;
  const seq=++st.statSeq;st.statLoading=true;st.statKey=key;st.stat=null;renderStats(true);
  updateQueryControls();
  try{const d=await getJSON('/usage-logs/stat?'+q,ctrl.signal);if(seq!==st.statSeq)return;st.stat=d;if(d&&d.quota_skipped)st.rates=d;renderStats()}
  catch(e){
    if(seq!==st.statSeq)return;
    const err=`<span class="ul-stat-err" title="${esc(e.message)}">${force===true?'用量计算失败':'统计加载失败'}：${esc(e.message)}</span>`;
    // A failed clicked SUM keeps the rates already shown and stays retryable;
    // a failed wide-range rates request still offers the explicit SUM.
    if(force===true&&st.rates){st.stat=st.rates;renderStats();$('ulStats').innerHTML+=err;return}
    st.stat=null;$('ulStats').innerHTML=err+(statWide()?'<button type="button" class="ul-btn ul-quota-calc" data-quota-calc>计算用量</button>':'');
  }
  finally{if(seq===st.statSeq)st.statLoading=false;updateQueryControls()}
}
function renderStats(loading){
  const s=st.stat;
  if(loading||!s){$('ulStats').innerHTML='<span class="ul-pill sky skeleton">用量 <i class="ul-skel"></i></span><span class="ul-pill rose skeleton">RPM <i class="ul-skel"></i></span><span class="ul-pill slate skeleton">TPM <i class="ul-skel"></i></span>';return}
  const quotaPill=s.quota_skipped
    ?`<button type="button" class="ul-pill sky ul-quota-calc" data-quota-calc title="所选时间超过 1 天，用量汇总需要读取全部消耗日志，较慢，点击后再计算；超过约 3 天可能超时，长期用量请看【用户用量】。">用量 <b>点击计算</b></button>`
    :`<span class="ul-pill sky" title="与中转站统计口径一致：只汇总消耗日志，忽略类型、请求 ID 和上游请求 ID 筛选。">用量 <b>${st.masked?'••••':esc(fmtQuota(s.quota))}</b></span>`;
  $('ulStats').innerHTML=`${quotaPill}<span class="ul-pill rose" title="服务器最近 60 秒的消耗请求数，与所选日期范围无关。">RPM <b>${fmtInt(s.rpm)}</b></span><span class="ul-pill slate" title="服务器最近 60 秒的输入与输出 Token 总数，与所选日期范围无关。">TPM <b>${fmtInt(s.tpm)}</b></span>`;
}

function rowClass(it){const o=parseOther(it);if(admin(o)&&admin(o).quota_saturation)return'ul-row-amber';if(it.type===5)return'ul-row-rose';if(it.type===6)return'ul-row-blue';return''}
function typeBadge(t){return`<span class="ul-type ${TYPE_CLASS[t]||'grey'}">${TYPE_LABELS[t]||'未知'}</span>`}
function cellTime(it){const t=fmtDT(it.created_at*1000);return`<div class="ul-mono ul-ts" title="${esc(t)}"><span class="ul-ts-y">${esc(t.slice(0,5))}</span>${esc(t.slice(5))}</div>${typeBadge(it.type)}`}
function cellChannel(it,i){
  if(!DISPLAYABLE.has(it.type))return'';
  const o=parseOther(it),a=admin(o)||{},chain=Array.isArray(a.use_channel)?a.use_channel:[];
  const name=it.channel_name||'';
  const tip=[`${name&&!st.masked?name+' ':''}#${it.channel}`];
  if(chain.length>1)tip.push('链路: '+chain.join(' → '));
  if(a.is_multi_key===true&&typeof a.multi_key_index==='number')tip.push('密钥: '+a.multi_key_index);
  if(a.channel_affinity){const af=a.channel_affinity;tip.push('渠道亲和性',`规则: ${af.rule_name||'-'}`,`分组: ${st.masked?'••••':(af.using_group||af.selected_group||'-')}`)}
  let html=`<div class="ul-line" title="${esc(tip.join('\n'))}"><button type="button" class="ul-chan" data-copy="${it.channel}" style="${chanStyle(it.channel)}">#${it.channel}${a.channel_affinity?`<span class="ul-spark" title="渠道亲和性">${svg(ICON.spark)}</span>`:''}</button>`;
  if(a.is_multi_key===true&&typeof a.multi_key_index==='number')html+=`<span class="ul-mk" title="多密钥索引">${a.multi_key_index}</span>`;
  if(chain.length>1)html+=`<button type="button" class="ul-chain" data-chain="${i}" title="重试链路：${esc(chain.join(' → '))}" aria-label="重试链路">${svg(ICON.branch)}</button>`;
  html+='</div>';
  if(name)html+=`<div class="ul-sub">${mask(name)}</div>`;
  return html;
}
function cellUser(it){
  if(!it.username)return'';
  const n=it.username.trim(),ch=st.masked?'•':(Array.from(n)[0]||'?').toUpperCase();
  return`<button type="button" class="ul-user" data-user="${it.user_id}" ${n.length>12&&!st.masked?`title="${esc(n)}"`:''}><span class="ul-avatar" style="${st.masked?'background:#475569':avatarStyle(n)}">${esc(ch)}</span><span class="ul-uname">${mask(n)}</span></button>`;
}
function cellToken(it){
  if(!DISPLAYABLE.has(it.type)||!it.token_name)return'';
  const o=parseOther(it)||{},group=it.group||o.group||'';
  let ratio='';
  if(o.user_group_ratio!=null&&Number(o.user_group_ratio)!==-1)ratio=fmtRatio(o.user_group_ratio)+'x';
  else if(o.group_ratio!=null&&Number(o.group_ratio)!==1)ratio=fmtRatio(o.group_ratio)+'x';
  const gl=group==='auto'?'自动':(group||'用户分组');
  return`<div><button type="button" class="ul-pill-key" data-copy="${esc(it.token_name)}" ${it.token_name.length>16&&!st.masked?`title="${esc(it.token_name)}"`:''}>${svg(ICON.key)}<span class="ul-ellip">${mask(it.token_name)}</span></button></div>${(group||ratio)?`<div class="ul-sub"><span class="ul-group">${st.masked?'••••':esc(gl)}</span>${ratio?` <span class="muted">${esc(ratio)}</span>`:''}</div>`:''}`;
}
function responseModelInfo(it,o){
  if(!o)return null;
  const mapped=!!(o.is_model_mapped&&o.upstream_model_name);
  const rm=o.response_model&&typeof o.response_model==='object'?o.response_model:null;
  let mismatch=false,returned='';
  if(rm){returned=String(rm.returned_model||'');const r=returned.toLowerCase();if(r){const req=String(rm.requested_model||it.model_name||'').toLowerCase(),up=String(rm.upstream_model||o.upstream_model_name||'').toLowerCase();const ok=x=>x&&(r.startsWith(x)||r.endsWith(x));mismatch=!(ok(req)||ok(up))}}
  return{mapped,rm,mismatch,returned,upstream:o.upstream_model_name||(rm&&rm.upstream_model)||''};
}
function cellModel(it,i){
  if(!DISPLAYABLE.has(it.type))return'';
  const o=parseOther(it),info=responseModelInfo(it,o);
  // 有映射/响应模型差异时点徽章打开对照弹窗，否则点徽章复制模型名
  const pop=info&&(info.mapped||info.mismatch||(info.returned&&info.returned!==it.model_name)||(info.upstream&&info.upstream!==it.model_name));
  let html=`<div class="ul-line"><button type="button" class="ul-model" ${pop?`data-model="${i}"`:`data-copy="${esc(it.model_name)}"`} title="${esc(it.model_name)}"><span class="ul-dot" style="background:hsl(${hashStr(it.model_name)%360},65%,60%)"></span><span class="ul-ellip">${esc(it.model_name)}</span></button>`;
  if(info&&info.mapped)html+=`<button type="button" class="ul-route" data-model="${i}" title="请求模型：${esc(it.model_name)}\n实际模型：${esc(o.upstream_model_name)}" aria-label="模型映射">${svg(ICON.route)}</button>`;
  html+='</div>';
  if(info&&info.mismatch)html+=`<div><button type="button" class="ul-warn" data-model="${i}" title="响应模型与请求模型不一致">${svg(ICON.alert)}<span class="ul-ellip">响应模型：${esc(info.returned)}</span></button></div>`;
  return html;
}
function cellStream(it){
  if(!TIMING.has(it.type))return'';
  const o=parseOther(it)||{};let l1;
  if(o.is_task)l1=`<span class="muted">${o.task_sync?'同步':'异步'}</span>`;
  else l1=it.is_stream?'<span class="ul-info">流</span>':'<span class="muted">非流</span>';
  const ss=o.stream_status;
  if(it.is_stream&&ss&&ss.status&&ss.status!=='ok'){const tip=['流状态: 错误',ss.end_reason||'unknown'];if(Number(ss.error_count)>0)tip.push('软错误: '+ss.error_count);l1+=` <span class="ul-alert" title="${esc(tip.join('\n'))}">${svg(ICON.alert)}</span>`}
  const tps=(it.use_time>0&&it.completion_tokens>0)?`${Math.round(it.completion_tokens/it.use_time)} t/s`:'—';
  return`<div>${l1}</div><div class="ul-sub">${tps}</div>`;
}
function cellTokens(it){
  if(!DISPLAYABLE.has(it.type))return'';
  if(!it.prompt_tokens&&!it.completion_tokens)return'-';
  const o=parseOther(it)||{},cr=Number(o.cache_tokens)||0,cw=cacheWrite(o);
  let l2='';if(cr>0)l2+=`缓存↓ ${fmtInt(cr)}`;if(cw>0)l2+=`${l2?' ':''}${cr>0?'':'缓存'}↑ ${fmtInt(cw)}`;
  return`<div class="ul-mono">${fmtInt(it.prompt_tokens)} / ${fmtInt(it.completion_tokens)}</div>${l2?`<div class="ul-sub">${l2}</div>`:''}`;
}
function hasSurcharge(o){
  if(!o)return false;
  if(Array.isArray(o.tool_surcharges)&&o.tool_surcharges.some(s=>s&&s.name&&Number(s.count)>0&&Number(s.price)>0))return true;
  if(o.web_search&&Number(o.web_search_call_count)>0&&Number(o.web_search_price)>0)return true;
  if(o.file_search&&Number(o.file_search_call_count)>0&&Number(o.file_search_price)>0)return true;
  return!!(o.image_generation_call&&Number(o.image_generation_call_price)>0);
}
function cellCost(it){
  if(!DISPLAYABLE.has(it.type))return'';
  const o=parseOther(it)||{};let icon='',amount=it.quota;
  if(o.billing_source==='subscription'){amount=o.subscription_consumed??it.quota;icon=`<span class="ul-cost-ico sub" title="订阅">${svg(ICON.crown)}</span>`}
  else if(o.billing_source==='wallet'&&meta().has_enabled_subscription_plans)icon=`<span class="ul-cost-ico" title="钱包">${svg(ICON.wallet)}</span>`;
  return`<div class="ul-line"><span class="ul-cost">${icon}${esc(fmtQuota(amount))}</span>${hasSurcharge(o)?`<span class="ul-surcharge" title="包含工具调用附加费">${svg(ICON.wrench)}+</span>`:''}</div>`;
}
function cellTime2(it){
  if(!TIMING.has(it.type))return'';
  const o=parseOther(it)||{},tc=totalClass(it);let rows='',bar='';
  if(it.is_stream){const f=Number(o.frt)>0?Number(o.frt)/1000:null;const fc=f==null?'n':frtClass(f);rows+=`<div><span class="ul-tl">首字</span><span class="ul-mono ${f==null?'muted':greenAmberRed(fc)}">${f==null?'不适用':fmtSeconds(f)}</span></div>`;bar=`<span class="ul-bar2"><i class="${f==null?'ul-n':greenAmberRed(fc)}"></i><i class="${greenAmberRed(tc)}"></i></span>`}
  else bar=`<span class="ul-bar1 ${greenAmberRed(tc)}"></span>`;
  rows+=`<div><span class="ul-tl">耗时</span><span class="ul-mono ${greenAmberRed(tc)}">${fmtSeconds(it.use_time)}</span></div>`;
  return`<div class="ul-timing">${bar}<div class="ul-trows">${rows}</div></div>`;
}
function cellDetail(it,i){
  const segs=detailSegments(it);
  let inner;
  if(segs.length){const s=segs[0];inner=`<span class="ul-ellip ${s.tone==='danger'?'ul-danger':s.tone==='muted'?'muted':''}">${esc(s.text)}</span>${segs.length>1?`<span class="ul-more">+${segs.length-1}</span>`:''}`}
  else{const c=contentText(it);inner=c?`<span class="ul-ellip ${it.type===5?'ul-danger':'muted'}">${esc(c)}</span>`:'<span class="muted">—</span>'}
  return`<button type="button" class="ul-detail" data-detail="${i}" title="点击查看详细信息">${inner}</button>`;
}
function renderTable(){
  const wrap=$('ulTableWrap');if(!wrap)return;
  if(!st.items.length){wrap.innerHTML='<div class="ul-empty"><b>未找到日志</b><div class="muted">暂无使用日志。发起 API 调用后日志将显示在此处。</div></div>';return}
  const cols=[['time','时间',cellTime]].concat([['channel','渠道',cellChannel],['user','用户',cellUser],['token','令牌',cellToken],['model','模型',cellModel],['stream','流',cellStream],['tokens','Token',cellTokens],['cost','费用',cellCost],['time2','耗时',cellTime2],['detail','详情',cellDetail]].filter(([k])=>colOn(k==='time2'?'time':k)));
  wrap.innerHTML=`<table class="ul-table"><thead><tr>${cols.map(c=>`<th class="ul-c-${c[0]}">${c[1]}</th>`).join('')}</tr></thead><tbody>${st.items.map((it,i)=>`<tr class="${rowClass(it)}">${cols.map(c=>`<td class="ul-c-${c[0]}">${c[2](it,i)}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
}
function pageStrip(cur,last){
  if(last<=4)return Array.from({length:last},(_,i)=>i+1);
  if(cur<=2)return[1,2,'…',last];
  if(cur>=last-1)return[1,'…',last-1,last];
  return[1,'…',cur,'…',last];
}
function renderPager(){
  if(!st.totalKnown){
    const btn=(p,label,disabled,active=false)=>`<button type="button" class="ul-pg${active?' active':''}" data-page="${p}" ${disabled?'disabled':''}>${label}</button>`;
    $('ulPager').innerHTML=`<span class="muted">总计：${esc(st.countError||'计算中…')}</span><span class="muted ul-ps-label">每页行数</span><select id="ulPageSize">${[10,20,30,40,50,100].map(n=>`<option value="${n}" ${n===st.pageSize?'selected':''}>${n}</option>`).join('')}</select><span class="ul-pg-btns">${btn(1,'«',st.page<=1)}${btn(st.page-1,'‹',st.page<=1)}${btn(st.page,String(st.page),false,true)}${btn(st.page+1,'›',!st.hasMore)}</span>`;
    return;
  }
  // The reused total is as of this search; rows that arrived since can make
  // a further page exist, which has_more from the list still reveals.
  const counted=Math.max(1,Math.ceil(st.total/st.pageSize)),beyond=st.hasMore&&st.page>=counted;
  const last=beyond?st.page+1:Math.max(counted,st.items.length?st.page:1),cur=Math.min(st.page,last);
  const btn=(p,label,dis,act)=>`<button type="button" class="ul-pg${act?' active':''}" data-page="${p}" ${dis?'disabled':''}>${label}</button>`;
  $('ulPager').innerHTML=`<span class="muted">总计：</span><b class="ul-mono" title="按本次搜索时计算；翻页期间新增的日志不计入，点搜索刷新">${fmtInt(st.total)}</b>
    <span class="muted ul-ps-label">每页行数</span><select id="ulPageSize">${[10,20,30,40,50,100].map(n=>`<option value="${n}" ${n===st.pageSize?'selected':''}>${n}</option>`).join('')}</select>
    <span class="ul-pg-btns">${btn(1,'«',cur<=1)}${btn(cur-1,'‹',cur<=1)}${pageStrip(cur,last).map(p=>p==='…'?'<span class="muted">…</span>':btn(p,p,false,p===cur)).join('')}${btn(cur+1,'›',cur>=last)}${btn(last,'»',cur>=last)}</span>`;
}
function onPagerClick(e){const b=e.target.closest('[data-page]');if(!b||b.disabled)return;const p=Number(b.dataset.page);if(!p||p===st.page)return;st.page=p;writeHash();loadList();$('ulTableWrap').scrollTop=0}

function onTableClick(e){
  const t=e.target.closest('button,[data-copy]');if(!t)return;
  if(t.dataset.detail!=null)return openDetail(st.items[Number(t.dataset.detail)]);
  if(t.dataset.user!=null)return openUser(Number(t.dataset.user));
  if(t.dataset.chain!=null){const it=st.items[Number(t.dataset.chain)];const a=admin(parseOther(it))||{};return openModal('重试链路',`<div class="ul-chainlist">${(a.use_channel||[]).map((c,k,arr)=>`<span class="ul-chan" style="${chanStyle(c)}">#${esc(c)}</span>${k<arr.length-1?'<span class="muted">→</span>':''}`).join('')}</div><div class="ul-sec-sub">按尝试顺序排列，最后一个为最终使用的渠道。</div>`)}
  if(t.dataset.model!=null){const it=st.items[Number(t.dataset.model)],o=parseOther(it),info=responseModelInfo(it,o);return openModal(info&&info.rm?'响应模型':'模型映射',modelRows(it,o)||kvCopy('请求模型',it.model_name))}
  if(t.dataset.copy!=null)copy(t.dataset.copy);
}
function copy(text){if(!text)return;(navigator.clipboard?navigator.clipboard.writeText(text):Promise.reject()).then(()=>toast('已复制')).catch(()=>toast('复制失败'))}
function toast(msg){let el=$('ulToast');if(!el){el=document.createElement('div');el.id='ulToast';el.className='ul-toast';document.body.appendChild(el)}el.textContent=msg;el.classList.add('show');clearTimeout(el._t);el._t=setTimeout(()=>el.classList.remove('show'),2200)}

// ---------- 弹窗 ----------
function openModal(title,body,foot,wide){st.modalSeq=(st.modalSeq||0)+1;if(st.userAbort){st.userAbort.abort();st.userAbort=null}$('ulModalTitle').innerHTML=title;$('ulModalBody').innerHTML=body;$('ulModalFoot').innerHTML=foot||'';$('ulModal').hidden=false;$('ulModal').querySelector('.ul-dialog').classList.toggle('wide',!!wide)}
function closeModal(){st.modalSeq=(st.modalSeq||0)+1;$('ulModal').hidden=true;if(st.userAbort){st.userAbort.abort();st.userAbort=null}}

// ---------- 常用用户快捷标签（每个管理员各自一份，存监控本地库） ----------
async function postJSON(url,body){
  const res=await fetch(url,{method:'POST',credentials:'same-origin',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify(body||{})});
  if(res.status===401){location.href='/login';throw new Error('未登录')}
  const j=await res.json().catch(()=>null);
  if(!j)throw new Error('HTTP '+res.status);
  if(!j.success)throw new Error(j.message||'请求失败');
  return j.data;
}
function scApply(d){if(!d)return;st.shortcuts=Array.isArray(d.items)?d.items:[];st.scMax=Number(d.max)||20;st.scError='';renderShortcuts()}
async function loadShortcuts(){try{scApply(await getJSON('/usage-logs/shortcuts'))}catch(e){st.scError=e.message||'加载失败';renderShortcuts()}}
const scText=s=>st.masked&&s.label===s.username?'••••':s.label;
function renderShortcuts(){
  const el=$('ulShortcuts');if(!el||!st.draft)return;
  const items=st.shortcuts||[],cur=st.draft.username||'',max=st.scMax||20;
  let h='<span class="ul-sc-title">常用用户</span>';
  if(st.scError)h+=`<span class="ul-sc-hint">常用用户加载失败</span><button type="button" class="ul-sc-link" data-sc-retry>重试</button>`;
  else{
    if(!items.length)h+='<span class="ul-sc-hint">把常看的用户加成标签，点一下就能查他的日志</span>';
    h+=items.map(s=>{const on=s.username===cur;return`<button type="button" class="ul-sc-chip${on?' active':''}" data-sc="${s.id}" aria-pressed="${on}" title="${esc(st.masked?(on?'再次点击取消筛选':'查看该用户的日志'):'用户名：'+s.username+(on?'（再次点击取消筛选）':''))}">${esc(scText(s))}</button>`}).join('');
    if(items.length<max)h+='<button type="button" class="ul-sc-add" data-sc-add title="添加常用用户">+ 添加</button>';
    if(items.length)h+='<button type="button" class="ul-sc-link" data-sc-manage>管理</button>';
  }
  el.innerHTML=h;
}
function onShortcutBarClick(e){
  const c=e.target.closest('[data-sc]');
  if(c){
    const s=(st.shortcuts||[]).find(x=>String(x.id)===c.dataset.sc);if(!s)return;
    $('ulUser').value=st.draft.username===s.username?'':s.username;
    readDraftFromInputs();renderToggle();search();return;
  }
  if(e.target.closest('[data-sc-add]'))return openShortcutForm(null,st.draft.username&&!st.draft.username.includes('%')?st.draft.username:'',false);
  if(e.target.closest('[data-sc-manage]'))return openShortcutManage();
  if(e.target.closest('[data-sc-retry]'))loadShortcuts();
}
function openShortcutForm(s,prefill,back){
  const label=s&&s.label!==s.username?s.label:'';
  const body=`<div class="ul-sc-form">
    <label><span>用户名</span><input class="ul-input" id="ulScUser" maxlength="64" autocomplete="off" spellcheck="false" placeholder="完整的 new-api 用户名" value="${esc(s?s.username:prefill||'')}"></label>
    <label><span>标签名称</span><input class="ul-input" id="ulScLabel" maxlength="16" autocomplete="off" placeholder="留空则显示用户名，最多 16 个字" value="${esc(label)}"></label>
    <div class="muted ul-sc-note">常用用户只有你自己能看到，最多 ${st.scMax||20} 个。</div>
    <div class="ul-err ul-sc-err" id="ulScErr" hidden></div></div>`;
  const foot=`<button type="button" class="ul-btn" ${back?'data-sc-back>返回':'data-sc-cancel>取消'}</button><button type="button" class="ul-btn primary" data-sc-save="${s?s.id:0}" data-sc-from="${back?'manage':''}">保存</button>`;
  openModal(s?'编辑常用用户':'添加常用用户',body,foot);
  const u=$('ulScUser');(u.value?$('ulScLabel'):u).focus();
  for(const id of['ulScUser','ulScLabel'])$(id).addEventListener('keydown',ev=>{if(ev.key==='Enter'){ev.preventDefault();saveShortcut(s?s.id:0,back)}});
}
async function saveShortcut(id,back){
  const btn=$('ulModalFoot').querySelector('[data-sc-save]'),err=$('ulScErr');if(!err||(btn&&btn.disabled))return;
  const body={username:$('ulScUser').value.trim(),label:$('ulScLabel').value.trim()};
  const fail=m=>{err.textContent=m;err.hidden=false;if(btn)btn.disabled=false};
  if(!body.username)return fail('请填写用户名');
  if(body.username.includes('%'))return fail('请填写完整用户名，快捷标签不支持 % 通配符');
  if(btn)btn.disabled=true;
  const seq=st.modalSeq;
  try{
    const d=await postJSON(id?'/usage-logs/shortcuts/'+id:'/usage-logs/shortcuts',body);scApply(d);
    if(seq!==st.modalSeq)return;
    toast(id?'已保存':'已添加');if(back)openShortcutManage();else closeModal();
  }catch(e){if(seq===st.modalSeq)fail(e.message||'保存失败')}
}
function openShortcutManage(){
  const items=st.shortcuts||[],max=st.scMax||20;
  const rows=items.map((s,i)=>`<div class="ul-sc-row" data-id="${s.id}">
    <div class="ul-sc-row-main"><b>${esc(scText(s))}</b><span class="muted ul-mono">${st.masked?'••••':esc(s.username)}</span></div>
    <div class="ul-sc-row-act"><button type="button" class="ul-icon-btn" data-sc-move="-1" ${i===0?'disabled':''} title="上移" aria-label="上移">${svg(ICON.chevUp)}</button><button type="button" class="ul-icon-btn" data-sc-move="1" ${i===items.length-1?'disabled':''} title="下移" aria-label="下移">${svg(ICON.chevDown)}</button><button type="button" class="ul-btn" data-sc-edit>编辑</button><button type="button" class="ul-btn ul-sc-del" data-sc-del>删除</button></div></div>`).join('');
  openModal(`管理常用用户 <span class="muted ul-sc-cnt">${items.length}/${max}</span>`,rows?`<div class="ul-sc-list">${rows}</div>`:'<div class="ul-empty muted">还没有常用用户</div>',`${items.length<max?'<button type="button" class="ul-btn" data-sc-new>添加</button>':''}<button type="button" class="ul-btn primary" data-sc-cancel>完成</button>`);
}
async function moveShortcut(id,dir){
  const ids=(st.shortcuts||[]).map(s=>s.id),i=ids.indexOf(id),j=i+dir;if(i<0||j<0||j>=ids.length||st.scBusy)return;
  [ids[i],ids[j]]=[ids[j],ids[i]];st.scBusy=true;
  try{scApply(await postJSON('/usage-logs/shortcuts/order',{ids}));if(!$('ulModal').hidden)openShortcutManage()}catch(e){toast(e.message||'保存顺序失败')}finally{st.scBusy=false}
}
async function deleteShortcut(id){
  if(st.scBusy)return;st.scBusy=true;
  try{scApply(await postJSON('/usage-logs/shortcuts/'+id+'/delete'));if(!$('ulModal').hidden)openShortcutManage();toast('已删除')}catch(e){toast(e.message||'删除失败')}finally{st.scBusy=false}
}
function onShortcutModalClick(e){
  const t=e.target.closest('[data-sc-save],[data-sc-cancel],[data-sc-back],[data-sc-new],[data-sc-edit],[data-sc-del],[data-sc-move],[data-sc-pin]');if(!t||t.disabled)return;
  if(t.hasAttribute('data-sc-cancel'))return closeModal();
  if(t.hasAttribute('data-sc-back'))return openShortcutManage();
  if(t.hasAttribute('data-sc-new'))return openShortcutForm(null,'',true);
  if(t.hasAttribute('data-sc-save'))return saveShortcut(Number(t.dataset.scSave)||0,t.dataset.scFrom==='manage');
  if(t.hasAttribute('data-sc-pin'))return openShortcutForm(null,t.dataset.scPin,false);
  const row=t.closest('.ul-sc-row');if(!row)return;
  const id=Number(row.dataset.id),s=(st.shortcuts||[]).find(x=>x.id===id);if(!s)return;
  if(t.hasAttribute('data-sc-edit'))return openShortcutForm(s,'',true);
  if(t.hasAttribute('data-sc-move'))return moveShortcut(id,Number(t.dataset.scMove));
  if(t.hasAttribute('data-sc-del')){
    if(t.dataset.armed!=='1'){t.dataset.armed='1';t.textContent='确认删除';t.classList.add('armed');clearTimeout(t._t);t._t=setTimeout(()=>{t.dataset.armed='';t.textContent='删除';t.classList.remove('armed')},3000);return}
    return deleteShortcut(id);
  }
}
const kv=(k,v,cls)=>v==null||v===''?'':`<div class="ul-kv"><span>${esc(k)}</span><b class="${cls||''}">${v}</b></div>`;
const kvCopy=(k,raw)=>raw?`<div class="ul-kv"><span>${esc(k)}</span><b class="ul-mono">${esc(raw)}<button type="button" class="ul-copy" data-copyval="${esc(raw)}" title="复制" aria-label="复制">${svg(ICON.copy)}</button></b></div>`:'';
const section=(title,body,danger)=>body?`<div class="ul-sec${danger?' danger':''}">${title?`<div class="ul-sec-title">${esc(title)}</div>`:''}${body}</div>`:'';
function modelRows(it,o){
  const info=responseModelInfo(it,o);if(!info)return'';
  if(info.rm&&(info.mismatch||info.returned)){
    return(info.mismatch?'<div class="ul-note warn">响应模型与请求模型不一致。别名或带日期的模型版本也可能导致此提示。</div>':'')+kvCopy('请求模型',info.rm.requested_model||it.model_name)+kvCopy('上游请求模型',info.upstream||info.rm.requested_model||it.model_name)+kvCopy('响应模型',info.returned);
  }
  if(info.mapped)return kvCopy('请求模型',it.model_name)+kvCopy('实际模型',o.upstream_model_name);
  return'';
}
function billingPath(a){
  if(!a)return'';const p=a.usage_billing_path;
  if(p==='local')return'本地计费';
  if(p==='upstream'||p==='')return'上游返回';
  if(p)return`上游返回 (${p})`;
  if(a.local_count_tokens!=null)return a.local_count_tokens?'本地计费':'上游返回';
  return'';
}
const TIER_VAR={p:'输入',c:'输出',cr:'缓存读取',cc:'缓存创建',cc1h:'缓存创建 (1h)',cc12h:'缓存创建 (12h)'};
const CACHE_VARS=new Set(['cr','cc','cc1h','cc12h']);
function tierTable(o){
  const tiers=parseTiers(o);let h='';
  if(o.image_count)h+=kv('计费图片数量',esc(o.image_count));
  if(!tiers||!tiers.some(t=>t.ok))return h+`<div class="muted">无法解析计费表达式，命中：${esc(o.matched_tier||'特殊计费表达式')}</div>`;
  const showCache=anyCache(o),want=norm(o.matched_tier);
  const vars=[];for(const t of tiers)for(const k of Object.keys(t.prices))if(!vars.includes(k)&&(showCache||!CACHE_VARS.has(k)))vars.push(k);
  h+=`<div class="ul-tier-wrap"><table class="ul-tier"><thead><tr><th>阶梯</th><th>条件</th>${vars.map(k=>`<th class="num">${esc(TIER_VAR[k]||k)}</th>`).join('')}</tr></thead><tbody>${tiers.map(t=>{const hit=t.ok&&norm(t.label)===want;return`<tr class="${hit?'hit':''}"><td>${esc(t.label||'默认')}${hit?' <span class="ul-badge green">命中</span>':''}</td><td class="ul-mono muted">${esc(t.cond||'其他')}</td>${vars.map(k=>`<td class="num ul-mono">${t.prices[k]!=null?esc(fmtPrice(t.prices[k])):'—'}</td>`).join('')}</tr>`}).join('')}</tbody></table></div><div class="ul-sec-sub">单价为每百万 Token；分组倍率另计。</div>`;
  return h;
}
function openDetail(it){
  const o=parseOther(it)||{},a=admin(o),t=it.type;let html='';
  // 1 概览
  let ov=kv('日志 ID',esc(it.id),'ul-mono')+kv('时间',esc(fmtDT(it.created_at*1000)))+kv('用户',esc(it.username))+kv('请求 ID',esc(it.request_id),'ul-mono')+kv('上游请求 ID',esc(it.upstream_request_id),'ul-mono');
  if(it.channel>0)ov+=kv('渠道',esc(`${it.channel}${it.channel_name?` (${it.channel_name})`:''}`));
  if(a&&Array.isArray(a.use_channel)&&a.use_channel.length)ov+=kv('重试链路',esc(a.use_channel.join(' → ')),'ul-mono');
  if(it.token_name)ov+=kv('令牌',esc(it.token_name));
  const grp=it.group||o.group;if(grp)ov+=kv('分组',esc(grp));
  if(it.ip&&(t===2||t===5||t===1))ov+=kv('IP 地址',esc(it.ip),'ul-mono');
  if(TIMING.has(t)&&it.use_time>0){const f=Number(o.frt)>0?Number(o.frt)/1000:0;ov+=kv('响应时间',`<span class="${greenAmberRed(totalClass(it))}">${fmtSeconds(it.use_time)}</span>${it.is_stream&&f>0?` (FRT: <span class="${greenAmberRed(frtClass(f))}">${fmtSeconds(f)}</span>)`:''}`)}
  html+=section('',ov);
  if(t!==6&&(o.request_path||o.request_conversion))html+=section('请求转换',kv('路径',esc(o.request_path||''),'ul-mono')+kv('转换',esc(Array.isArray(o.request_conversion)&&o.request_conversion.length>1?o.request_conversion.join(' -> '):'原生格式')));
  if(a&&Array.isArray(a.request_policy)&&a.request_policy.length)html+=section('请求策略决策',`<pre class="ul-pre">${esc(JSON.stringify(a.request_policy,null,2))}</pre>`);
  if(a&&a.quota_saturation){const q=a.quota_saturation;html+=section('额度已钳制','<div>额度饱和保护已触发</div>'+kv('类型',esc({overflow:'溢出',underflow:'下溢',nan:'无效 (NaN)'}[q.kind]||q.kind||''))+kv('原始值',esc(q.original))+kv('钳制为',esc(q.clamped))+kv('操作',esc(q.op)),true)}
  if(a&&a.reject_reason)html+=section('拒绝原因',`<div>${esc(a.reject_reason)}</div>`,true);
  if(o.violation_fee||o.violation_fee_code||o.violation_fee_marker)html+=section('违规扣费',kv('违规代码',esc(o.violation_fee_code))+kv('违规标记',esc(o.violation_fee_marker))+kv('扣费金额',esc(fmtQuota(o.fee_quota??it.quota))),true);
  if(t===6&&(o.task_id||o.reason))html+=section('退款详情',kv('任务 ID',esc(o.task_id),'ul-mono')+kv('原因',esc(o.reason)));
  if(a&&a.task_plugin){const p=a.task_plugin;html+=section('任务插件',kv('插件',esc(p.key))+kv('名称',esc(p.name))+kv('版本',esc(p.version))+kv('作者',esc(p.author)))}
  if(o.root_info){const r=o.root_info;html+=section('Root 诊断',kv('API 版本',esc(r.task_plugin&&r.task_plugin.api_version))+kv('插件代次',esc(r.task_plugin&&r.task_plugin.generation))+kv('上游任务 ID',esc(r.upstream_task_id),'ul-mono')+kv('节点名称',esc(r.node_name)))}
  if(t===1){html+=section('充值审计信息',a?(kv('订单支付方式',esc(a.payment_method))+kv('回调支付方式',esc(a.callback_payment_method))+kv('回调调用者 IP',esc(a.caller_ip))+kv('服务器 IP',esc(a.server_ip))+kv('节点名称',esc(a.node_name))+kv('系统版本',esc(a.version))):'<div class="muted">历史记录早于审计追踪，无审计信息。</div>')}
  const opp=(o.op&&typeof o.op.params==='object'&&o.op.params)||{};
  if(t===1&&o.op&&o.op.action){html+=section('额度调整详情',kv('操作',esc(o.op.action))+Object.entries(opp).map(([k,v])=>kv(k,esc(typeof v==='object'?JSON.stringify(v):v))).join(''))}
  if(t===3&&a&&(a.admin_username||a.admin_id))html+=section('操作管理员',`<div>${a.admin_username&&a.admin_id?`${esc(a.admin_username)} (ID: ${esc(a.admin_id)})`:a.admin_username?esc(a.admin_username):`ID: ${esc(a.admin_id)}`}</div>`);
  const au=(o.audit_info&&typeof o.audit_info==='object'&&o.audit_info)||null;
  if(t===3&&a&&(contentText(it)||(au&&au.route))){
    const am={access_token:'访问令牌',token:'访问令牌',session:'会话'};
    const FL={status:'状态',models:'模型',model:'模型',group:'分组',groups:'分组',type:'类型',base_url:'Base URL',key:'密钥'};
    const fields=Array.isArray(opp.fields)?opp.fields.map(f=>FL[f]||f).join('、'):'';
    let x=kv('操作',esc(contentText(it)))+kv('认证方式',esc(a.auth_method?(am[a.auth_method]||a.auth_method):''))+kv('变更字段',esc(fields));
    if(au){if(au.route)x+=kv('请求',esc(`${au.method||''} ${au.route}`.trim()),'ul-mono');if(au.status!=null||au.success!=null)x+=kv('结果',`<span class="${au.success===false?'ul-r':'ul-g'}">${au.success===false?'失败':'成功'}${au.status!=null?` (${esc(au.status)})`:''}</span>`)}
    html+=section('操作审计信息',x);
  }
  if(t===7){html+=section('登录信息',kv('操作',esc(contentText(it)))+kv('登录方式',esc(opp.method||opp.login_method||(a&&a.auth_method)||''))+kv('IP 地址',esc(it.ip),'ul-mono')+kv('用户代理',esc(opp.user_agent||o.user_agent||'')))}
  if(o.ws||o.audio)html+=section('语音 Token',kv('音频输入',Number(o.audio_input)>0?fmtKM(o.audio_input):'')+kv('音频输出',Number(o.audio_output)>0?fmtKM(o.audio_output):'')+kv('文本输入',Number(o.text_input)>0?fmtKM(o.text_input):'')+kv('文本输出',Number(o.text_output)>0?fmtKM(o.text_output):''));
  if(o.reasoning_effort)html+=section('推理强度',`<span class="ul-badge ${({max:'orange',xhigh:'orange',high:'orange',medium:'yellow',low:'green',minimal:'green'})[o.reasoning_effort]||'grey'}">${esc(o.reasoning_effort)}</span>`);
  if(o.is_system_prompt_overwritten)html+=section('',`<span>系统提示词：</span><span class="ul-badge orange">已覆盖</span>`);
  const mr=modelRows(it,o);if(mr)html+=section(responseModelInfo(it,o).rm?'响应模型':'模型映射',mr);
  if(DISPLAYABLE.has(t)&&(it.prompt_tokens>0||it.completion_tokens>0)&&it.other){
    let tk=kv('输入 Token',fmtInt(it.prompt_tokens))+kv('输出 Token',fmtInt(it.completion_tokens));
    if(Number(o.cache_tokens)>0)tk+=kv('缓存读取',fmtInt(o.cache_tokens));
    if(o.image_cache_tokens!=null)tk+=kv('图片缓存',fmtInt(o.image_cache_tokens));
    const c5=Number(o.cache_creation_tokens_5m)||0,c1=Number(o.cache_creation_tokens_1h)||0,c12=Number(o.cache_creation_tokens_12h)||0;
    if(Number(o.cache_creation_tokens)>0&&!c5&&!c1&&!c12)tk+=kv('缓存写入',fmtInt(o.cache_creation_tokens));
    if(c5>0)tk+=kv('缓存写入 (5m)',fmtInt(c5));if(c1>0)tk+=kv('缓存写入 (1h)',fmtInt(c1));
    if(c12>0){tk+=kv('缓存写入 (30m)',fmtInt(Math.max((Number(o.cache_creation_tokens)||0)-c12,0)))+kv('缓存写入 (12h)',fmtInt(c12))}
    if(o.image)tk+=kv('图像 Token',fmtInt(o.image_output));
    if(o.billing_tokens&&typeof o.billing_tokens==='object')tk+='<div class="ul-sec-sub">计价 Token 明细</div>'+Object.entries(o.billing_tokens).map(([k,v])=>kv(k,fmtInt(v))).join('');
    html+=section('Token 明细',tk);
  }
  if(t===2&&!(o.violation_fee||o.violation_fee_code||o.violation_fee_marker)&&it.other){
    let b='';
    if(o.billing_mode==='tiered_expr'){
      const tier=matchedTier(o);b+=kv('计费模式','动态计费')+kv('命中阶梯',esc(tier?tier.label||'默认':(o.matched_tier||'无匹配结果')));
      if(tier)for(const [k,v] of Object.entries(tier.prices))b+=kv(({p:'输入',c:'输出',cr:'缓存读取',cc:'缓存创建',cc1h:'缓存创建 (1h)',cc12h:'缓存创建 (12h)'})[k]||k,esc(fmtPrice(v)+'/M'));
    }else if(Number(o.model_price)>0){b+=kv('计费模式','每次调用')+kv('模型价格',esc(fmtPrice(o.model_price)))}
    else if(o.model_ratio!=null){
      const inp=Number(o.model_ratio)*2;b+=kv('计费模式','按 Token')+kv('输入',esc(fmtPrice(inp)+'/M'))+kv('输出',esc(fmtPrice(inp*(o.completion_ratio!=null?Number(o.completion_ratio):1))+'/M'));
      const c12=Number(o.cache_creation_tokens_12h)||0;
      if((o.claude||c12>0)&&anyCache(o)){
        if(o.cache_ratio!=null&&Number(o.cache_ratio)!==1)b+=kv('缓存读取',esc(fmtPrice(inp*o.cache_ratio)+'/M'));
        if(o.cache_creation_ratio!=null&&Number(o.cache_creation_ratio)!==1)b+=kv(c12>0?'缓存创建 (30m)':'缓存创建',esc(fmtPrice(inp*o.cache_creation_ratio)+'/M'));
        for(const [k,l] of[['cache_creation_ratio_5m','缓存创建 (5m)'],['cache_creation_ratio_1h','缓存创建 (1h)'],['cache_creation_ratio_12h','缓存创建 (12h)']])if(Number(o[k]))b+=kv(l,esc(fmtPrice(inp*o[k])+'/M'));
      }
      if(o.audio_ratio!=null&&Number(o.audio_ratio)!==1)b+=kv('音频输入',esc(fmtPrice(inp*o.audio_ratio)+'/M'));
      if(o.audio_completion_ratio!=null&&Number(o.audio_completion_ratio)!==1)b+=kv('音频输出',esc(fmtPrice(inp*o.audio_completion_ratio)+'/M'));
      if(o.image_ratio!=null&&Number(o.image_ratio)!==1)b+=kv('图片输入',esc(fmtPrice(inp*o.image_ratio)+'/M'));
    }
    const r=effectiveRatio(o);if(r&&Number.isFinite(r.v))b+=kv(r.kind,esc(r.v.toFixed(4)+'x'));
    if(o.web_search)b+=kv('网页搜索',esc(`${o.web_search_call_count||0}x (${fmtPrice(o.web_search_price||0)})`));
    if(o.file_search)b+=kv('文件搜索',esc(`${o.file_search_call_count||0}x (${fmtPrice(o.file_search_price||0)})`));
    if(o.image_generation_call)b+=kv('图片生成',esc(fmtPrice(o.image_generation_call_price||0)));
    if(o.audio_input_seperate_price)b+=kv('音频输入价格',esc(fmtPrice(o.audio_input_price||0)));
    if(Array.isArray(o.tool_surcharges))for(const s of o.tool_surcharges)if(s&&s.name)b+=kv(s.name,esc(`${s.count||0}x (${fmtPrice(s.price||0)})`));
    const bp=billingPath(a);if(bp)b+=kv('计费路径',esc(bp));
    if(o.usage_facts&&typeof o.usage_facts==='object')b+=Object.entries(o.usage_facts).map(([k,v])=>kv(k,esc(typeof v==='object'?JSON.stringify(v):v))).join('');
    b+=kv('总费用',`<span class="ul-total">${esc(fmtQuota(it.quota))}</span>`);
    html+=section('计费详情',b);
  }
  if(t===2&&o.billing_mode==='tiered_expr'&&o.expr_b64)html+=section('动态计费',tierTable(o));
  if(t!==2&&t!==6&&a){const bp=billingPath(a);if(bp)html+=section('',kv('计费路径',esc(bp)))}
  const ss=o.stream_status;
  if(ss&&ss.status&&ss.status!=='ok')html+=section('流状态',kv('状态',`<span class="ul-badge red">${esc(ss.status)}</span>`)+kv('结束原因',esc(ss.end_reason))+kv('软错误',esc(ss.error_count))+kv('结束错误',esc(ss.end_error))+(Array.isArray(ss.errors)&&ss.errors.length?`<pre class="ul-pre">${esc(ss.errors.join('\n'))}</pre>`:''),true);
  if(o.billing_source==='subscription'){let s=kv('套餐',esc(`#${o.subscription_plan_id??''} ${o.subscription_plan_title||''}`))+kv('实例',esc(`#${o.subscription_id??''}`))+kv('预扣费',esc(fmtQuota(o.subscription_pre_consumed||0)));if(Number(o.subscription_post_delta))s+=kv('结算差额',esc(fmtQuota(o.subscription_post_delta)));s+=kv('最终消耗',esc(fmtQuota(o.subscription_consumed??it.quota)))+kv('剩余',esc(`${fmtQuota(o.subscription_remain||0)} / ${fmtQuota(o.subscription_total||0)}`));html+=section('订阅计费',s)}
  if(Array.isArray(o.po)&&o.po.length){const AL={set:'设置',delete:'删除',copy:'复制',move:'移动',append:'追加',prepend:'前置',replace:'替换'};html+=section(`参数覆盖 (${o.po.length})`,o.po.map(line=>{const s=String(line),i=s.indexOf(' ');const act=i>0?s.slice(0,i):s,rest=i>0?s.slice(i+1):'';return`<div class="ul-po"><span class="ul-badge grey">${esc(AL[act]||act)}</span> <span class="ul-mono">${esc(rest)}</span></div>`}).join(''))}
  const ct=contentText(it);
  if(ct)html+=section('内容',`<div class="ul-content">${esc(ct)}</div><div class="ul-copyrow"><button type="button" class="ul-btn sm" data-copytext="1">${svg(ICON.copy)}复制</button></div>`);
  // Keep unrecognized historical/new fields inspectable, without interpreting
  // them as a price formula. Preserve other verbatim (including large integers).
  const raw={...it};delete raw._o;delete raw.other;
  html+=`<details class="ul-raw"><summary>原始日志数据</summary><pre class="ul-pre">${esc(JSON.stringify(raw,null,2))}</pre>${it.other?`<div class="ul-sec-sub">other（原始记录）</div><pre class="ul-pre">${esc(it.other)}</pre>`:''}</details>`;
  openModal(`日志详情 <span class="ul-type ${TYPE_CLASS[t]||'grey'}">${TYPE_LABELS[t]||'未知'}</span>`,html||'<div class="muted">无详情</div>','',o.billing_mode==='tiered_expr');
  const cb=$('ulModalBody').querySelector('[data-copytext]');if(cb)cb.addEventListener('click',()=>copy(ct));
}
async function openUser(id){
  openModal('用户信息','<div class="ul-empty"><span class="ul-spin big"></span><div class="muted">加载中…</div></div>');
  const seq=st.modalSeq,ctrl=new AbortController();st.userAbort=ctrl;
  try{
    const u=await getJSON('/usage-logs/user/'+encodeURIComponent(id),ctrl.signal);
    if(seq!==st.modalSeq)return;
    if(!u){$('ulModalBody').innerHTML='<div class="ul-empty">暂无用户信息</div>';return}
    const compact=n=>{n=Number(n)||0;return n>=10000?(n/10000).toFixed(1).replace(/\.0$/,'')+'万':fmtInt(n)};
    const item=(k,v,cls)=>v==null||v===''?'':`<div class="ul-uitem"><span>${esc(k)}</span><b class="${cls||''}">${esc(v)}</b></div>`;
    const nm=String(u.username||'').trim();
    let h=`<div class="ul-uhead"><span class="ul-avatar lg" style="${avatarStyle(nm)}">${esc((Array.from(nm)[0]||'?').toUpperCase())}</span><div><b>${esc(nm||'-')}</b>${u.display_name?`<div class="muted">${esc(u.display_name)}</div>`:''}</div><span class="muted ul-mono">ID ${esc(u.id)}</span></div>`;
    h+=`<div class="ul-ugrid">${item('用户名',u.username)}${u.display_name?item('显示名称',u.display_name):''}${item('余额',fmtQuotaShort(u.quota),'ul-mono')}${item('消耗额度',fmtQuotaShort(u.used_quota),'ul-mono')}${item('请求计数',compact(u.request_count),'ul-mono')}${u.group?item('用户分组',u.group):''}</div>`;
    if(u.aff_code||u.aff_count||u.aff_quota>0)h+=section('邀请信息',`<div class="ul-ugrid">${u.aff_code?item('邀请码',u.aff_code,'ul-mono'):''}${item('受邀用户',compact(u.aff_count),'ul-mono')}${u.aff_quota>0?item('邀请额度',fmtQuotaShort(u.aff_quota),'ul-mono'):''}</div>`);
    if(u.remark)h+=section('备注',`<div class="ul-content">${esc(u.remark)}</div>`);
    $('ulModalBody').innerHTML=h;
    if(nm)$('ulModalFoot').innerHTML=(st.shortcuts||[]).some(s=>s.username===nm)?'<button type="button" class="ul-btn" disabled>已在常用用户中</button>':`<button type="button" class="ul-btn" data-sc-pin="${esc(nm)}">加为常用用户</button>`;
  }catch(e){if(seq!==st.modalSeq||e.name==='AbortError')return;$('ulModalBody').innerHTML=`<div class="ul-empty"><b class="ul-err">获取用户信息失败</b><div class="muted">${esc(e.message||'')}</div></div>`;toast(e.message||'获取用户信息失败')}
}

// ---------- 入口 ----------
async function activate(){
  const seq=(st.activationSeq||0)+1;st.activationSeq=seq;
  if(!st.inited){
    st.inited=true;shell();loadShortcuts();
    if(!st.pageSize){let ps=0;try{ps=Number(localStorage.getItem(PAGE_SIZE_KEY))}catch{}st.pageSize=[10,20,30,40,50,100].includes(ps)?ps:(MOBILE()?20:100)}
  }
  const fromHash=readHash();
  // Pin the default range to the displayed selection. Otherwise each page
  // change silently advances its upper bound and changes the query scope.
  const range=rangeOf(fromHash);fromHash.startTime=range.start;fromHash.endTime=range.end;
  st.pageCursors.clear();st.totalKey=null;st.totalCached=null;
  st.draft={...fromHash};st.applied={...fromHash};
  writeInputs();
  if(!st.meta&&!await loadMeta())return;
  if(seq!==st.activationSeq)return;
  prepareSearch();loadList();
}
function deactivate(){if(!st.inited)return;st.activationSeq=(st.activationSeq||0)+1;++st.seq;++st.statSeq;for(const k of['listAbort','statAbort'])if(st[k]){st[k].abort();st[k]=null}if(st.countTask)st.countTask.ctrl.abort();st.countTask=null;st.statRequested=false;st.loading=false;st.statLoading=false;closeModal()}
// 由 page.html 的 switchTab 调用（侧栏点击与带 hash 打开都会经过它），不另挂 monitor:navigate，避免重复查询。
window.usageLogsActivate=activate;
window.usageLogsDeactivate=deactivate;
})();
