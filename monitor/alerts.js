(function(){
'use strict';

// 问题预警：统计未到达任何渠道的请求（令牌配错、请求不存在的模型等）。
// 这类请求不进渠道稳定率，故单独成页；只读 /alerts/rejections。

let loaded=false;
// date 与客户排障同口径：单日、CST。空=今天。
let date="";
let inited=false;
let fReason="";
let fUser="";
let lastRows=[];
let rowTotal=0,hasMore=false,nextCursor="",loading=false,generation=0,abort=null,lastError="";
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?"":s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const nfmt=n=>(+n||0).toLocaleString("zh-CN");
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

window.alertsActivate=function(){
  if(!inited)init();
  if(!loaded)load();
};

function init(){
  inited=true;
  date=date||cstToday();
  $("alPrev")?.addEventListener("click",()=>go(shiftDate(date,-1)));
  $("alNext")?.addEventListener("click",()=>{
    if(date>=cstToday())return;
    go(shiftDate(date,1));
  });
  $("alToday")?.addEventListener("click",()=>go(cstToday()));
  $("alDate")?.addEventListener("change",()=>{
    const v=$("alDate").value;
    if(v&&v<=cstToday())go(v);
    else sync();
  });
  // 原因在服务端精确筛选；客户 ID 只在按钮/回车提交，避免每个输入字符都查一次。
  $("alReason")?.addEventListener("change",()=>{fReason=$("alReason").value;load();});
  const applyUser=()=>{
    const raw=$("alUser")?.value.trim()||"";
    if(raw&&!/^\d+$/.test(raw)){setStatus("客户 ID 必须是非负整数");return;}
    fUser=raw;load();
  };
  $("alApply")?.addEventListener("click",applyUser);
  $("alUser")?.addEventListener("keydown",e=>{if(e.key==="Enter")applyUser();});
  $("alReset")?.addEventListener("click",()=>{
    fReason="";fUser="";
    if($("alReason"))$("alReason").value="";
    if($("alUser"))$("alUser").value="";
    load();
  });
  $("alMore")?.addEventListener("click",()=>load(true));
  sync();
}
// sync 同步日期状态到控件。
function sync(){
  const i=$("alDate");
if(i)i.value=date;
const n=$("alNext");
if(n)n.disabled=(date>=cstToday());
const l=$("alDateLabel");
if(l)l.textContent=date+" (CST)";
}
function go(d){
date=d;
sync();
load();
}
// setStatus/setNotes 与客户排障一致：状态短句放筛选栏，覆盖说明放独立条。
function setStatus(t){const el=$("alStatus");if(el)el.textContent=t||"";}
function setNotes(t){
  const el=$("alNotes");
  if(!el)return;
  if(!t){el.hidden=true;el.textContent="";return;}
  el.hidden=false;
  el.textContent=t;
}
function setBody(html){
  const b=$("alTableBody");
  if(b)b.innerHTML=html;
}

let lastMeta={enabled:true,total:0,truncated:false,note:"",unauth_count:0,
  unknown_customer_count:0,unknown_user_quota_count:0,unknown_pre_consume_count:0,
  unknown_token_quota_count:0,unknown_customer_other_count:0};

async function load(more=false){
  if(more&&(!hasMore||!nextCursor||loading))return;
  const gen=++generation;
  loading=true;
  lastError="";
  abort?.abort();
  const ac=new AbortController();abort=ac;
  if(!more){lastRows=[];rowTotal=0;hasMore=false;nextCursor="";}
  setStatus(more?"加载更多…":"正在读取…");
  if(!more)setBody('<tr><td colspan="6" class="lc-empty">加载中…</td></tr>');
  syncMore();
  try{
    const q=new URLSearchParams({from:date,to:date,limit:"100"});
    if(fReason)q.set("reason",fReason);
    if(fUser!=="")q.set("user_id",fUser);
    if(more)q.set("cursor",nextCursor);
    const r=await fetch("/alerts/rejections?"+q.toString(),{
      credentials:"same-origin",cache:"no-store",signal:ac.signal,headers:{"Accept":"application/json"}
    });
    const text=await r.text();
    if(gen!==generation)return;
    let d={};
    try{d=JSON.parse(text);}catch(e){throw new Error("响应不是 JSON（HTTP "+r.status+")："+text.slice(0,160));}
    if(!r.ok)throw new Error(d.error||("HTTP "+r.status));
    loaded=true;
    lastError="";
    lastRows=more?lastRows.concat(d.rows||[]):(d.rows||[]);
    rowTotal=+d.row_total||0;
    hasMore=!!d.has_more;
    nextCursor=d.next_cursor||"";
    lastMeta={enabled:d.enabled!==false,total:d.total||0,
      truncated:!!d.has_more,note:d.coverage_note||"",
      unauth_count:d.unauth_count||0,
      unknown_customer_count:d.unknown_customer_count||0,
      unknown_user_quota_count:d.unknown_user_quota_count||0,
      unknown_pre_consume_count:d.unknown_pre_consume_count||0,
      unknown_token_quota_count:d.unknown_token_quota_count||0,
      unknown_customer_other_count:d.unknown_customer_other_count||0};
    fillReasonOptions(d.reason_options||[]);
    loading=false;
    setStatus("");
    paint();
  }catch(e){
    if(e.name==="AbortError"||gen!==generation)return;
    loading=false;
    lastError=e.message||String(e);
    setStatus("读取失败："+lastError);
    if(!more)lastRows=[];
    paint();
  }finally{
    if(gen===generation){loading=false;syncMore();}
  }
}

function syncMore(){
  const b=$("alMore");if(!b)return;
  b.hidden=!hasMore;
  b.disabled=loading;
  b.textContent=loading?"加载中…":"加载更多";
}

// fillReasonOptions 用服务端返回的同日/同客户原因全集填下拉，不受当前页限制。
function fillReasonOptions(reasons){
  const sel=$("alReason");
  if(!sel)return;
  const seen=[...new Set((reasons||[]).filter(Boolean))];
  const keep=fReason;
  if(keep&&!seen.includes(keep))seen.push(keep);
  // 未知码在下拉里要带上原始值：两个不同的新码都显示「待分类」就选不动了。
  sel.innerHTML='<option value="">全部错误原因</option>'+
    seen.map(r=>{
      const t=isUnclassified(r)?rlabel(r)+"："+r:rlabel(r);
      return '<option value="'+esc(r)+'">'+esc(t)+"</option>";
    }).join("");
  if(keep&&seen.includes(keep))sel.value=keep;
  else sel.value="";
}

// REASON 把采集器的 reason 代码翻成中文。
// 六种来自真实日志实测；同一种错误在 new-api 里有中英文两种措辞，
// 采集器已映射到同一代码，这里只管显示。
const REASON={
  no_available_channel:"无可用渠道",
  invalid_token:"无效令牌",
  token_model_forbidden:"令牌无权访问该模型",
  user_quota_insufficient:"用户额度不足",
  token_quota_insufficient:"令牌额度不足",
  pre_consume_failed:"预扣费失败"
};

// ★ 新错误类型必须显式标成「待分类」，不能把原始码当成正常标签混在中文里 ★
//
// 后端对 reason 没有白名单（server.go 只 clip 到 64 字符），采集器认出什么就存什么。
// 所以这里会遇到两种未知，它们要修的地方不同，必须分开显示：
//
//   other      = 采集器读到了日志行但正则没匹配上 → 要改采集器的正则
//   其它未知码 = 采集器认出来了但这里没有中文标签 → 要改本文件的 REASON
//
// 两者都不能装作已知：规范要求新出现的错误码默认进入「未知/待分类」。
const COLLECTOR_UNMATCHED="other";
const isUnclassified=r=>r===COLLECTOR_UNMATCHED||!REASON[r];
const rlabel=r=>{
  if(REASON[r])return REASON[r];
  if(r===COLLECTOR_UNMATCHED)return "采集器未能识别";
  return "待分类";
};
// rcell 渲染原因单元格：invalid_token 结合 user_id 说明是否已识别客户；
// 未知码把原始值显示在副行，否则光看「待分类」没法知道该给哪个码加映射。
// 底层 reason 和筛选值不变，只有显示文案细分。
const rcell=x=>{
  const r=x.reason;
  if(r==="invalid_token"){
    return "<td>"+(+x.user_id?"无效令牌 · 可定位客户":"无效令牌 · 未识别用户")+"</td>";
  }
  const label=esc(rlabel(r));
  if(!isUnclassified(r))return "<td>"+label+"</td>";
  return '<td class="lc-cust"><div style="color:var(--yellow)">'+label+
    '</div><div class="lc-sub">'+esc(r||"(空)")+"</div></td>";
};

// fmtTs 把分钟桶起点显示成 CST 的「MM-DD HH:mm」。
// 只到分钟：采集侧就是按分钟聚合的，显示秒会是假精度。
const fmtTs=ts=>{
  if(!ts)return "—";
  const p=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',
    hour:'2-digit',minute:'2-digit',hour12:false}).formatToParts(new Date(ts*1000));
  const v=t=>p.find(x=>x.type===t)?.value||'';
  return `${v('month')}-${v('day')} ${v('hour')}:${v('minute')}`;
};

// cust 客户单元格。与客户排障 logchain.js 完全同形：
// 上行用户名（无名字退回 #ID），下行「ID nnn」。
// 缓存没命中时绝不显示空白，否则看不出是「没这个客户」还是「没查到名字」。
const cust=x=>{
  const uid=+x.user_id;
  if(!uid){
    // 只有 invalid_token 能确定是未鉴权：真实日志明确写 user 0。
    // 额度类等错误原文没有 user 段，不能把「日志没提供」说成「鉴权失败」。
    if(x.reason==="invalid_token"){
      return '<td class="lc-cust"><div>未鉴权</div><div class="lc-sub">无效令牌 · 无用户身份</div></td>';
    }
    // 定位不到客户的三种已知额度原因仍要直接区分；错误原因列虽也会显示，
    // 但客户列副行解释「为什么这里没有客户」，避免被理解成用户名同步失败。
    const why={
      user_quota_insufficient:"用户额度不足",
      pre_consume_failed:"预扣费失败",
      token_quota_insufficient:"令牌额度不足"
    }[x.reason]||"其他错误";
    return '<td class="lc-cust"><div>客户未知</div><div class="lc-sub">'+why+' · 日志无用户 ID</div></td>';
  }
  const name=x.username?esc(x.username):("#"+uid);
  return '<td class="lc-cust"><div>'+name+'</div><div class="lc-sub">ID '+uid+"</div></td>";
};

// paint 只画一张明细表。
//
// 这一页故意不做「按错误类型」的汇总表：那是稳定性报表的活，
// 两处各算一遍口径一旦漂移就会互相打脸。这里只回答
// 「哪个客户、什么时候、请求什么、为什么被拒」。
function paint(){
  if(lastError){
    setNotes(lastRows.length?"加载更多失败，已保留此前加载的记录。":"读取失败，请检查连接后重试。");
    if(!lastRows.length)setBody('<tr><td colspan="6" class="lc-empty">读取失败：'+esc(lastError)+'</td></tr>');
    setCounter(lastRows.length,rowTotal);
    syncMore();
    return;
  }
  if(!lastMeta.enabled){
    setNotes("稳定性采集未开启，本页无数据。");
    setBody('<tr><td colspan="6" class="lc-empty">未开启</td></tr>');
    return;
  }
  // ★ 0 条必须说成「未采集」，不能让人以为没有问题 ★
  if(!lastRows.length){
    const filtered=!!(fReason||fUser!=="");
    setNotes(filtered?"当前筛选下无记录。":(lastMeta.note||"当日无记录。"));
    setBody('<tr><td colspan="6" class="lc-empty">'+(filtered?"当前筛选下无记录":"当日无记录")+'</td></tr>');
    setCounter(0,0);
    return;
  }

  const rows=lastRows;

  let note="这些请求从未到达任何渠道，不计入渠道稳定率；但客户当时确实用不了。";
  if(hasMore)note+=" 当前筛选共有 "+nfmt(rowTotal)+" 条聚合记录，可继续加载。";
  // ★ 身份缺失分两档 ★
  // invalid_token 的真实日志明确写 user 0，属于未鉴权；其他 user_id=0 是原文没有
  // user 段，只能称「客户未知」。混在一起会把采集字段缺失误判成鉴权失败。
  if(lastMeta.unauth_count>0){
    note+=" 其中 "+nfmt(lastMeta.unauth_count)+" 次为无效令牌导致的未鉴权请求，无法定位客户。";
  }
  if(lastMeta.unknown_customer_count>0){
    const parts=[];
    if(lastMeta.unknown_user_quota_count)parts.push("用户额度不足 "+nfmt(lastMeta.unknown_user_quota_count)+" 次");
    if(lastMeta.unknown_pre_consume_count)parts.push("预扣费失败 "+nfmt(lastMeta.unknown_pre_consume_count)+" 次");
    if(lastMeta.unknown_token_quota_count)parts.push("令牌额度不足 "+nfmt(lastMeta.unknown_token_quota_count)+" 次");
    if(lastMeta.unknown_customer_other_count)parts.push("其他 "+nfmt(lastMeta.unknown_customer_other_count)+" 次");
    note+=" 另有 "+nfmt(lastMeta.unknown_customer_count)+" 次日志未提供用户 ID";
    if(parts.length)note+="（"+parts.join("、")+"）";
    note+="，客户暂时未知。";
  }
  // ★ 出现未分类原因必须主动说出来 ★
  // 不说的话，页面上只是多了几行「待分类」，没人会注意到该去补规则；
  // 两种未知要修的地方不同，所以分开报。
  const unmatched=new Set(), unlabeled=new Set();
  for(const x of lastRows){
    if(x.reason===COLLECTOR_UNMATCHED)unmatched.add(x.reason);
    else if(!REASON[x.reason])unlabeled.add(x.reason);
  }
  if(unmatched.size)note+=" 有 "+COLLECTOR_UNMATCHED+" 档记录：采集器读到了日志但没匹配上，需补采集器的正则。";
  if(unlabeled.size)note+=" 出现 "+unlabeled.size+" 种未见过的错误码（"+
    [...unlabeled].slice(0,3).join("、")+(unlabeled.size>3?" 等":"")+"），本页尚无中文标签，需补映射。";
  setNotes(note);

  const html=rows.map(x=>{
    // 分组/模型：额度类错误的日志本来就不带这两项，显示「—」而不是猜。
    const grp=x.grp?esc(x.grp):'<span class="lc-sub">—</span>';
    const model=(x.model&&x.model!=="unknown")?esc(x.model):'<span class="lc-sub">—</span>';
    return "<tr><td>"+fmtTs(x.ts)+"</td>"+
      cust(x)+
      "<td>"+grp+"</td>"+
      "<td>"+model+"</td>"+
      rcell(x)+
      '<td class="right">'+nfmt(x.count)+"</td></tr>";
  }).join("");
  setBody(html);
  setCounter(rows.length,rowTotal);
  syncMore();
}

// setCounter 的总数与所有身份分项都来自服务端当前筛选口径。
function setCounter(shown,all){
  const el=$("alCounter");
  if(!el)return;
  if(!all&&!shown){el.textContent="";return;}
  el.innerHTML="已加载 <b>"+nfmt(shown)+"</b> / 共 <b>"+nfmt(all)+
    "</b> 条聚合记录 · 当前筛选共 <b>"+nfmt(lastMeta.total)+"</b> 次被拒";
}

})();
