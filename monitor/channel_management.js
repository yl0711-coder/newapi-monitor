(function(){
'use strict';

const cm={
  inited:false,loaded:false,days:7,custom:null,preset:'',report:null,abort:null,sort:'cost',
  filters:{search:'',domain:'',vendor:'',group:'',status:''},
  expandedDomains:new Set(),expandedVendors:new Set(),expandedChannels:new Set(),
  financeDomain:null,financeGroups:[]
};
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const zero=()=>({requests:0,tokens:0,cost_usd:0});
const add=(a,b)=>{a.requests+=(+b?.requests||0);a.tokens+=(+b?.tokens||0);a.cost_usd+=(+b?.cost_usd||0);return a};
const nfmt=n=>(+n||0).toLocaleString('zh-CN');
const compact=n=>{n=+n||0;const a=Math.abs(n);for(const [d,u] of [[1e12,'T'],[1e9,'B'],[1e6,'M'],[1e3,'k']])if(a>=d)return (n/d>=100?(n/d).toFixed(0):(n/d).toFixed(1)).replace(/\.0$/,'')+u;return nfmt(n)};
const usd=n=>{n=+n||0;return '$'+(n===0||Math.abs(n)>=.01?n.toFixed(2):n.toFixed(4))};
const metric=u=>cm.sort==='requests'?(+u?.requests||0):cm.sort==='tokens'?(+u?.tokens||0):(+u?.cost_usd||0);
const metricLabel=()=>cm.sort==='requests'?'请求数':cm.sort==='tokens'?'Tokens':'用户侧消费';
const metricText=u=>cm.sort==='requests'?nfmt(u?.requests):cm.sort==='tokens'?compact(u?.tokens):usd(u?.cost_usd);
const dateTime=ts=>ts?new Date(ts*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'—';
const shortDateTime=ts=>ts?new Date(ts*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}):'—';
const cstDate=ts=>{const p=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date(ts*1000));const v=t=>p.find(x=>x.type===t)?.value||'';return `${v('year')}-${v('month')}-${v('day')}`};
const cmPresetRange=(preset,now=Date.now())=>{
  const ts=Math.floor(now/1000),today=cstDate(ts);
  if(preset==='today')return {from:today,to:today};
  if(preset==='yesterday'){const day=cstDate(ts-86400);return {from:day,to:day}}
  const weekday=new Intl.DateTimeFormat('en-US',{timeZone:'Asia/Shanghai',weekday:'short'}).format(new Date(now));
  const sinceMonday=({Mon:0,Tue:1,Wed:2,Thu:3,Fri:4,Sat:5,Sun:6})[weekday]??0;
  return {from:cstDate(ts-sinceMonday*86400),to:today};
};

window.channelManagementActivate=function(){
  if(!cm.inited)init();
  const changed=applyNavigationContext();
  if(!cm.loaded||changed)loadReport();
};
window.channelManagementOpen=function(context){window.monitorNavigate?.('channels',context||{})};
function applyNavigationContext(){
  const c=window.monitorNavigationContext?.()||{};let changed=false;
  if(!Object.keys(c).length)return false;
  if(c.from&&c.to){if(!cm.custom||cm.custom.from!==c.from||cm.custom.to!==c.to){cm.custom={from:c.from,to:c.to};cm.preset='custom';changed=true}}
  else if(+c.days>0&&cm.days!==+c.days){cm.days=+c.days;cm.custom=null;cm.preset='';changed=true}
  const search=c.channel?String(c.channel):(c.domain||'');
  if(cm.filters.search!==search){cm.filters.search=search;changed=true}
  for(const [key,value] of Object.entries({domain:'',vendor:'',group:c.group||'',status:''}))if(cm.filters[key]!==value){cm.filters[key]=value;changed=true}
  if($('cmSearch'))$('cmSearch').value=search;
  syncRange();return changed;
}

function init(){
  cm.inited=true;
  document.querySelectorAll('[data-cm-days]').forEach(btn=>btn.addEventListener('click',()=>{
    cm.days=+btn.dataset.cmDays;cm.custom=null;cm.preset='';syncRange();loadReport();
  }));
  document.querySelectorAll('[data-cm-preset]').forEach(btn=>btn.addEventListener('click',()=>{
    cm.preset=btn.dataset.cmPreset;cm.custom=cmPresetRange(cm.preset);syncRange();loadReport();
  }));
  $('cmCustomToggle')?.addEventListener('click',()=>{
    $('cmCustomRange')?.classList.toggle('show');
    $('cmCustomToggle')?.classList.toggle('active',$('cmCustomRange')?.classList.contains('show'));
  });
  $('cmCustomApply')?.addEventListener('click',()=>{
    const from=$('cmCustomFrom').value,to=$('cmCustomTo').value;
    if(!from||!to||from>to){showError('请选择有效的开始和结束日期。');return}
    cm.custom={from,to};cm.preset='custom';syncRange();loadReport();
  });
  ['cmDomain','cmVendor','cmGroup','cmStatus'].forEach(id=>$(id)?.addEventListener('change',()=>{
    const key={cmDomain:'domain',cmVendor:'vendor',cmGroup:'group',cmStatus:'status'}[id];
    cm.filters[key]=$(id).value;render();
  }));
  $('cmSearch')?.addEventListener('input',()=>{cm.filters.search=$('cmSearch').value.trim().toLowerCase();render()});
  $('cmReset')?.addEventListener('click',()=>{
    cm.filters={search:'',domain:'',vendor:'',group:'',status:''};
    if($('cmSearch'))$('cmSearch').value='';
    ['cmDomain','cmVendor','cmGroup','cmStatus'].forEach(id=>{if($(id))$(id).value=''});
    render();
  });
  document.querySelectorAll('[data-cm-sort]').forEach(btn=>btn.addEventListener('click',()=>{
    cm.sort=btn.dataset.cmSort;
    document.querySelectorAll('[data-cm-sort]').forEach(x=>x.classList.toggle('active',x===btn));
    render();
  }));
  $('cmBody')?.addEventListener('click',event=>{
    const stability=event.target.closest('[data-cm-stability]');
    if(stability){event.stopPropagation();const ctx={days:cm.days,channel:stability.dataset.cmStability};if(cm.custom){ctx.from=cm.custom.from;ctx.to=cm.custom.to;delete ctx.days}if(cm.filters.group)ctx.group=cm.filters.group;window.stabilityOpen?.(ctx);return}
    const finance=event.target.closest('[data-cm-finance]');
    if(finance){event.stopPropagation();openFinance(finance.dataset.cmFinance);return}
    const domain=event.target.closest('[data-cm-domain-toggle]');
    if(domain){toggleSet(cm.expandedDomains,domain.dataset.cmDomainToggle);render();return}
    const vendor=event.target.closest('[data-cm-vendor-toggle]');
    if(vendor){toggleSet(cm.expandedVendors,vendor.dataset.cmVendorToggle);render();return}
    const channel=event.target.closest('[data-cm-channel-toggle]');
    if(channel){toggleSet(cm.expandedChannels,channel.dataset.cmChannelToggle);render()}
  });
  $('cmBody')?.addEventListener('keydown',event=>{
    if(event.key!=='Enter'&&event.key!==' ')return;
    if(event.target.closest('[data-cm-finance]'))return;
    const target=event.target.closest('[data-cm-domain-toggle],[data-cm-vendor-toggle],[data-cm-channel-toggle]');
    if(target){event.preventDefault();target.click()}
  });
  $('cmFinanceClose')?.addEventListener('click',closeFinance);
  $('cmFinanceCancel')?.addEventListener('click',closeFinance);
  $('cmFinanceMask')?.addEventListener('click',closeFinance);
  $('cmFinanceSave')?.addEventListener('click',saveFinance);
  ['cmFinanceFX','cmFinanceSitePaid','cmFinanceSiteCredit','cmFinanceUpPaid','cmFinanceUpCredit'].forEach(id=>$(id)?.addEventListener('input',refreshFinancePreview));
  $('cmFinanceGroupRows')?.addEventListener('input',event=>{if(event.target.matches('[data-cm-finance-input]'))refreshFinancePreview()});
  document.addEventListener('keydown',event=>{if(event.key==='Escape'&&cm.financeDomain)closeFinance()});
  window.addEventListener('monitor:navigate',event=>{if(event.detail?.tab==='channels'&&applyNavigationContext())loadReport()});
}

function toggleSet(set,key){if(set.has(key))set.delete(key);else set.add(key)}
function syncRange(){
  document.querySelectorAll('[data-cm-days]').forEach(btn=>btn.classList.toggle('active',!cm.custom&&+btn.dataset.cmDays===cm.days));
  document.querySelectorAll('[data-cm-preset]').forEach(btn=>btn.classList.toggle('active',btn.dataset.cmPreset===cm.preset));
  $('cmCustomToggle')?.classList.toggle('active',cm.preset==='custom');
  $('cmCustomRange')?.classList.toggle('show',cm.preset==='custom');
}
function queryString(){
  const q=new URLSearchParams();
  if(cm.custom){q.set('from',cm.custom.from);q.set('to',cm.custom.to)}else q.set('days',String(cm.days));
  return q.toString();
}
function loading(){if($('cmBody'))$('cmBody').innerHTML='<div class="cm-loading"><i></i><span>正在读取本地渠道用量汇总…</span></div>'}
function showError(message){if($('cmBody'))$('cmBody').innerHTML=`<div class="cm-empty"><b>渠道数据暂时无法读取</b><p>${esc(message||'请稍后重试。')}</p></div>`}
async function loadReport(){
  if(cm.abort)cm.abort.abort();
  cm.abort=new AbortController();loading();
  try{
    const res=await fetch('/channels/report?'+queryString(),{headers:{Accept:'application/json'},signal:cm.abort.signal});
    if(res.status===401){location.href='/login';return}
    const data=await res.json();
    if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    if(data.enabled===false){showError('渠道用量依赖稳定性本地小时汇总，当前功能未启用。');return}
    cm.report=data;cm.loaded=true;populateFilters();render();
  }catch(error){if(error.name!=='AbortError')showError(error.message)}
}

function setOptions(id,items,current,placeholder){
  const el=$(id);if(!el)return;
  el.innerHTML=`<option value="">${esc(placeholder)}</option>`+items.map(v=>`<option value="${esc(v)}">${esc(v)}</option>`).join('');
  el.value=current||'';
}
function populateFilters(){
  const f=cm.report?.filters||{};
  setOptions('cmDomain',f.domains||[],cm.filters.domain,'全部主域名');
  setOptions('cmVendor',f.vendors||[],cm.filters.vendor,'全部厂商类型');
  setOptions('cmGroup',f.groups||[],cm.filters.group,'全部服务分组');
}

function statusMatches(ch){
  const status=cm.filters.status;
  if(!status)return true;
  if(status==='historical')return !ch.current;
  if(status==='enabled')return ch.current&&+ch.status===1;
  return ch.current&&+ch.status!==1;
}
function sortByMetric(rows,name){
  return rows.sort((a,b)=>metric(b.usage)-metric(a.usage)||(+b.usage.requests||0)-(+a.usage.requests||0)||String(a[name]||'').localeCompare(String(b[name]||''),'zh-CN'));
}
function filteredDomains(){
  const q=cm.filters.search;
  const out=[];
  for(const sourceDomain of cm.report?.domains||[]){
    if(cm.filters.domain&&sourceDomain.domain!==cm.filters.domain)continue;
    const domainHit=q&&sourceDomain.domain.toLowerCase().includes(q);
    const domain={...sourceDomain,usage:zero(),groups:[],vendors:[]};
    const domainGroups=new Map();
    for(const sourceVendor of sourceDomain.vendors||[]){
      if(cm.filters.vendor&&sourceVendor.name!==cm.filters.vendor)continue;
      const vendorHit=q&&sourceVendor.name.toLowerCase().includes(q);
      const vendor={...sourceVendor,usage:zero(),channels:[]};
      for(const sourceChannel of sourceVendor.channels||[]){
        if(!statusMatches(sourceChannel))continue;
        const channelHit=!q||domainHit||vendorHit||sourceChannel.name.toLowerCase().includes(q)||(sourceChannel.host||'').toLowerCase().includes(q)||String(sourceChannel.id)===q||('#'+sourceChannel.id).includes(q);
        if(!channelHit)continue;
        let groups=[...(sourceChannel.groups||[])];
        let usage={...sourceChannel.usage};
        if(cm.filters.group){
          const found=groups.find(g=>g.name===cm.filters.group);
          const configured=(sourceChannel.configured_groups||[]).includes(cm.filters.group);
          if(!found&&!configured)continue;
          groups=found?[found]:[];usage=found?{...found.usage}:zero();
        }
        const channel={...sourceChannel,usage,groups};
        vendor.channels.push(channel);add(vendor.usage,usage);add(domain.usage,usage);
        for(const group of groups){
          const current=domainGroups.get(group.name)||zero();add(current,group.usage);domainGroups.set(group.name,current);
        }
      }
      if(vendor.channels.length){sortByMetric(vendor.channels,'name');domain.vendors.push(vendor)}
    }
    if(domain.vendors.length){
      sortByMetric(domain.vendors,'name');
      domain.groups=[...domainGroups.entries()].map(([name,usage])=>({name,usage}));sortByMetric(domain.groups,'name');
      out.push(domain);
    }
  }
  return sortByMetric(out,'domain');
}

function statusLabel(ch){
  if(!ch.current)return'<span class="cm-status historical">历史记录</span>';
  if(+ch.status===1)return'<span class="cm-status enabled">启用</span>';
  if(+ch.status===3)return'<span class="cm-status disabled">自动禁用</span>';
  return'<span class="cm-status disabled">停用</span>';
}
function metricCell(usage,total){
  const share=metric(total)>0?metric(usage)/metric(total)*100:0;
  return `<div class="cm-metric-main"><b>${esc(metricText(usage))}</b><small>${share.toFixed(1)}%</small></div>`;
}
const pct=n=>(+n*100).toFixed(2)+'%';
function multiplierGap(n){
  const value=Math.abs(+n)<1e-9?0:+n;
  const number=Math.abs(value).toFixed(4).replace(/0+$/,'').replace(/\.$/,'')||'0';
  return `${value>0?'+':value<0?'-':''}${number}x`;
}
function groupFinance(finance){
  const multipliersReady=!!finance?.site_configured&&!!finance?.upstream_configured;
  if(!multipliersReady){
    const missing=!finance?.site_configured&&!finance?.upstream_configured?'双方倍率待配置':!finance?.site_configured?'我方倍率待配置':'上游倍率待配置';
    return `<div class="cm-group-finance pending"><span>${esc(missing)}</span></div>`;
  }
  const gap=`<span class="cm-multiplier-gap" title="我方倍率 − 上游实际倍率（基础倍率 × 折扣系数 ÷ 充值比例）">倍率差 <b>${esc(multiplierGap(finance.multiplier_gap))}</b></span>`;
  if(!finance.complete)return `<div class="cm-group-finance incomplete">${gap}<span>折扣口径待配置</span></div>`;
  const margin=+finance.estimated_margin||0,state=margin<0?'loss':margin<.1?'low':'good';
  return `<div class="cm-group-finance">${gap}<span title="我方折扣 / 上游折扣">双方折扣 <b>${pct(finance.site_discount)} / ${pct(finance.upstream_discount)}</b></span><strong class="${state}">预估毛利 ${pct(margin)}</strong></div>`;
}
function groupUsage(groups,total){
  if(!groups?.length)return'<div class="cm-no-groups">该范围暂无服务分组用量</div>';
  return `<div class="cm-domain-groups"><div class="cm-domain-groups-title"><b>服务分组用量</b><span>当前排行口径：${esc(metricLabel())}</span></div><div class="cm-group-grid">${groups.map(group=>{
    const share=metric(total)>0?metric(group.usage)/metric(total)*100:0;
    return `<div class="cm-group-chip"><span title="${esc(group.name)}">${esc(group.name)}</span><b>${esc(metricText(group.usage))}</b><i><em style="width:${Math.max(share&&2,share)}%"></em></i><small>${share.toFixed(1)}%</small>${groupFinance(group.finance)}</div>`;
  }).join('')}</div></div>`;
}
function channelGroups(domainKey,ch){
  const key=domainKey+':'+ch.id;
  if(!cm.expandedChannels.has(key))return'';
  return `<div class="cm-channel-groups"><div class="cm-channel-group-head"><span>服务分组</span><span>请求数</span><span>Tokens</span><span>用户侧消费</span><span>渠道内占比</span></div>${(ch.groups||[]).map(group=>{
    const share=metric(ch.usage)>0?metric(group.usage)/metric(ch.usage)*100:0;
    return `<div class="cm-channel-group-row"><b>${esc(group.name)}</b><span>${nfmt(group.usage.requests)}</span><span title="${nfmt(group.usage.tokens)}">${compact(group.usage.tokens)}</span><span>${usd(group.usage.cost_usd)}</span><span>${share.toFixed(1)}%</span></div>`;
  }).join('')||'<div class="cm-no-groups">该渠道在当前范围无服务分组用量</div>'}</div>`;
}
function channelRow(domainKey,ch,domainTotal){
  const configured=(ch.configured_groups||[]).join('、')||'未配置分组';
  const key=domainKey+':'+ch.id;
  const host=ch.host?`主机 ${ch.host} · `:'';
  return `<div class="cm-channel-wrap"><div class="cm-channel-row">
    <div class="cm-channel-name"><b>#${ch.id} ${esc(ch.name)}</b><small title="${esc(host+'配置分组 '+configured)}"><span class="cm-channel-host">${esc(host)}</span>配置分组 ${esc(configured)} · ${nfmt(ch.model_count)} 个模型</small></div>
    <div>${statusLabel(ch)}</div>
    <span class="cm-number">${nfmt(ch.usage.requests)}</span>
    <span class="cm-number" title="${nfmt(ch.usage.tokens)}">${compact(ch.usage.tokens)}</span>
    <span class="cm-number">${usd(ch.usage.cost_usd)}</span>
    ${metricCell(ch.usage,domainTotal)}
    <div class="cm-channel-actions"><button type="button" class="cm-detail-btn" data-cm-stability="${ch.id}">稳定性</button><button type="button" class="cm-detail-btn" data-cm-channel-toggle="${esc(key)}">${cm.expandedChannels.has(key)?'收起':'分组用量'} <i>${cm.expandedChannels.has(key)?'▴':'▾'}</i></button></div>
  </div>${channelGroups(domainKey,ch)}</div>`;
}
function vendorSection(domain,vendor){
  const key=domain.key+':vendor:'+vendor.name,open=cm.expandedVendors.has(key);
  return `<section class="cm-vendor-section${open?' open':''}"><header class="cm-vendor-head" role="button" tabindex="0" aria-expanded="${open}" data-cm-vendor-toggle="${esc(key)}"><div><span class="cm-vendor-dot"></span><b>${esc(vendor.name)}</b><small>${vendor.channels.length} 个实际渠道</small></div><div><span>${nfmt(vendor.usage.requests)} 渠道请求</span><span>${compact(vendor.usage.tokens)} Tokens</span><strong>${usd(vendor.usage.cost_usd)}</strong><i class="cm-vendor-chevron">${open?'−':'+'}</i></div></header>${open?`<div class="cm-vendor-body">
    <div class="cm-channel-head"><span>实际渠道</span><span>状态</span><span>请求数</span><span>Tokens</span><span>用户侧消费</span><span>${esc(metricLabel())}占比</span><span>明细</span></div>
    ${vendor.channels.map(ch=>channelRow(domain.key,ch,domain.usage)).join('')}</div>`:''}</section>`;
}
function domainCard(domain,index,total,filtered){
  const channels=domain.vendors.flatMap(v=>v.channels),enabled=channels.filter(ch=>ch.current&&+ch.status===1).length;
  const open=cm.expandedDomains.has(domain.key),share=metric(total)>0?metric(domain.usage)/metric(total)*100:0;
  const financeGroups=domain.finance_groups||[],completeFinance=financeGroups.filter(g=>g.finance?.complete).length;
  const financeVersion=+domain.finance?.version||0;
  const financeLabel=!financeGroups.length?'暂无可配置分组':financeVersion?`倍率版本 v${financeVersion} · ${completeFinance}/${financeGroups.length} 分组`:`倍率待配置 · 0/${financeGroups.length} 分组`;
  const financeButton=cm.report?.finance?.can_edit&&domain.configured?`<button type="button" class="cm-finance-open" data-cm-finance="${esc(domain.key)}">倍率配置</button>`:'';
  return `<article class="cm-domain-card${open?' open':''}"><div class="cm-domain-head" role="button" tabindex="0" data-cm-domain-toggle="${esc(domain.key)}">
    <span class="cm-rank">${String(index+1).padStart(2,'0')}</span>
    <div class="cm-domain-identity"><span class="cm-domain-icon">${domain.configured?'◎':'—'}</span><div><b>${esc(domain.domain)}</b><small>${domain.vendors.length} 个厂商 · ${channels.length} 个实际渠道 · ${enabled} 个启用</small><div class="cm-domain-finance"><span class="${completeFinance===financeGroups.length&&financeGroups.length?'ready':'pending'}">${esc(financeLabel)}</span>${financeButton}</div></div></div>
    <div class="cm-share"><div><b>${share.toFixed(1)}%</b><small>${filtered?'筛选内':'全站'}${esc(metricLabel())}</small></div><i><em style="width:${Math.max(share&&2,share)}%"></em></i></div>
    <div class="cm-domain-metrics"><span><small>渠道请求数</small><b>${nfmt(domain.usage.requests)}</b></span><span><small>Tokens</small><b title="${nfmt(domain.usage.tokens)}">${compact(domain.usage.tokens)}</b></span><span><small>用户侧消费</small><b>${usd(domain.usage.cost_usd)}</b></span></div>
    <span class="cm-chevron">${open?'−':'+'}</span>
  </div>${open?`<div class="cm-domain-body">${groupUsage(domain.groups,domain.usage)}${domain.vendors.map(v=>vendorSection(domain,v)).join('')}</div>`:''}</article>`;
}

function filtersActive(){return !!(cm.filters.search||cm.filters.domain||cm.filters.vendor||cm.filters.group||cm.filters.status)}
function freshness(meta){
  const historical=meta?.to&&meta.to<cstDate(Date.now()/1000);
  if(historical)return `<span class="cm-fresh-state neutral"><i></i>区间数据截至 ${esc(shortDateTime(meta.data_until))}</span><small>历史区间 · 小时汇总</small>`;
  const until=+meta?.latest_data_until||+meta?.data_until||0;
  if(!until)return '<span class="cm-fresh-state stale"><i></i>暂无小时汇总数据</span><small>等待本地采集器生成汇总</small>';
  const ageSec=Math.max(0,Date.now()/1000-until),state=ageSec<=7200?'normal':ageSec<=10800?'warning':'stale';
  const label=state==='normal'?'正常':state==='warning'?'存在延迟':'采集可能中断';
  return `<span class="cm-fresh-state ${state}"><i></i>小时汇总 · ${label} · 截至 ${esc(shortDateTime(until))}</span><small>${state==='normal'?'正常延迟不超过约 2 小时':'请核对本地采集器状态'}</small>`;
}

function financeNumber(id){
  const value=Number($(id)?.value);
  return Number.isFinite(value)&&value>0?value:null;
}
function financeGroupInput(index,side){
  const el=document.querySelector(`[data-cm-finance-input="${side}"][data-cm-finance-index="${index}"]`);
  if(!el||el.value.trim()==='')return null;
  const value=Number(el.value);
  return Number.isFinite(value)&&value>0?value:null;
}
function showFinanceMessage(message,error=false){
  const el=$('cmFinanceMessage');if(!el)return;
  el.textContent=message||'';el.classList.toggle('error',!!error);
}
function renderFinanceRows(){
  const rows=cm.financeGroups||[];
  $('cmFinanceGroupRows').innerHTML=rows.map((row,index)=>{
    const finance=row.finance||{};
    const site=finance.site_configured?finance.site_multiplier:'';
    const upstream=finance.upstream_configured?finance.upstream_multiplier:'';
    const upstreamFactor=finance.upstream_configured?(finance.upstream_discount_factor||1):'';
    return `<div class="cm-finance-group-row" data-cm-finance-row="${index}">
      <b title="${esc(row.name)}">${esc(row.name)}</b>
      <input type="number" min="0.000001" step="0.0001" inputmode="decimal" value="${site}" data-cm-finance-input="site" data-cm-finance-index="${index}" aria-label="${esc(row.name)} 我方倍率">
      <input type="number" min="0.000001" step="0.0001" inputmode="decimal" value="${upstream}" data-cm-finance-input="upstream" data-cm-finance-index="${index}" aria-label="${esc(row.name)} 上游基础倍率">
      <input type="number" min="0.000001" step="0.0001" inputmode="decimal" value="${upstreamFactor}" placeholder="1" data-cm-finance-input="upstream-factor" data-cm-finance-index="${index}" aria-label="${esc(row.name)} 上游折扣系数">
      <span data-cm-finance-effective="${index}">—</span><span data-cm-finance-discounts="${index}">—</span><strong data-cm-finance-margin="${index}">待配置</strong>
    </div>`;
  }).join('')||'<div class="cm-no-groups">该主域名暂无可配置服务分组</div>';
  refreshFinancePreview();
}
function refreshFinancePreview(){
  if(!cm.financeDomain)return;
  const fx=financeNumber('cmFinanceFX'),sitePaid=financeNumber('cmFinanceSitePaid'),siteCredit=financeNumber('cmFinanceSiteCredit');
  const upPaid=financeNumber('cmFinanceUpPaid'),upCredit=financeNumber('cmFinanceUpCredit');
  let complete=0;
  (cm.financeGroups||[]).forEach((_,index)=>{
    const site=financeGroupInput(index,'site'),up=financeGroupInput(index,'upstream'),upFactor=financeGroupInput(index,'upstream-factor');
    const siteDiscount=fx&&sitePaid&&siteCredit&&site?site*(sitePaid/siteCredit)/fx:null;
    const effective=upPaid&&upCredit&&up&&upFactor?up*upFactor*(upPaid/upCredit):null;
    const upDiscount=fx&&effective?effective/fx:null;
    const margin=siteDiscount&&upDiscount!=null?(siteDiscount-upDiscount)/siteDiscount:null;
    const effectiveEl=document.querySelector(`[data-cm-finance-effective="${index}"]`),discountsEl=document.querySelector(`[data-cm-finance-discounts="${index}"]`),marginEl=document.querySelector(`[data-cm-finance-margin="${index}"]`);
    if(effectiveEl)effectiveEl.textContent=effective==null?'—':effective.toFixed(4).replace(/0+$/,'').replace(/\.$/,'');
    if(discountsEl){
      discountsEl.textContent=siteDiscount==null||upDiscount==null?'—':`${pct(siteDiscount)} / ${pct(upDiscount)}`;
      discountsEl.title='我方计价折扣 / 上游计价折扣';
    }
    if(marginEl){
      marginEl.textContent=margin==null?'待配置':pct(margin);
      marginEl.className=margin==null?'':margin<0?'loss':margin<.1?'low':'good';
    }
    document.querySelector(`[data-cm-finance-row="${index}"]`)?.classList.toggle('complete',margin!=null);
    if(margin!=null)complete++;
  });
  if($('cmFinanceConfiguredCount'))$('cmFinanceConfiguredCount').textContent=`${complete}/${cm.financeGroups.length} 个分组口径完整`;
}
function openFinance(domainKey){
  if(!cm.report?.finance?.can_edit)return;
  const domain=(cm.report.domains||[]).find(item=>item.key===domainKey&&item.configured);
  if(!domain)return;
  cm.financeDomain=domain;
  cm.financeGroups=[...(domain.finance_groups||[])];
  const global=cm.report.finance||{},upstream=domain.finance||{};
  $('cmFinanceTitle').textContent=`${domain.domain} · 成本与毛利率`;
  $('cmFinanceSubtitle').textContent=upstream.version
    ? `当前版本 v${upstream.version} · 生效于 ${dateTime(upstream.effective_at)}。更新会创建新版本，旧版本永久保留。`
    : '首次保存将创建倍率版本 v1，生效时间为保存时间。配置只保存在 Monitor 本地。';
  $('cmFinanceFX').value=global.fx_benchmark||7;
  $('cmFinanceSitePaid').value=global.site_recharge_paid||1;
  $('cmFinanceSiteCredit').value=global.site_recharge_credit||1;
  $('cmFinanceUpPaid').value=upstream.configured?upstream.recharge_paid:1;
  $('cmFinanceUpCredit').value=upstream.configured?upstream.recharge_credit:1;
  $('cmFinanceSave').textContent=upstream.version?'更新为新版本':'保存为 v1';
  showFinanceMessage('');renderFinanceRows();
  $('cmFinanceMask').hidden=false;
  $('cmFinanceDialog').classList.add('show');$('cmFinanceDialog').setAttribute('aria-hidden','false');
  document.body.classList.add('cm-dialog-open');
  setTimeout(()=>$('cmFinanceFX')?.focus(),30);
}
function closeFinance(){
  if(!cm.financeDomain)return;
  cm.financeDomain=null;cm.financeGroups=[];
  $('cmFinanceMask').hidden=true;$('cmFinanceDialog').classList.remove('show');$('cmFinanceDialog').setAttribute('aria-hidden','true');
  document.body.classList.remove('cm-dialog-open');showFinanceMessage('');
}
async function saveFinance(){
  if(!cm.financeDomain||!cm.report?.finance?.can_edit)return;
  const values={
    fx_benchmark:financeNumber('cmFinanceFX'),site_recharge_paid:financeNumber('cmFinanceSitePaid'),site_recharge_credit:financeNumber('cmFinanceSiteCredit'),
    upstream_recharge_paid:financeNumber('cmFinanceUpPaid'),upstream_recharge_credit:financeNumber('cmFinanceUpCredit')
  };
  if(Object.values(values).some(value=>value==null)){showFinanceMessage('折扣基准和双方充值比例都必须填写大于 0 的数字。',true);return}
  const groups=[];
  for(let index=0;index<cm.financeGroups.length;index++){
    const siteEl=document.querySelector(`[data-cm-finance-input="site"][data-cm-finance-index="${index}"]`),upEl=document.querySelector(`[data-cm-finance-input="upstream"][data-cm-finance-index="${index}"]`),factorEl=document.querySelector(`[data-cm-finance-input="upstream-factor"][data-cm-finance-index="${index}"]`);
    const siteRaw=siteEl?.value.trim()||'',upRaw=upEl?.value.trim()||'',factorRaw=factorEl?.value.trim()||'';
    if(!siteRaw&&!upRaw&&!factorRaw)continue;
    const site=financeGroupInput(index,'site'),upstream=financeGroupInput(index,'upstream'),upstreamFactor=financeGroupInput(index,'upstream-factor');
    if(site==null||upstream==null||upstreamFactor==null){showFinanceMessage(`${cm.financeGroups[index].name} 必须同时填写我方倍率、上游基础倍率和上游折扣系数，且都大于 0。`,true);return}
    groups.push({group:cm.financeGroups[index].name,site_multiplier:site,upstream_multiplier:upstream,upstream_discount_factor:upstreamFactor});
  }
  const payload={domain:cm.financeDomain.domain,...values,groups};
  const button=$('cmFinanceSave');button.disabled=true;showFinanceMessage('正在核对倍率版本…');
  try{
    let res=await fetch('/channels/finance',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify(payload)});
    if(res.status===401){location.href='/login';return}
    let data=await res.json();
    if(res.status===409&&data.confirmation_required){
	  const globalImpact=data.global_changed&&+data.affected_domains>1
	    ?`\n\n我方计价基准发生变化，将同步核对 ${data.affected_domains} 个主域名并为实际受影响的渠道追加版本。`
	    :'';
      const confirmed=window.confirm(`当前是倍率版本 v${data.current_version}。\n\n确认更新为 v${data.next_version} 吗？${globalImpact}\n旧版本会完整保留，新版本从确认保存时开始生效。`);
      if(!confirmed){showFinanceMessage('已取消更新，当前版本未变更。');return}
      showFinanceMessage(`正在创建倍率版本 v${data.next_version}…`);
      res=await fetch('/channels/finance',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify({...payload,confirm_update:true,expected_version:data.current_version,expected_global_revision:data.current_global_revision||''})});
      if(res.status===401){location.href='/login';return}
      data=await res.json();
    }
    if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    if(data.unchanged){showFinanceMessage(`配置没有变化，仍为倍率版本 v${data.version}。`);return}
    closeFinance();cm.loaded=false;await loadReport();
  }catch(error){showFinanceMessage(error.message||'保存失败，请稍后重试。',true)}finally{button.disabled=false}
}

function render(){
  if(!cm.report)return;
  const domains=filteredDomains();
  const filteredUsage=zero(),channels=[];
  domains.forEach(domain=>{add(filteredUsage,domain.usage);domain.vendors.forEach(v=>channels.push(...v.channels))});
  const configuredDomains=domains.filter(d=>d.configured).length,currentChannels=channels.filter(ch=>ch.current),enabled=currentChannels.filter(ch=>+ch.status===1).length;
  const unconfigured=domains.filter(d=>d.key==='special:unconfigured').flatMap(d=>d.vendors.flatMap(v=>v.channels)).filter(ch=>ch.current).length;
  const historical=channels.filter(ch=>!ch.current).length,filtered=filtersActive();
  const allUsage=cm.report.summary?.usage||zero();
  const share=metric(allUsage)>0?metric(filteredUsage)/metric(allUsage)*100:0;
  const dataCoverage=cm.report.meta?.data_coverage||{};
  const coverageWarning=dataCoverage.complete?'':`<div class="alert">当前日期范围的小时数据完整率为 ${(+dataCoverage.percent||0).toFixed(1)}%（${nfmt(dataCoverage.completed_hours)}/${nfmt(dataCoverage.expected_hours)} 小时）；缺失时段不会被当作零流量，当前金额、请求与 Tokens 可能偏低。</div>`;
  $('cmBody').innerHTML=`${coverageWarning}<section class="cm-kpis">
    <article><small>已配置主域名</small><b>${nfmt(configuredDomains)}</b><span>当前显示 ${nfmt(domains.length)} 个归并项</span></article>
    <article><small>当前实际渠道</small><b>${nfmt(currentChannels.length)}</b><span>${nfmt(enabled)} 启用 · ${nfmt(currentChannels.length-enabled)} 停用${historical?' · '+nfmt(historical)+' 历史':''}</span></article>
    <article class="${unconfigured?'warn':''}"><small>未归并渠道</small><b>${nfmt(unconfigured)}</b><span>${unconfigured?'尚未配置主地址':'当前渠道均已归并'}</span></article>
    <article><small>渠道请求数</small><b>${nfmt(filteredUsage.requests)}</b><span>${cm.report.meta.from} 至 ${cm.report.meta.to}</span></article>
    <article><small>区间 Tokens</small><b title="${nfmt(filteredUsage.tokens)}">${compact(filteredUsage.tokens)}</b><span>prompt + completion</span></article>
    <article class="accent"><small>用户侧消费</small><b>${usd(filteredUsage.cost_usd)}</b><span>NewAPI logs.quota</span></article>
    ${filtered?`<article><small>筛选${esc(metricLabel())}占比</small><b>${share.toFixed(1)}%</b><span>相对当前日期全部渠道</span></article>`:''}
  </section>
  <section class="cm-list-head"><div><h3>渠道排名</h3><p>共 ${nfmt(domains.length)} 个归并项 · 按 ${esc(metricLabel())} 从高到低排序，逐级展开厂商类型、实际渠道与服务分组。</p></div><div class="cm-fresh">${freshness(cm.report.meta)}<small>渠道配置快照 ${esc(dateTime(cm.report.meta.channel_config_updated_at))}</small></div></section>
  <div class="cm-domain-list">${domains.map((domain,index)=>domainCard(domain,index,filteredUsage,filtered)).join('')||'<div class="cm-empty"><b>当前筛选没有匹配渠道</b><p>请重置筛选条件或更换日期范围。</p></div>'}</div>`;
}

document.addEventListener('DOMContentLoaded',()=>{if($('tab-channels')&&!cm.inited)init()});
})();
