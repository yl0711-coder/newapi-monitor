(function(){
'use strict';

const cm={
  inited:false,loaded:false,loadedAt:0,loading:false,refreshTimer:null,hours:24,days:7,custom:null,preset:'',report:null,abort:null,sort:'cost',
  economics:null,economicsError:'',economicsSeq:0,economicsHourly:new Map(),economicsHourlySeq:new Map(),
  costLedger:new Map(),costLedgerSeq:new Map(),costLedgerOpen:new Set(),
  pricingOps:new Map(),
  filters:{search:'',domain:'',vendor:'',group:'',status:''},
  expandedDomains:new Set(),expandedVendors:new Set(),collapsedGroups:new Set(),
  financeMode:'',financeDomain:null,financeGroups:[],financeChannels:[],financeChannel:null,upstreamDomain:null,upstreamConfig:null,
  removedAICodeWithKeyIDs:new Set()
};
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const zero=()=>({requests:0,tokens:0,cost_usd:0});
const add=(a,b)=>{a.requests+=(+b?.requests||0);a.tokens+=(+b?.tokens||0);a.cost_usd+=(+b?.cost_usd||0);return a};
const nfmt=n=>(+n||0).toLocaleString('zh-CN');
const compact=n=>{n=+n||0;const a=Math.abs(n);for(const [d,u] of [[1e12,'T'],[1e9,'B'],[1e6,'M'],[1e3,'k']])if(a>=d)return (n/d>=100?(n/d).toFixed(0):(n/d).toFixed(1)).replace(/\.0$/,'')+u;return nfmt(n)};
const usd=n=>{n=+n||0;const digits=n===0||Math.abs(n)>=.01?2:4;return '$'+n.toLocaleString('en-US',{minimumFractionDigits:digits,maximumFractionDigits:digits})};
const metric=u=>cm.sort==='requests'?(+u?.requests||0):cm.sort==='tokens'?(+u?.tokens||0):(+u?.cost_usd||0);
const metricLabel=()=>cm.sort==='requests'?'请求数':cm.sort==='tokens'?'Tokens':'用户侧消费';
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
  if(!cm.loaded||changed||Date.now()-cm.loadedAt>=30000)loadReport({quiet:cm.loaded&&!changed});
  scheduleReportRefresh();
};
window.channelManagementDeactivate=function(){if(cm.refreshTimer){clearTimeout(cm.refreshTimer);cm.refreshTimer=null}};
window.channelManagementOpen=function(context){window.monitorNavigate?.('channels',context||{})};
function scheduleReportRefresh(){
  if(cm.refreshTimer)clearTimeout(cm.refreshTimer);
  if($('tab-channels')?.hidden)return;
  cm.refreshTimer=setTimeout(async()=>{
    cm.refreshTimer=null;
    // 编辑弹窗或计价台账打开时不在后台替换其依赖的报表，
    // 避免自动刷新打断人工配置和审批。
    if(!$('tab-channels')?.hidden&&!cm.financeMode&&!cm.upstreamDomain&&!cm.costLedgerOpen.size&&!cm.pricingOps.size)await loadReport({quiet:true});
    scheduleReportRefresh();
  },60000);
}
function applyNavigationContext(){
  const c=window.monitorNavigationContext?.()||{};let changed=false;
  if(!Object.keys(c).length)return false;
  if(c.from&&c.to){if(!cm.custom||cm.custom.from!==c.from||cm.custom.to!==c.to||cm.hours){cm.custom={from:c.from,to:c.to};cm.hours=0;cm.preset='custom';changed=true}}
  else if(+c.hours>0&&cm.hours!==+c.hours){cm.hours=+c.hours;cm.custom=null;cm.preset='';changed=true}
  else if(+c.days>0&&(cm.days!==+c.days||cm.hours)){cm.days=+c.days;cm.hours=0;cm.custom=null;cm.preset='';changed=true}
  const search=c.channel?String(c.channel):(c.domain||'');
  if(cm.filters.search!==search){cm.filters.search=search;changed=true}
  for(const [key,value] of Object.entries({domain:'',vendor:'',group:c.group||'',status:''}))if(cm.filters[key]!==value){cm.filters[key]=value;changed=true}
  if($('cmSearch'))$('cmSearch').value=search;
  syncRange();return changed;
}

function init(){
  cm.inited=true;
  document.querySelectorAll('[data-cm-hours]').forEach(btn=>btn.addEventListener('click',()=>{
    cm.hours=+btn.dataset.cmHours;cm.custom=null;cm.preset='';syncRange();loadReport();
  }));
  document.querySelectorAll('[data-cm-days]').forEach(btn=>btn.addEventListener('click',()=>{
    cm.days=+btn.dataset.cmDays;cm.hours=0;cm.custom=null;cm.preset='';syncRange();loadReport();
  }));
  document.querySelectorAll('[data-cm-preset]').forEach(btn=>btn.addEventListener('click',()=>{
    cm.hours=0;cm.preset=btn.dataset.cmPreset;cm.custom=cmPresetRange(cm.preset);syncRange();loadReport();
  }));
  $('cmCustomToggle')?.addEventListener('click',()=>{
    $('cmCustomRange')?.classList.toggle('show');
    $('cmCustomToggle')?.classList.toggle('active',$('cmCustomRange')?.classList.contains('show'));
  });
  $('cmCustomApply')?.addEventListener('click',()=>{
    const from=$('cmCustomFrom').value,to=$('cmCustomTo').value;
    if(!from||!to||from>to){showError('请选择有效的开始和结束日期。');return}
    cm.hours=0;cm.custom={from,to};cm.preset='custom';syncRange();loadReport();
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
  $('cmRefresh')?.addEventListener('click',()=>loadReport());
  document.querySelectorAll('[data-cm-sort]').forEach(btn=>btn.addEventListener('click',()=>{
    cm.sort=btn.dataset.cmSort;
    document.querySelectorAll('[data-cm-sort]').forEach(x=>x.classList.toggle('active',x===btn));
    render();
  }));
  $('cmBody')?.addEventListener('click',event=>{
    const ledger=event.target.closest('[data-cm-cost-ledger]');
    if(ledger){event.stopPropagation();toggleCostLedger(ledger.dataset.cmCostLedger);return}
    const binding=event.target.closest('[data-cm-cost-binding]');
    if(binding){event.stopPropagation();saveCostBinding(binding.dataset.cmCostBinding,+binding.dataset.sourceIndex);return}
    const decision=event.target.closest('[data-cm-proposal-action]');
    if(decision){event.stopPropagation();decidePricingProposal(decision.dataset.cmLedgerDomain,+decision.dataset.proposalIndex,decision.dataset.cmProposalAction);return}
    const cancelActivation=event.target.closest('[data-cm-activation-cancel]');
    if(cancelActivation){event.stopPropagation();cancelPricingActivation(cancelActivation.dataset.cmLedgerDomain,+cancelActivation.dataset.proposalIndex);return}
    const finance=event.target.closest('[data-cm-finance]');
    if(finance){event.stopPropagation();openFinance(finance.dataset.cmFinance);return}
    const upstream=event.target.closest('[data-cm-upstream]');
    if(upstream){event.stopPropagation();openUpstream(upstream.dataset.cmUpstream);return}
    const domain=event.target.closest('[data-cm-domain-toggle]');
    if(domain){
      const key=domain.dataset.cmDomainToggle;toggleSet(cm.expandedDomains,key);render();
      if(cm.expandedDomains.has(key))loadEconomicsDomain(key);
      return
    }
    const vendor=event.target.closest('[data-cm-vendor-toggle]');
    if(vendor){toggleSet(cm.expandedVendors,vendor.dataset.cmVendorToggle);render();return}
    const group=event.target.closest('[data-cm-group-toggle]');
    if(group){toggleSet(cm.collapsedGroups,group.dataset.cmGroupToggle);render()}
  });
  $('cmBody')?.addEventListener('change',event=>{
    const mode=event.target.closest('[data-cost-mode]');if(!mode)return;
    const channel=mode.closest('.cm-cost-source')?.querySelector('[data-cost-channel]');if(!channel)return;
    channel.disabled=mode.value!=='allocated';
    if(channel.disabled)channel.value='0';
  });
  $('cmBody')?.addEventListener('keydown',event=>{
    if(event.key!=='Enter'&&event.key!==' ')return;
    if(event.target.closest('[data-cm-finance],[data-cm-upstream]'))return;
    const target=event.target.closest('[data-cm-domain-toggle],[data-cm-vendor-toggle],[data-cm-group-toggle]');
    if(target){event.preventDefault();target.click()}
  });
  $('cmFinanceClose')?.addEventListener('click',closeFinance);
  $('cmFinanceCancel')?.addEventListener('click',closeFinance);
  $('cmFinanceMask')?.addEventListener('click',closeFinance);
  $('cmFinanceSave')?.addEventListener('click',saveFinance);
  $('cmFinanceSyncGroups')?.addEventListener('click',syncWebsiteGroups);
  $('cmSiteFinanceOpen')?.addEventListener('click',openSiteFinance);
  ['cmFinanceFX','cmFinanceSitePaid','cmFinanceSiteCredit','cmFinanceUpPaid','cmFinanceUpCredit'].forEach(id=>$(id)?.addEventListener('input',refreshFinancePreview));
  $('cmFinanceGroupRows')?.addEventListener('input',event=>{if(event.target.matches('[data-cm-finance-input]'))refreshFinancePreview()});
  $('cmUpstreamClose')?.addEventListener('click',closeUpstream);
  $('cmUpstreamCancel')?.addEventListener('click',closeUpstream);
  $('cmUpstreamMask')?.addEventListener('click',closeUpstream);
  $('cmUpstreamProvider')?.addEventListener('change',syncUpstreamFields);
  $('cmUpstreamAuthMode')?.addEventListener('change',syncUpstreamFields);
  $('cmUpstreamAddAPIKey')?.addEventListener('click',()=>addAICodeWithKeyInput());
  $('cmUpstreamAPIKeyList')?.addEventListener('click',event=>{
    const button=event.target.closest('[data-remove-api-key]');if(!button)return;
    const row=button.closest('.cm-upstream-key-row');if(row?.dataset.slotId)cm.removedAICodeWithKeyIDs.add(row.dataset.slotId);
    row?.remove();refreshAICodeWithKeyRows();
  });
  $('cmUpstreamSave')?.addEventListener('click',saveUpstream);
  $('cmUpstreamSync')?.addEventListener('click',syncUpstreamNow);
  $('cmUpstreamUsageSync')?.addEventListener('click',syncUpstreamUsageNow);
  document.addEventListener('keydown',event=>{if(event.key==='Escape'){if(cm.upstreamDomain)closeUpstream();else if(cm.financeMode)closeFinance()}});
  window.addEventListener('monitor:navigate',event=>{if(event.detail?.tab==='channels'&&applyNavigationContext())loadReport()});
}

function toggleSet(set,key){if(set.has(key))set.delete(key);else set.add(key)}
function syncRange(){
  document.querySelectorAll('[data-cm-hours]').forEach(btn=>btn.classList.toggle('active',!cm.custom&&+btn.dataset.cmHours===cm.hours));
  document.querySelectorAll('[data-cm-days]').forEach(btn=>btn.classList.toggle('active',!cm.custom&&!cm.hours&&+btn.dataset.cmDays===cm.days));
  document.querySelectorAll('[data-cm-preset]').forEach(btn=>btn.classList.toggle('active',btn.dataset.cmPreset===cm.preset));
  $('cmCustomToggle')?.classList.toggle('active',cm.preset==='custom');
  $('cmCustomRange')?.classList.toggle('show',cm.preset==='custom');
}
function queryString(){
  const q=new URLSearchParams();
  if(cm.custom){q.set('from',cm.custom.from);q.set('to',cm.custom.to)}
  else if(cm.hours){q.set('hours',String(cm.hours))}
  else q.set('days',String(cm.days));
  return q.toString();
}
function loading(){
  $('cmSummary')?.setAttribute('aria-busy','true');
  if($('cmBody'))$('cmBody').innerHTML='<div class="cm-loading"><i></i><span>正在读取本地渠道用量汇总…</span></div>';
}
function showError(message){
  if($('cmSummary')){$('cmSummary').innerHTML='';$('cmSummary').removeAttribute('aria-busy')}
  if($('cmBody'))$('cmBody').innerHTML=`<div class="cm-empty"><b>渠道数据暂时无法读取</b><p>${esc(message||'请稍后重试。')}</p></div>`;
}
async function loadReport({quiet=false}={}){
  if(quiet&&cm.loading)return;
  if(cm.abort)cm.abort.abort();
  cm.abort=new AbortController();const controller=cm.abort,signal=controller.signal,seq=++cm.economicsSeq;cm.loading=true;
  if(!quiet){cm.economics=null;cm.economicsError='';cm.economicsHourly.clear();cm.economicsHourlySeq.clear();cm.costLedger.clear();cm.costLedgerSeq.clear();loading()}
  try{
    const res=await fetch('/channels/report?'+queryString(),{cache:'no-store',headers:{Accept:'application/json'},signal});
    if(res.status===401){location.href='/login';return}
    const data=await res.json();
    if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    if(data.enabled===false){showError('渠道用量依赖稳定性本地小时汇总，当前功能未启用。');return}
    cm.report=data;cm.loaded=true;cm.loadedAt=Date.now();populateFilters();render();
    if(!quiet)for(const key of cm.costLedgerOpen)loadCostLedger(key);
    // 精确成本是独立的影子读模型。先交付原有渠道页，再加载经济账；
    // 新接口关闭、超时或迁移中都不得破坏原有功能。
    try{
      const economicsRes=await fetch('/channels/economics?'+queryString(),{cache:'no-store',headers:{Accept:'application/json'},signal});
      if(economicsRes.status===401){location.href='/login';return}
      const economicsData=await economicsRes.json();
      if(!economicsRes.ok)throw new Error(economicsData.error||`HTTP ${economicsRes.status}`);
      if(seq!==cm.economicsSeq)return;
      cm.economics=economicsData?.enabled?economicsData:null;cm.economicsError='';render();
      for(const key of cm.expandedDomains)loadEconomicsDomain(key);
    }catch(error){
      if(error.name==='AbortError'||seq!==cm.economicsSeq)return;
      cm.economics=null;cm.economicsError=error.message||'精确成本读模型暂不可用';render();
    }
  }catch(error){if(error.name!=='AbortError'&&(!quiet||!cm.report))showError(error.message)}
  finally{if(cm.abort===controller)cm.abort=null;if(seq===cm.economicsSeq)cm.loading=false}
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
const pct=n=>(+n*100).toFixed(2)+'%';
function multiplierGap(n){
  const value=Math.abs(+n)<1e-9?0:+n;
  const number=Math.abs(value).toFixed(4).replace(/0+$/,'').replace(/\.$/,'')||'0';
  return `${value>0?'+':value<0?'-':''}${number}x`;
}
function formatMultiplier(value){
  const n=Number(value);
  if(!Number.isFinite(n)||n<=0)return'—';
  return `${n.toFixed(2).replace(/0+$/,'').replace(/\.$/,'')}×`;
}
function stabilityText(value){
  if(value===null||value===undefined||value==='')return'—';
  const n=Number(value);
  return Number.isFinite(n)?`${n.toFixed(2)}%`:'—';
}
function channelGroupFor(ch,name){
  return (ch.groups||[]).find(group=>group.name===name)||{name,usage:zero(),finance:{}};
}
function channelGroupRows(domain,group){
  const finance=group.finance||{};
  return group.channels.map(({channel,groupData})=>{
    const usage=groupData.usage||zero();
    const f=groupData.finance||finance;
    const conflict=!!f.upstream_conflict;
    const gap=!conflict&&f.site_configured&&f.upstream_configured?multiplierGap(f.multiplier_gap):'—';
    return `<div class="cm-group-channel-row">
      <div class="cm-channel-name"><b>#${channel.id} ${esc(channel.name)}</b>${economicsChannelLine(domain,channel.id)}</div>
      <div class="cm-group-rate"><b>${conflict?'配置冲突':formatMultiplier(f.upstream_effective_multiplier)}</b></div>
      <div class="cm-group-gap"><b>${gap}</b></div>
      <div class="cm-group-models"><b>${nfmt(channel.model_count)} 个模型</b></div>
      <div>${statusLabel(channel)}</div>
      <div class="cm-group-stability"><b>${stabilityText(channel.stability)}</b></div>
      <span class="cm-number">${nfmt(usage.requests)}</span>
      <span class="cm-number" title="${nfmt(usage.tokens)}">${compact(usage.tokens)}</span>
      <span class="cm-number">${usd(usage.cost_usd)}</span>
    </div>`;
  }).join('')||'<div class="cm-no-groups">该服务分组暂无渠道用量</div>';
}
function vendorGroups(vendor){
  const groups=new Map();
  for(const channel of vendor.channels||[]){
    const names=new Set([...(channel.groups||[]).map(group=>group.name),...(channel.configured_groups||[])]);
    for(const name of names){
      if(!name)continue;
      let group=groups.get(name);
      if(!group){group={name,usage:zero(),channels:[],finance:{}};groups.set(name,group)}
      const groupData=channelGroupFor(channel,name);
      add(group.usage,groupData.usage);
      if(group.channels.every(item=>item.channel.id!==channel.id))group.channels.push({channel,groupData});
      if(!group.finance?.complete&&groupData.finance)group.finance=groupData.finance;
    }
  }
  return [...groups.values()].sort((a,b)=>metric(b.usage)-metric(a.usage)||b.usage.requests-a.usage.requests||a.name.localeCompare(b.name,'zh-CN'));
}
function groupSection(domain,vendor,group,index,domainTotal){
  const key=`${domain.key}:group:${vendor.name}:${group.name}`,open=!cm.collapsedGroups.has(key);
  const f=group.finance||{},active=group.channels.filter(item=>metric(item.groupData.usage)>0).length;
  const models=group.channels.reduce((total,item)=>total+(+item.channel.model_count||0),0);
  const share=metric(domainTotal)>0?metric(group.usage)/metric(domainTotal)*100:0;
  return `<section class="cm-group-section${open?' open':''}">
    <header class="cm-group-head" role="button" tabindex="0" aria-expanded="${open}" data-cm-group-toggle="${esc(key)}">
      <div class="cm-group-title"><b>${esc(group.name)}</b><span>网站分组倍率 ${formatMultiplier(f.site_multiplier)}</span><small>${group.channels.length} 个候选渠道 · ${nfmt(models)} 个配置模型 · 本期 ${nfmt(active)} 个渠道有请求</small></div>
      <div class="cm-group-metrics"><span><small>请求数</small><b>${nfmt(group.usage.requests)}</b></span><span><small>Tokens</small><b>${compact(group.usage.tokens)}</b></span><span><small>用户侧消费</small><b>${usd(group.usage.cost_usd)}</b></span><span><small>域名消费占比</small><b>${share.toFixed(1)}%</b></span></div>
      <i class="cm-group-chevron">${open?'−':'+'}</i>
    </header>
    ${open?`<div class="cm-group-body"><div class="cm-group-channel-head"><span>渠道 ID / 渠道名</span><span>上游折算倍率</span><span>倍率差</span><span>关联模型</span><span>状态</span><span>稳定性</span><span>请求数</span><span>Tokens</span><span>用户侧消费</span></div>${channelGroupRows(domain,group)}</div>`:''}
  </section>`;
}
function vendorSection(domain,vendor){
  const key=domain.key+':vendor:'+vendor.name,open=cm.expandedVendors.has(key),groups=vendorGroups(vendor);
  return `<section class="cm-vendor-section${open?' open':''}"><header class="cm-vendor-head" role="button" tabindex="0" aria-expanded="${open}" data-cm-vendor-toggle="${esc(key)}"><div><span class="cm-vendor-dot"></span><b>${esc(vendor.name)}</b><small>${vendor.channels.length} 个实际渠道 · ${groups.length} 个服务分组</small></div><div><span>${nfmt(vendor.usage.requests)} 渠道请求</span><span>${compact(vendor.usage.tokens)} Tokens</span><strong>${usd(vendor.usage.cost_usd)}</strong><i class="cm-vendor-chevron">${open?'−':'+'}</i></div></header>${open?`<div class="cm-vendor-body">${groups.map((group,index)=>groupSection(domain,vendor,group,index,domain.usage)).join('')}</div>`:''}</section>`;
}
function upstreamSummary(upstream){
  if(!upstream?.configured)return '<span class="pending">余额未配置</span>';
  const balance=upstream.balance_usd==null?'余额未知':`余额 ${usd(upstream.balance_usd)}`;
  const assessment=upstream.assessment||{},runway=assessment.estimated_runway_days;
  const estimate=assessment.available
    ? assessment.status==='idle'?` · ${esc(assessment.reason||'近期无显著消耗')}`:` · 预计可用 ${Number(runway).toFixed(1)} 天`
    : assessment.reason?` · ${esc(assessment.reason)}`:'';
  const usageStatus=upstream.usage_tail_phase||upstream.usage_effective_status||upstream.usage_status;
  const usage=upstream.usage_sync_enabled
    ? usageStatus==='global_off'?' · 消费同步灰度关闭'
      :usageStatus==='stale'?' · 消费数据已陈旧'
      :usageStatus==='ok'?` · 消费账单 ${esc(shortDateTime(upstream.usage_last_success_at))} 同步`
	  :usageStatus==='queued'?' · 消费同步等待首次调度'
      :upstream.usage_status==='unsupported'?' · 消费接口待适配'
      :upstream.usage_status==='error'||upstream.usage_status==='reconnect'?' · 消费同步待处理'
      :' · 消费等待同步'
    :'';
	const backfill=upstream.usage_sync_enabled&&upstream.usage_worker_enabled&&!upstream.usage_backfill_done
		?upstream.usage_history_phase==='queued'?' · 历史补全等待首次调度':upstream.usage_history_phase==='retry'?' · 历史补全退避重试中':upstream.usage_history_phase==='blocked'?' · 历史补全受阻':upstream.usage_backfill_progress?' · 历史补全断点续传中':' · 历史补全中'
		:'';
  if(!upstream.enabled)return `<span class="neutral">${esc(upstream.provider_name||upstream.provider)} · 已停用 · ${balance}${usage}</span>`;
  if(upstream.status==='reconnect')return `<span class="bad">${esc(upstream.provider_name||upstream.provider)} · 需要重新连接 · ${balance}</span>`;
  if(upstream.status==='error')return `<span class="warn">${esc(upstream.provider_name||upstream.provider)} · 同步异常 · ${balance}</span>`;
  if(upstream.status==='ok'){
    const cls=assessment.status==='critical'?'bad':assessment.status==='warning'?'warn':assessment.status==='healthy'?'ready':'neutral';
	return `<span class="${cls}" title="近 ${Number(assessment.lookback_days||0)} 个完整自然日预估日均上游成本 ${usd(assessment.average_daily_cost_usd||0)}；小时完整率 ${Number(assessment.coverage_pct||0).toFixed(1)}%">${esc(upstream.provider_name||upstream.provider)} · ${balance}${estimate}${usage}${backfill}</span>`;
  }
  return `<span class="pending">${esc(upstream.provider_name||upstream.provider)} · 等待同步</span>`;
}
function economicsDomain(domain){
  const name=String(domain?.domain||domain||'').toLowerCase();
  return (cm.economics?.domains||[]).find(item=>String(item.domain||'').toLowerCase()===name)||null;
}
function economicsChannel(domain,channelID){
  return (economicsDomain(domain)?.channels||[]).find(item=>+item.channel_id===+channelID)||null;
}
function economicsMoneyLabel(value,known=true){
  return known&&value?.display?value.display:'不可判定';
}
function economicsReason(reason){
  return ({account_epoch_overlap:'账户代际冲突',refund_unallocated:'存在未归属退款',publication_missing:'小时发布缺失',coverage_incomplete:'证据未闭合'})[reason]||'证据未闭合';
}
function economicsCoverageLabel(coverage){
  if(!coverage)return'等待生成';
  if(coverage.complete)return`${nfmt(coverage.verified_hours)}/${nfmt(coverage.expected_hours)} 小时已核验`;
  return`${nfmt(coverage.verified_hours)}/${nfmt(coverage.expected_hours)} 小时已核验 · 缺失 ${nfmt(coverage.missing_hours)} · 未知 ${nfmt(coverage.unknown_hours)}`;
}
function economicsStrip(domain){
  if(!cm.economics)return cm.economicsError?`<section class="cm-economics-note warn">精确成本账本本次未读取：${esc(cm.economicsError)}。原有渠道用量不受影响。</section>`:'';
  const item=economicsDomain(domain);
  if(!item)return'<section class="cm-economics-note">该主域名尚未进入精确成本白名单。</section>';
  const totals=item.totals||{},coverage=item.coverage||{};
  const state=totals.profit_known?'ready':'warn';
  return `<section class="cm-economics-panel ${state}">
    <header><div><b>精确成本与利润</b><small>服务端权威小时账本 · 不从页面粗算</small></div><span class="${state}">${esc(economicsCoverageLabel(coverage))}</span></header>
    <div class="cm-economics-metrics">
      <span><small>本地净收入</small><b>${economicsMoneyLabel(totals.revenue,totals.revenue_known)}</b></span>
      <span><small>上游账面成本</small><b>${economicsMoneyLabel(totals.upstream_cost,totals.upstream_cost_known)}</b></span>
      <span><small>上游修正成本</small><b>${economicsMoneyLabel(totals.corrected_cost,totals.corrected_cost_known)}</b></span>
      <span><small>毛利润</small><b>${economicsMoneyLabel(totals.profit,totals.profit_known)}</b></span>
      <span><small>毛利率</small><b>${totals.profit_known?esc(totals.margin_display||'不可判定'):'不可判定'}</b></span>
      <span><small>口径</small><b>${totals.profit_known?'已闭合':esc(economicsReason(totals.unknown_reason))}</b></span>
    </div>${economicsChart(domain)}
  </section>`;
}

const proposalValue=(row,snake,pascal)=>row?.[snake]??row?.[pascal];
function reportDomain(key){
  return (cm.report?.domains||[]).find(item=>String(item.key||item.domain)===String(key))||null;
}
function domainChannels(domain){
  const byID=new Map();
  for(const vendor of domain?.vendors||[])for(const channel of vendor.channels||[])if(channel.current)byID.set(+channel.id,channel);
  return [...byID.values()].sort((a,b)=>+a.id-+b.id);
}
function costLedgerStatus(status){
  return ({pending:'待审批',scheduled:'待生效',applied:'已生效',rejected:'已驳回',rollback_scheduled:'回滚待生效',rolled_back:'已回滚',conflict:'冲突',cancelled:'已取消'})[status]||status||'未知';
}
function costClosureAllowed(domain){
  const capability=cm.report?.cost_closure;
  return !!cm.report?.finance?.can_edit&&!!capability?.enabled&&(capability.domains||[]).some(value=>String(value).toLowerCase()===String(domain?.domain||'').toLowerCase());
}
function costClosureRecoveryAllowed(domain){
  const capability=cm.report?.cost_closure;
  return !!cm.report?.finance?.can_edit&&(capability?.recovery_domains||[]).some(value=>String(value).toLowerCase()===String(domain?.domain||'').toLowerCase());
}
function costClosureAccessible(domain){return costClosureAllowed(domain)||costClosureRecoveryAllowed(domain)}
function costLedgerPanel(domain){
  if(!costClosureAccessible(domain))return'';
  const key=String(domain.key||domain.domain),open=cm.costLedgerOpen.has(key),data=cm.costLedger.get(key);
  const pending=(data?.proposals||[]).filter(row=>['pending','scheduled','rollback_scheduled'].includes(String(proposalValue(row,'status','Status')))).length;
  const recoveryOnly=!costClosureAllowed(domain);
  if(!open)return `<section class="cm-cost-ledger-closed"><button type="button" data-cm-cost-ledger="${esc(key)}">倍率证据与变更台账${pending?` · ${nfmt(pending)} 待处理`:''}</button><span>${recoveryOnly?'安全闸门已关闭 · 仅可查看和取消待生效任务':'来源映射、历史变更、审批与回滚'}</span></section>`;
  let body='<div class="cm-cost-ledger-loading">正在读取本地计价证据…</div>';
  if(data?.error)body=`<div class="cm-cost-ledger-error">${esc(data.error)}</div>`;
  else if(data&&!data.loading)body=(recoveryOnly?'':costSourceRows(domain,data)+financeVersionRows(data))+pricingProposalRows(domain,data);
  return `<section class="cm-cost-ledger"><header><div><b>倍率证据与变更台账</b><small>${recoveryOnly?'安全闸门已关闭；待生效任务已停止执行，仅保留查看和取消入口':'自动发现只生成候选；审批后在下一整点原子生效，可取消、驳回或回滚'}</small></div><button type="button" data-cm-cost-ledger="${esc(key)}">收起</button></header>${body}</section>`;
}
function costSourceRows(domain,data){
  const sources=data.sources||[],channels=domainChannels(domain);
  const rows=sources.map((source,index)=>{
    const binding=source.current_binding||null,mode=String(proposalValue(binding,'allocation_mode','AllocationMode')||'unallocated');
    const channelID=+(proposalValue(binding,'local_channel_id','LocalChannelID')||0);
    const channelOptions=channels.map(channel=>`<option value="${+channel.id}" ${channelID===+channel.id?'selected':''}>#${+channel.id} ${esc(channel.name)}</option>`).join('');
    const groups=(source.source_groups||[]).slice(0,3),models=(source.upstream_models||[]).slice(0,3),identity=String(source.source_ref||'').slice(-8);
    return `<article class="cm-cost-source"><div><b>${esc(groups.join(' / ')||'未命名分组')} · ${esc(models.join(' / ')||'未知模型')}</b><small>来源 …${esc(identity)} · ${nfmt(source.dimension_count)} 个计价维度 · ${nfmt(source.requests)} 请求 · ${shortDateTime(source.first_hour)} 至 ${shortDateTime(source.last_hour)}</small></div><label><span>归属方式</span><select data-cost-mode>${['allocated','shared','unallocated'].map(value=>`<option value="${value}" ${mode===value?'selected':''}>${value==='allocated'?'指定本地渠道':value==='shared'?'共享/无法拆分':'暂不归属'}</option>`).join('')}</select></label><label><span>本地渠道</span><select data-cost-channel ${mode!=='allocated'?'disabled':''}><option value="0">请选择</option>${channelOptions}</select></label><button type="button" data-cm-cost-binding="${esc(String(domain.key||domain.domain))}" data-source-index="${index}">保存映射</button></article>`;
  }).join('');
  return `<div class="cm-cost-ledger-block"><h4>上游计价来源映射 <small>${nfmt(sources.length)} 个</small></h4>${data.sourcesTruncated?'<p class="cm-cost-limit">来源超过安全展示上限，当前仅显示最近 500 个；未展示来源不会被自动归属。</p>':''}${rows||'<p class="cm-cost-empty">当前账户代际还没有已核验的上游计价来源。</p>'}</div>`;
}
function pricingProposalRows(domain,data){
  const proposals=data.proposals||[];
  const canMutate=costClosureAllowed(domain);
  const rows=proposals.map((row,index)=>{
    const status=String(proposalValue(row,'status','Status')||''),activation=row.activation||null;
    const proposalKey=String(proposalValue(row,'proposal_key','ProposalKey')||'');
    const evidenceFrom=+proposalValue(row,'evidence_from_hour','EvidenceFromHour')||0,evidenceTo=+proposalValue(row,'evidence_to_hour','EvidenceToHour')||0;
    const oldValue=proposalValue(row,'old_value','OldValue'),newValue=proposalValue(row,'new_value','NewValue');
    let actions='';
    if(status==='pending'&&canMutate)actions=`<button type="button" data-cm-proposal-action="approve" data-cm-ledger-domain="${esc(String(domain.key||domain.domain))}" data-proposal-index="${index}">审批并排期</button><button class="danger" type="button" data-cm-proposal-action="reject" data-cm-ledger-domain="${esc(String(domain.key||domain.domain))}" data-proposal-index="${index}">驳回</button>`;
    else if((status==='scheduled'||status==='rollback_scheduled')&&activation)actions=`<button class="danger" type="button" data-cm-activation-cancel data-cm-ledger-domain="${esc(String(domain.key||domain.domain))}" data-proposal-index="${index}">取消待生效</button>`;
    else if(status==='applied'&&canMutate&&proposalValue(row,'rollback_allowed','RollbackAllowed'))actions=`<button class="danger" type="button" data-cm-proposal-action="rollback" data-cm-ledger-domain="${esc(String(domain.key||domain.domain))}" data-proposal-index="${index}">排期回滚</button>`;
    const activationText=activation?` · ${costLedgerStatus(activation.status)} ${shortDateTime(activation.effective_at)}`:'',impact=proposalValue(row,'impact','Impact'),impactRows=proposalValue(impact,'rows','Rows')||[];
    const impactTotal=+(proposalValue(row,'impact_total','ImpactTotal')||impactRows.length),impactTruncated=!!proposalValue(row,'impact_truncated','ImpactTruncated');
    const rollbackAllowed=!!proposalValue(row,'rollback_allowed','RollbackAllowed'),impactFallback=status==='applied'&&!rollbackAllowed?'历史版本，不可直接回滚':'尚未生成倍率补丁';
    const impactHTML=impactRows.length?`<div class="cm-proposal-impact"><b>实际影响 ${nfmt(impactTotal)} 个服务分组${impactTruncated?' · 当前预览前 20 个，操作时读取完整清单':''}</b>${impactRows.map(item=>{const before=proposalValue(item,'before','Before')||{},after=proposalValue(item,'after','After')||{};return `<span>${esc(proposalValue(before,'group','Group')||'未知分组')}：${esc(proposalValue(before,'multiplier','Multiplier'))}× × ${esc(proposalValue(before,'discount_factor','DiscountFactor'))} → ${esc(proposalValue(after,'multiplier','Multiplier'))}× × ${esc(proposalValue(after,'discount_factor','DiscountFactor'))}</span>`}).join('')}</div>`:`<p class="cm-impact-error">影响范围不可确认：${esc(proposalValue(row,'impact_error','ImpactError')||impactFallback)}</p>`;
    return `<article class="cm-pricing-proposal"><div class="cm-proposal-main"><b>#${nfmt(proposalValue(row,'local_channel_id','LocalChannelID'))} ${esc(proposalValue(row,'source_group','SourceGroup')||'未知上游分组')}</b><span class="status ${esc(status)}">${esc(costLedgerStatus(status))}</span><strong>${esc(oldValue)} → ${esc(newValue)}</strong></div><small>${esc(proposalValue(row,'value_kind','ValueKind')||'倍率')} · 证据 ${shortDateTime(evidenceFrom)} 至 ${shortDateTime(evidenceTo)} · ${nfmt(proposalValue(row,'verified_hours','VerifiedHours'))} 小时 / ${nfmt(proposalValue(row,'evidence_requests','EvidenceRequests'))} 请求${esc(activationText)}</small>${impactHTML}${activation?.last_error?`<p>${esc(activation.last_error)}</p>`:''}<div class="cm-proposal-actions" data-proposal-key="${esc(proposalKey)}">${actions}</div></article>`;
  }).join('');
  return `<div class="cm-cost-ledger-block"><h4>倍率变化与审批历史 <small>${nfmt(proposals.length)} 条</small></h4>${data.actionableTruncated?'<p class="cm-cost-limit">待处理任务超过 500 条，当前优先显示最早的 500 条；处理后后续任务会自动进入列表。</p>':data.proposalsTruncated?'<p class="cm-cost-limit">历史记录超过安全展示上限；所有待处理任务和当前可回滚版本仍优先保留。</p>':''}${rows||'<p class="cm-cost-empty">尚未发现满足连续证据门槛的倍率变化。</p>'}</div>`;
}
function financeVersionRows(data){
  const labels={fx_benchmark:'折扣基准',site_recharge_paid:'我方充值支付',site_recharge_credit:'我方充值到账',upstream_recharge_paid:'上游充值支付',upstream_recharge_credit:'上游充值到账',site_multiplier:'网站分组倍率',upstream_multiplier:'上游基础倍率',upstream_discount_factor:'上游折扣系数',upstream_group_name:'上游分组名',record:'配置记录'};
  const rows=(data.versions||[]).map(version=>{
    const changes=(version.changes||[]).map(change=>`<li><span>${esc(change.scope)} · ${esc(change.key||'全局')} · ${esc(labels[change.field]||change.field)}</span><b>${esc(change.old_value||'—')} → ${esc(change.new_value||'—')}</b></li>`).join('');
    return `<article class="cm-finance-version"><header><b>计价版本 v${nfmt(version.version)}</b><span>生效 ${shortDateTime(version.effective_at)} · ${esc(version.updated_by||'system')}</span></header>${changes?`<ul>${changes}</ul>`:'<small>初始完整快照或当前版本没有字段变化。</small>'}${version.truncated?'<p>变更项超过 200 条，页面仅展示前 200 条。</p>':''}<code title="完整快照 SHA-256">${esc(String(version.snapshot_hash||'').slice(0,16))}…</code></article>`;
  }).join('');
  return `<div class="cm-cost-ledger-block"><h4>实际生效版本审计 <small>${nfmt((data.versions||[]).length)} 版</small></h4>${data.versionsTruncated?'<p class="cm-cost-limit">版本历史超过安全展示上限，当前仅显示最近 100 版。</p>':''}${rows||'<p class="cm-cost-empty">尚无已生效计价版本。</p>'}</div>`;
}
function toggleCostLedger(key){
  if(cm.costLedgerOpen.has(key)){cm.costLedgerOpen.delete(key);render();return}
  cm.costLedgerOpen.add(key);render();loadCostLedger(key);
}
async function loadCostLedger(key){
  const domain=reportDomain(key);if(!domain||!costClosureAccessible(domain))return;
  const generation=cm.economicsSeq,signal=cm.abort?.signal;
  const seq=(cm.costLedgerSeq.get(key)||0)+1;cm.costLedgerSeq.set(key,seq);cm.costLedger.set(key,{loading:true,sources:[],proposals:[]});render();
  try{
    const q='domain='+encodeURIComponent(domain.domain),recoveryOnly=!costClosureAllowed(domain);
    const [sourceRes,proposalRes]=await Promise.all([recoveryOnly?Promise.resolve(null):fetch('/channels/cost/sources?'+q,{cache:'no-store',headers:{Accept:'application/json'},signal}),fetch('/channels/cost/proposals?'+q,{cache:'no-store',headers:{Accept:'application/json'},signal})]);
    if(sourceRes?.status===401||proposalRes.status===401){location.href='/login';return}
    const sourceData=sourceRes?await sourceRes.json():{account_epoch:'',sources:[],truncated:false},proposalData=await proposalRes.json();
    if(sourceRes&&!sourceRes.ok)throw new Error(sourceData.error||`来源 HTTP ${sourceRes.status}`);
    if(!proposalRes.ok)throw new Error(proposalData.error||`台账 HTTP ${proposalRes.status}`);
    if(generation!==cm.economicsSeq||cm.costLedgerSeq.get(key)!==seq)return;
	for(const row of proposalData.proposals||[]){const proposalKey=String(proposalValue(row,'proposal_key','ProposalKey')||''),status=String(proposalValue(row,'status','Status')||'');if(status!=='pending'){cm.pricingOps.delete(proposalKey+':approve');cm.pricingOps.delete(proposalKey+':reject')}if(status!=='applied')cm.pricingOps.delete(proposalKey+':rollback')}
    cm.costLedger.set(key,{loading:false,accountEpoch:sourceData.account_epoch,sources:sourceData.sources||[],sourcesTruncated:!!sourceData.truncated,proposals:proposalData.proposals||[],proposalsTruncated:!!proposalData.proposals_truncated,actionableTruncated:!!proposalData.actionable_truncated,versions:proposalData.versions||[],versionsTruncated:!!proposalData.versions_truncated});render();
  }catch(error){if(error.name==='AbortError'||generation!==cm.economicsSeq||cm.costLedgerSeq.get(key)!==seq)return;cm.costLedger.set(key,{loading:false,error:error.message||'计价台账读取失败',sources:[],proposals:[]});render()}
}
async function saveCostBinding(key,index){
  const domain=reportDomain(key),data=cm.costLedger.get(key),source=data?.sources?.[index];if(!domain||!source)return;
  const button=document.querySelector(`[data-cm-cost-binding="${CSS.escape(key)}"][data-source-index="${index}"]`),row=button?.closest('.cm-cost-source');
  const mode=row?.querySelector('[data-cost-mode]')?.value||'unallocated',selectedChannelID=+(row?.querySelector('[data-cost-channel]')?.value||0),channelID=mode==='allocated'?selectedChannelID:0;
  if(mode==='allocated'&&!channelID){alert('指定本地渠道时必须选择一个渠道。');return}
  const current=source.current_binding||null,expected=+(proposalValue(current,'valid_from','ValidFrom')||0);
  const currentMode=String(proposalValue(current,'allocation_mode','AllocationMode')||'未配置'),currentChannel=+(proposalValue(current,'local_channel_id','LocalChannelID')||0),effective=Math.floor(Date.now()/3600000)*3600+3600;
  const reason=window.prompt('请输入来源映射的审计原因：','已核对上游令牌与本地渠道归属');if(!reason?.trim())return;
  if(!window.confirm(`来源 …${String(source.source_ref||'').slice(-8)}\n当前：${currentMode}${currentChannel?' #'+currentChannel:''}\n新配置：${mode}${channelID?' #'+channelID:''}\n生效：${dateTime(effective)}\n\n确认保存？`))return;
  const body={domain:domain.domain,account_epoch:data.accountEpoch,source_ref:source.source_ref,source_ref_kind:source.source_ref_kind,hmac_key_id:source.hmac_key_id,local_channel_id:channelID,allocation_mode:mode,reason:reason.trim(),expected_current_valid_from:expected,expected_current_signature:source.current_binding_signature||''};
  button.disabled=true;
  try{const res=await fetch('/channels/cost/bindings',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify(body)}),result=await res.json();if(!res.ok)throw new Error(result.error||`HTTP ${res.status}`);await loadCostLedger(key)}catch(error){alert(error.message||'保存来源映射失败');await loadCostLedger(key)}finally{button.disabled=false}
}
function proposalAt(key,index){return cm.costLedger.get(key)?.proposals?.[index]||null}
function idempotencyKey(action){return `${action}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2,10)}`}
function setProposalBusy(key,index,busy){document.querySelectorAll(`[data-cm-ledger-domain="${CSS.escape(key)}"][data-proposal-index="${index}"],[data-cm-activation-cancel][data-cm-ledger-domain="${CSS.escape(key)}"][data-proposal-index="${index}"]`).forEach(button=>button.disabled=busy)}
async function decidePricingProposal(key,index,action){
  const row=proposalAt(key,index);if(!row)return;
  const status=String(proposalValue(row,'status','Status')||''),label=action==='approve'?'审批该倍率并在下一整点生效':action==='rollback'?'在下一整点回滚该倍率':'驳回该倍率候选';
	let impact=proposalValue(row,'impact','Impact'),impactRows=proposalValue(impact,'rows','Rows')||[];
	const proposalKey=String(proposalValue(row,'proposal_key','ProposalKey')||'');
	if(action!=='reject'){setProposalBusy(key,index,true);try{const res=await fetch('/channels/cost/proposals/'+encodeURIComponent(proposalKey)+'/impact',{cache:'no-store',headers:{Accept:'application/json'},signal:cm.abort?.signal}),result=await res.json();if(!res.ok)throw new Error(result.error||`HTTP ${res.status}`);if(String(result.status||'')!==status)throw new Error('倍率候选状态已变化，请刷新后重试');impact=result.impact;impactRows=proposalValue(impact,'rows','Rows')||[]}catch(error){if(error.name!=='AbortError')alert(error.message||'读取完整影响范围失败');return}finally{setProposalBusy(key,index,false)}}
	if(action!=='reject'&&!impactRows.length){alert('无法确认本次倍率变更的实际影响范围，已拒绝继续操作。');return}
	const impactText=impactRows.map(item=>{const before=proposalValue(item,'before','Before')||{},after=proposalValue(item,'after','After')||{};return `${proposalValue(before,'group','Group')||'未知分组'}：${proposalValue(before,'multiplier','Multiplier')}× × ${proposalValue(before,'discount_factor','DiscountFactor')} → ${proposalValue(after,'multiplier','Multiplier')}× × ${proposalValue(after,'discount_factor','DiscountFactor')}`}).join('\n');
  const reason=window.prompt(`${label}\n请输入审计原因：`,'已核对上游计价证据');if(!reason?.trim())return;
  if(!window.confirm(`${label}？此操作会写入不可变审计记录。${impactText?`\n\n实际影响 ${impactRows.length} 个服务分组：\n${impactText}`:''}`))return;
  const opKey=proposalKey+':'+action;
  if(!cm.pricingOps.has(opKey))cm.pricingOps.set(opKey,idempotencyKey(action));
  const body={action,expected_status:status,expected_base_version:+proposalValue(row,'base_version','BaseVersion')||0,expected_evidence_digest:String(proposalValue(row,'evidence_digest','EvidenceDigest')||''),idempotency_key:cm.pricingOps.get(opKey),reason:reason.trim()};
  if(action!=='reject')body.effective_from=Math.floor(Date.now()/3600000)*3600+3600;
  setProposalBusy(key,index,true);
  try{const res=await fetch('/channels/cost/proposals/'+encodeURIComponent(proposalKey)+'/decisions',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify(body)}),result=await res.json();if(!res.ok)throw new Error(result.error||`HTTP ${res.status}`);cm.pricingOps.delete(opKey);await loadCostLedger(key)}catch(error){alert(error.message||'倍率审批失败');await loadCostLedger(key)}
}
async function cancelPricingActivation(key,index){
  const row=proposalAt(key,index),activation=row?.activation;if(!activation)return;
  const reason=window.prompt('请输入取消待生效任务的原因：','重新核对上游计价证据');if(!reason?.trim())return;
  if(!window.confirm(`取消 ${shortDateTime(activation.effective_at)} 生效的倍率任务？此操作会保留审计记录。`))return;
  setProposalBusy(key,index,true);
  try{const res=await fetch('/channels/cost/activations/'+encodeURIComponent(activation.activation_id)+'/cancel',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify({reason:reason.trim()})}),result=await res.json();if(!res.ok)throw new Error(result.error||`HTTP ${res.status}`);await loadCostLedger(key)}catch(error){alert(error.message||'取消待生效任务失败');await loadCostLedger(key)}
}
function economicsChannelLine(domain,channelID){
  const item=economicsChannel(domain,channelID);if(!item)return'';
  const totals=item.totals||{},known=!!totals.profit_known;
  return `<small class="cm-channel-economics" title="该渠道在当前查询区间的全分组合计；同一渠道出现在多个服务分组时会重复展示，不参与页面汇总">全渠道：收入 ${economicsMoneyLabel(totals.revenue,totals.revenue_known)} · 修正成本 ${economicsMoneyLabel(totals.corrected_cost,totals.corrected_cost_known)} · 利润 ${economicsMoneyLabel(totals.profit,known)}</small>`;
}
function economicsChart(domain){
  const report=cm.economicsHourly.get(String(domain.key||domain.domain||domain));
  if(!report){return `<div class="cm-economics-chart loading"><span>小时收入/成本曲线将在展开后读取…</span></div>`}
  if(report.error)return `<div class="cm-economics-chart empty"><span>小时曲线暂时无法读取：${esc(report.error)}</span></div>`;
  const item=(report.domains||[])[0],rows=item?.hourly||[];
  if(!rows.length)return'<div class="cm-economics-chart empty"><span>当前区间暂无已发布小时账本。</span></div>';
  const width=760,height=170,left=42,right=12,top=18,bottom=30,from=+report.from,to=+report.to||from+3600;
  const values=[];for(const row of rows){values.push(Number(row.totals?.revenue?.micro_usd||0));if(row.totals?.corrected_cost_known)values.push(Number(row.totals?.corrected_cost?.micro_usd||0))}
  const maxValue=Math.max(1,...values.filter(Number.isFinite));
  const x=ts=>left+Math.max(0,Math.min(1,(+ts-from)/Math.max(3600,to-from)))*(width-left-right);
  const y=value=>top+(1-Math.max(0,Number(value)||0)/maxValue)*(height-top-bottom);
  const segments=(selector)=>{let current=[],result=[];for(const row of rows){const value=selector(row);if(value==null||!Number.isFinite(Number(value))){if(current.length)result.push(current);current=[];continue}if(current.length&&+row.hour_ts-current[current.length-1].ts>3700){result.push(current);current=[]}current.push({ts:+row.hour_ts+1800,value:Number(value)})}if(current.length)result.push(current);return result};
  const polylines=(items,cls)=>items.map(segment=>`<polyline class="${cls}" points="${segment.map(point=>`${x(point.ts).toFixed(1)},${y(point.value).toFixed(1)}`).join(' ')}"></polyline>`).join('');
  const revenue=segments(row=>row.totals?.revenue_known?row.totals?.revenue?.micro_usd:null),cost=segments(row=>row.totals?.corrected_cost_known?row.totals?.corrected_cost?.micro_usd:null);
  const start=shortDateTime(from),end=shortDateTime(to);
  return `<div class="cm-economics-chart"><header><b>1 小时收入 / 修正成本</b><span><i class="revenue"></i>本地净收入 <i class="cost"></i>上游修正成本</span></header><svg viewBox="0 0 ${width} ${height}" role="img" aria-label="小时收入与修正成本曲线"><line x1="${left}" y1="${height-bottom}" x2="${width-right}" y2="${height-bottom}"></line>${polylines(revenue,'revenue')}${polylines(cost,'cost')}<text x="${left}" y="${height-8}">${esc(start)}</text><text text-anchor="end" x="${width-right}" y="${height-8}">${esc(end)}</text></svg></div>`;
}
async function loadEconomicsDomain(key){
  if(!cm.economics?.enabled)return;
  const domain=(cm.report?.domains||[]).find(item=>item.key===key);if(!domain?.domain)return;
  const generation=cm.economicsSeq,seq=(cm.economicsHourlySeq.get(key)||0)+1;cm.economicsHourlySeq.set(key,seq);
  try{
    const res=await fetch('/channels/economics?'+queryString()+'&domain='+encodeURIComponent(domain.domain),{cache:'no-store',headers:{Accept:'application/json'},signal:cm.abort?.signal});
    if(res.status===401){location.href='/login';return}
    const data=await res.json();if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    if(generation!==cm.economicsSeq||cm.economicsHourlySeq.get(key)!==seq||!cm.expandedDomains.has(key))return;
    cm.economicsHourly.set(key,data);render();
  }catch(error){
    if(error.name==='AbortError'||generation!==cm.economicsSeq||cm.economicsHourlySeq.get(key)!==seq)return;
    cm.economicsHourly.set(key,{from:0,to:0,domains:[],error:error.message||'小时曲线读取失败'});render();
  }
}
function domainCard(domain,index,total,filtered){
  const channels=domain.vendors.flatMap(v=>v.channels),enabled=channels.filter(ch=>ch.current&&+ch.status===1).length;
  const open=cm.expandedDomains.has(domain.key),share=metric(total)>0?metric(domain.usage)/metric(total)*100:0;
  const rates=domain.rate_config||{},rateConfigured=+rates.configured_channels||0,rateEnabled=+rates.enabled_channels||0;
  const financeLabel=rateEnabled>0&&rates.complete?`上游渠道倍率已配置 · ${rateConfigured}/${rateEnabled}`:`上游渠道倍率待配置 · ${rateConfigured}/${rateEnabled}`;
  const upstreamUsage=domain.upstream_usage||{};
  // 上游日志只按账户（归并主域名）汇总；没有可靠的远端渠道 ID 映射时，
  // 绝不能伪装成某一条本地渠道的上游账单。
  const upstreamGranularity=upstreamUsage.granularity==='day'?'中国自然日账单':'小时日志';
  const upstreamCoverage=upstreamUsage.complete?`当前${upstreamGranularity}已覆盖`:`${upstreamUsage.granularity==='day'?'当前自然日':'查询范围'}为已同步部分`;
  const upstreamCoverageState=upstreamUsage.complete?'完整':'补全中';
  const upstreamBalance=domain.upstream?.balance_usd==null?'未知':usd(domain.upstream.balance_usd);
  const upstreamSpend=upstreamUsage.available?usd(upstreamUsage.cost_usd):'等待同步';
	const financeConfigured=!!domain.finance?.configured;
	const configuredRatio=financeConfigured&&Number(domain.finance.recharge_paid)>0?Number(domain.finance.recharge_credit)/Number(domain.finance.recharge_paid):0;
	const observedRatio=Number(upstreamUsage.recharge_ratio),ratio=Number.isFinite(observedRatio)&&observedRatio>0?observedRatio:configuredRatio;
	const ratioLabel=financeConfigured&&Number.isFinite(ratio)&&ratio>0?`到账/支付 ${ratio.toLocaleString(undefined,{maximumFractionDigits:4})}×`:'充值比例待配置';
	const adjustedSpend=upstreamUsage.adjusted_cost_available?usd(upstreamUsage.adjusted_cost_usd):(financeConfigured?'等待消费同步':'待配置');
  const upstreamSpendLabel=upstreamUsage.granularity==='day'?'自然日上游消费':'区间上游消费';
  const upstreamMetrics=domain.upstream?.configured||upstreamUsage.available?`<span class="cm-domain-upstream-spend" title="消费按上游账户（主域名）汇总，不是逐渠道上游账单。${upstreamCoverage}"><small>${upstreamSpendLabel}</small><b title="${upstreamSpend}">${upstreamSpend}</b><em class="cm-domain-metric-note ${upstreamUsage.available?(upstreamUsage.complete?'ready':'pending'):'neutral'}">${upstreamUsage.available?`${upstreamGranularity} · ${upstreamCoverageState}`:'同步未开启或尚无数据'}</em></span><span class="cm-domain-upstream-adjusted" title="上游修正消费 = 账面消费 × 充值支付 ÷ 充值到账"><small>上游修正消费</small><b title="${adjustedSpend}">${adjustedSpend}</b><em class="cm-domain-metric-note ${upstreamUsage.adjusted_cost_available?'ready':'pending'}">${esc(ratioLabel)}</em></span><span class="cm-domain-upstream-balance"><small>上游当前余额</small><b title="${upstreamBalance}">${upstreamBalance}</b><em class="cm-domain-metric-note neutral">最新余额快照</em></span>`:'';
  const financeButton=cm.report?.finance?.can_edit&&domain.configured?`<button type="button" class="cm-finance-open" data-cm-finance="${esc(domain.key)}">倍率配置</button>`:'';
  const upstreamButton=cm.report?.finance?.can_edit&&domain.configured?`<button type="button" class="cm-upstream-open" data-cm-upstream="${esc(domain.key)}">账户配置</button>`:'';
  return `<article class="cm-domain-card${open?' open':''}"><div class="cm-domain-head" role="button" tabindex="0" data-cm-domain-toggle="${esc(domain.key)}">
    <span class="cm-rank">${String(index+1).padStart(2,'0')}</span>
    <div class="cm-domain-identity"><span class="cm-domain-icon">${domain.configured?'◎':'—'}</span><div><b>${esc(domain.domain)}</b><small>${domain.vendors.length} 个厂商 · ${channels.length} 个实际渠道 · ${enabled} 个启用</small><div class="cm-domain-config"><div class="cm-domain-finance"><span class="${domain.finance?.configured?'ready':'pending'}">${esc(financeLabel)}</span>${financeButton}</div><div class="cm-domain-upstream">${upstreamSummary(domain.upstream)}${upstreamButton}</div></div></div></div>
    <div class="cm-share"><div><b>${share.toFixed(1)}%</b><small>${filtered?'筛选内':'全站'}${esc(metricLabel())}</small></div><i><em style="width:${Math.max(share&&2,share)}%"></em></i></div>
    <div class="cm-domain-metrics"><span class="cm-domain-requests"><small>渠道请求数</small><b>${nfmt(domain.usage.requests)}</b></span><span class="cm-domain-tokens"><small>Tokens</small><b title="${nfmt(domain.usage.tokens)}">${compact(domain.usage.tokens)}</b></span><span class="cm-domain-user-spend"><small>用户侧消费</small><b title="${usd(domain.usage.cost_usd)}">${usd(domain.usage.cost_usd)}</b><em class="cm-domain-metric-note neutral">当前查询区间</em></span>${upstreamMetrics}</div>
    <span class="cm-chevron">${open?'−':'+'}</span>
  </div>${open?`<div class="cm-domain-body">${economicsStrip(domain)}${costLedgerPanel(domain)}${domain.vendors.map(v=>vendorSection(domain,v)).join('')}</div>`:''}</article>`;
}

function filtersActive(){return !!(cm.filters.search||cm.filters.domain||cm.filters.vendor||cm.filters.group||cm.filters.status)}
function freshness(meta){
  const coverage=meta?.data_coverage||{},hasCoverage=typeof coverage.complete==='boolean',expected=+coverage.expected_hours||0,completed=+coverage.completed_hours||0,missing=+coverage.missing_hours||0;
  if(hasCoverage&&!coverage.complete&&!coverage.latest_hour_pending){
    return `<span class="cm-fresh-state warning"><i></i>小时数据待补 · ${(+coverage.percent||0).toFixed(1)}%</span><small>${nfmt(missing)} 个历史小时未完成，当前统计可能偏低</small>`;
  }
  const historical=meta?.to&&meta.to<cstDate(Date.now()/1000);
  if(historical)return `<span class="cm-fresh-state neutral"><i></i>区间数据截至 ${esc(shortDateTime(meta.data_until))}</span><small>历史区间 · 小时汇总</small>`;
  const until=+meta?.latest_data_until||+meta?.data_until||0;
  if(!until)return '<span class="cm-fresh-state stale"><i></i>暂无小时汇总数据</span><small>等待本地采集器生成汇总</small>';
  const ageSec=Math.max(0,Date.now()/1000-until),state=ageSec<=7200?'normal':ageSec<=10800?'warning':'stale';
  const label=state==='normal'?'正常':state==='warning'?'存在延迟':'采集可能中断';
  const detail=hasCoverage&&coverage.latest_hour_pending?`最新完整小时汇总中 · ${nfmt(completed)}/${nfmt(expected)} 小时`:state==='normal'?'正常延迟不超过约 2 小时':'请核对本地采集器状态';
  return `<span class="cm-fresh-state ${state}"><i></i>小时汇总 · ${label} · 截至 ${esc(shortDateTime(until))}</span><small>${detail}</small>`;
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
  if(cm.financeMode!=='site'){$('cmFinanceGroupRows').innerHTML='';return}
  $('cmFinanceGroupRows').innerHTML=rows.map((row,index)=>{
    const site=row.site_configured?row.site_multiplier:'';
    const source=Number.isFinite(+row.source_multiplier)&&(+row.source_multiplier)>0?`${(+row.source_multiplier).toFixed(2)}×`:'—';
    return `<div class="cm-finance-group-row" data-cm-finance-row="${index}">
      <b class="cm-finance-group-name" title="${esc(row.name)}">${esc(row.name)}</b>
      <span class="cm-finance-source-rate">${source}</span>
      <input type="number" min="0.000001" step="0.0001" inputmode="decimal" value="${site}" data-cm-finance-input="site" data-cm-finance-index="${index}" aria-label="${esc(row.name)} 我方倍率">
    </div>`;
  }).join('')||'<div class="cm-no-groups">尚未同步 NewAPI 分组，请点击“一键同步 NewAPI 分组”。</div>';
  refreshFinancePreview();
}
function refreshFinancePreview(){
  if(cm.financeMode==='site'){
    const complete=(cm.financeGroups||[]).filter((_,index)=>financeGroupInput(index,'site')!=null).length;
    if($('cmFinanceConfiguredCount'))$('cmFinanceConfiguredCount').textContent=`${complete}/${cm.financeGroups.length} 个分组已配置`;
  }
}
function financeChannelGroups(channel){
  const groups=new Map();
  (channel.groups||[]).forEach(group=>groups.set(group.name,group));
  (channel.configured_groups||[]).forEach(name=>{if(name&&!groups.has(name))groups.set(name,{name,usage:zero(),finance:{}})});
  return [...groups.values()].sort((a,b)=>String(a.name).localeCompare(String(b.name),'zh-CN'));
}
function channelStatusRank(channel){
  if(!channel.current)return 2;
  return +channel.status===1?0:1;
}
function renderFinanceChannelRows(){
  const target=$('cmFinanceDomainChannelRows');if(!target)return;
  const channels=cm.financeChannels||[];
  if(!channels.length){target.innerHTML='<div class="cm-no-groups">该主域名当前没有可配置的实际渠道</div>';return}
  target.innerHTML=channels.map(channel=>{
    const groups=financeChannelGroups(channel);
    const configured=groups.map(group=>group.finance||{}).filter(finance=>finance.upstream_configured);
    const first=configured[0]||{};
    const conflict=configured.some(finance=>finance.upstream_conflict||finance.upstream_group_name!==first.upstream_group_name||finance.upstream_multiplier!==first.upstream_multiplier||finance.upstream_discount_factor!==first.upstream_discount_factor);
    const upstreamGroupName=configured.find(finance=>String(finance.upstream_group_name||'').trim())?.upstream_group_name||'';
    const multiplier=conflict?'':(first.upstream_configured?first.upstream_multiplier:'');
    const discount=conflict?'':(first.upstream_configured?(first.upstream_discount_factor||1):'');
    const state=conflict?'<small class="cm-finance-channel-warning">历史配置不一致，请重新确认</small>':'';
    return `<article class="cm-finance-domain-channel"><div class="cm-finance-domain-channel-name"><b>#${esc(channel.id)} ${esc(channel.name)}</b>${state}</div><div class="cm-finance-channel-status">${statusLabel(channel)}</div><label class="cm-finance-upstream-group"><input type="text" maxlength="128" value="${esc(upstreamGroupName)}" data-cm-domain-rate="group-name" data-channel-id="${esc(channel.id)}" aria-label="#${esc(channel.id)} 上游分组名" placeholder="填写上游对应分组"></label><label><input type="number" min="0.000001" step="0.0001" inputmode="decimal" value="${multiplier}" data-cm-domain-rate="multiplier" data-channel-id="${esc(channel.id)}" aria-label="#${esc(channel.id)} 上游基础倍率"></label><label><input type="number" min="0.000001" step="0.0001" inputmode="decimal" value="${discount}" data-cm-domain-rate="discount" data-channel-id="${esc(channel.id)}" aria-label="#${esc(channel.id)} 上游折扣系数"></label></article>`;
  }).join('');
  target.insertAdjacentHTML('afterbegin','<div class="cm-finance-domain-channel-head"><span>渠道</span><span>状态</span><span>上游分组名</span><span>上游基础倍率</span><span>上游折扣系数</span></div>');
}
function setFinanceMode(mode){
  cm.financeMode=mode;
  $('cmFinanceGlobalSection').hidden=mode!=='site';
  $('cmFinanceGlobalGroups').hidden=mode!=='site';
  $('cmFinanceDomainSection').hidden=mode!=='domain';
  $('cmFinanceDomainChannels').hidden=mode!=='domain';
}
function showFinanceDialog(){
  $('cmFinanceMask').hidden=false;$('cmFinanceDialog').classList.add('show');$('cmFinanceDialog').setAttribute('aria-hidden','false');
  document.body.classList.add('cm-dialog-open');
}
function openSiteFinance(){
  if(!cm.report?.finance?.can_edit)return;
  cm.financeDomain=null;cm.financeChannel=null;cm.financeChannels=[];
  cm.financeGroups=Array.isArray(cm.report.website_groups)?cm.report.website_groups.slice():[];
  const global=cm.report.finance||{};
  $('cmFinanceScope').textContent='网站计价配置';
  $('cmFinanceTitle').textContent='网站计价基准';
  $('cmFinanceSubtitle').textContent='全站唯一配置；分组目录与当前倍率来自 NewAPI 分组管理，修改后会影响所有主域名和所有渠道的成本对照，并为受影响主域名保留版本。';
  $('cmFinanceFX').value=global.fx_benchmark||7;$('cmFinanceSitePaid').value=global.site_recharge_paid||1;$('cmFinanceSiteCredit').value=global.site_recharge_credit||1;
  $('cmFinanceFormulaTitle').textContent='网站计价折扣 = 我方分组倍率 × (我方充值支付 ÷ 我方充值到账) ÷ 折扣基准';
  $('cmFinanceFormulaNote').textContent='这里只维护网站全局口径；具体上游渠道的基础倍率和折扣系数在对应渠道行中配置。';
  const syncedAt=+cm.report.website_groups_synced_at||0;
  $('cmFinanceGroupSource').textContent=syncedAt?`分组来源：NewAPI 分组管理 · 最近同步 ${dateTime(syncedAt)}`:'尚未同步 NewAPI 分组';
  $('cmFinanceSyncGroups').hidden=false;
  $('cmFinanceSave').textContent='保存网站计价基准';setFinanceMode('site');showFinanceMessage('');renderFinanceRows();showFinanceDialog();
  setTimeout(()=>$('cmFinanceFX')?.focus(),30);
}
function openFinance(domainKey){
  if(!cm.report?.finance?.can_edit)return;
  const domain=(cm.report.domains||[]).find(item=>item.key===domainKey&&item.configured);
  if(!domain)return;
  cm.financeDomain=domain;cm.financeChannel=null;cm.financeGroups=[];
  cm.financeChannels=(domain.vendors||[]).flatMap(vendor=>(vendor.channels||[]).filter(channel=>channel.current).map(channel=>({...channel,vendor:vendor.name})))
    .sort((a,b)=>channelStatusRank(a)-channelStatusRank(b)||(+a.id||0)-(+b.id||0));
  const upstream=domain.finance||{};
  $('cmFinanceScope').textContent='主域名倍率配置';
  $('cmFinanceTitle').textContent=`${domain.domain} · 倍率配置`;
  $('cmFinanceSubtitle').textContent=upstream.version
    ? `当前版本 v${upstream.version} · 生效于 ${dateTime(upstream.effective_at)}。更新会创建新版本，旧版本永久保留。`
    : '首次保存将创建倍率版本 v1，生效时间为保存时间。配置只保存在 Monitor 本地。';
  $('cmFinanceUpPaid').value=upstream.configured?upstream.recharge_paid:1;
  $('cmFinanceUpCredit').value=upstream.configured?upstream.recharge_credit:1;
  $('cmFinanceFormulaTitle').textContent='上游实际倍率 = 基础倍率 × 折扣系数 ÷ 上游充值比例';
  $('cmFinanceFormulaNote').textContent='上游充值比例 = 充值到账 ÷ 充值支付；下方集中维护该主域名的所有渠道和其服务分组倍率。';
  $('cmFinanceSave').textContent=upstream.version?'更新倍率配置':'保存倍率配置';setFinanceMode('domain');showFinanceMessage('');renderFinanceChannelRows();showFinanceDialog();
}
function closeFinance(){
  if(!cm.financeMode)return;
  cm.financeMode='';cm.financeDomain=null;cm.financeChannel=null;cm.financeGroups=[];
  $('cmFinanceMask').hidden=true;$('cmFinanceDialog').classList.remove('show');$('cmFinanceDialog').setAttribute('aria-hidden','true');
  document.body.classList.remove('cm-dialog-open');showFinanceMessage('');
}
async function syncWebsiteGroups(){
  if(cm.financeMode!=='site'||!cm.report?.finance?.can_edit)return;
  const button=$('cmFinanceSyncGroups');if(!button)return;
  button.disabled=true;showFinanceMessage('正在读取 NewAPI 分组管理…');
  try{
    let res=await fetch('/channels/finance/site-groups/sync',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:'{}'});
    if(res.status===401){location.href='/login';return}
    let data=await res.json();
    if(res.status===409&&data.confirmation_required){
      const count=Number(data.group_count)||0;
      const impact=Number(data.affected_domains)||0;
      const confirmed=window.confirm(`NewAPI 当前有 ${count} 个用户可用分组，网站计价目录将按此结果更新。${impact?`\n\n将为 ${impact} 个主域名追加倍率版本。`:''}\n现有历史版本会保留，是否继续？`);
      if(!confirmed){showFinanceMessage('已取消同步，当前网站分组和倍率未变更。');return}
      showFinanceMessage('正在确认并保存 NewAPI 分组…');
      res=await fetch('/channels/finance/site-groups/sync',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify({confirm_update:true,expected_global_revision:data.current_global_revision||''})});
      if(res.status===401){location.href='/login';return}
      data=await res.json();
    }
    if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    closeFinance();cm.loaded=false;await loadReport();
  }catch(error){showFinanceMessage(error.message||'同步失败，请稍后重试。',true)}finally{button.disabled=false}
}
async function saveFinance(){
  if(!cm.financeMode||!cm.report?.finance?.can_edit)return;
  let endpoint,payload,kind;
  if(cm.financeMode==='site'){
    const values={fx_benchmark:financeNumber('cmFinanceFX'),site_recharge_paid:financeNumber('cmFinanceSitePaid'),site_recharge_credit:financeNumber('cmFinanceSiteCredit')};
    if(Object.values(values).some(value=>value==null)){showFinanceMessage('折扣基准和我方充值比例都必须填写大于 0 的数字。',true);return}
    const groups=[];for(let index=0;index<cm.financeGroups.length;index++){const site=financeGroupInput(index,'site');if(site!=null)groups.push({group:cm.financeGroups[index].name,site_multiplier:site})}
    endpoint='/channels/finance/site';payload={...values,groups};kind='site';
  }else if(cm.financeMode==='domain'){
    const paid=financeNumber('cmFinanceUpPaid'),credit=financeNumber('cmFinanceUpCredit');
    if(paid==null||credit==null){showFinanceMessage('上游充值支付和到账都必须填写大于 0 的数字。',true);return}
    const rates=[];
    for(const input of document.querySelectorAll('[data-cm-domain-rate="multiplier"]')){
      const groupNameInput=input.closest('.cm-finance-domain-channel')?.querySelector('[data-cm-domain-rate="group-name"]');
      const discountInput=input.closest('.cm-finance-domain-channel')?.querySelector('[data-cm-domain-rate="discount"]');
      const multiplier=Number(input.value),discount=Number(discountInput?.value);
      const groupName=String(groupNameInput?.value||'').trim();
      const hasMultiplier=input.value.trim()!==''||!!discountInput?.value.trim()||!!groupName;
      if(!hasMultiplier)continue;
      if(!Number.isFinite(multiplier)||multiplier<=0||!Number.isFinite(discount)||discount<=0){showFinanceMessage(`#${input.dataset.channelId} 的上游基础倍率和折扣系数必须同时填写大于 0 的数字。`,true);return}
      rates.push({channel_id:+input.dataset.channelId,upstream_group_name:groupName,upstream_multiplier:multiplier,upstream_discount_factor:discount});
    }
    endpoint='/channels/finance/domain-rates';payload={domain:cm.financeDomain.domain,upstream_recharge_paid:paid,upstream_recharge_credit:credit,rates};kind='domain';
  }
  const button=$('cmFinanceSave');button.disabled=true;showFinanceMessage('正在核对配置版本…');
  try{
    let res=await fetch(endpoint,{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify(payload)});
    if(res.status===401){location.href='/login';return}
    let data=await res.json();
    if(res.status===409&&data.confirmation_required){
      const globalImpact=kind==='site'&&+data.affected_domains>0?`\n\n将为 ${data.affected_domains} 个主域名追加版本。`:'';
      const current=kind==='site'?'全站计价配置':`当前版本 v${data.current_version}`;
      const next=kind==='site'?'新版本':`v${data.next_version}`;
      const confirmed=window.confirm(`${current}需要确认更新。\n\n确认保存为 ${next} 吗？${globalImpact}\n旧版本会完整保留，新版本从确认保存时开始生效。`);
      if(!confirmed){showFinanceMessage('已取消更新，当前版本未变更。');return}
      showFinanceMessage('正在创建新配置版本…');
      const confirmPayload={...payload,confirm_update:true};if(kind==='site')confirmPayload.expected_global_revision=data.current_global_revision||'';else confirmPayload.expected_version=data.current_version;
      res=await fetch(endpoint,{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify(confirmPayload)});
      if(res.status===401){location.href='/login';return}
      data=await res.json();
    }
    if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    // “没有变化”只表示数据库中的值已经相同，当前弹窗仍可能来自保存前
    // 的旧报表。无论是否创建新版本，都重新读取 SQLite 作为页面真相源。
    closeFinance();cm.loaded=false;await loadReport();
  }catch(error){showFinanceMessage(error.message||'保存失败，请稍后重试。',true)}finally{button.disabled=false}
}

function showUpstreamMessage(message,error=false){
  const el=$('cmUpstreamMessage');if(!el)return;
  el.textContent=message||'';el.classList.toggle('error',!!error);
}
function refreshAICodeWithKeyRows(){
  const list=$('cmUpstreamAPIKeyList');if(!list)return;
  const rows=[...list.querySelectorAll('.cm-upstream-key-row')];
  if(!rows.length){addAICodeWithKeyInput();return}
  rows.forEach((row,index)=>{
    const number=row.querySelector('[data-api-key-number]');if(number)number.textContent=String(index+1);
    const remove=row.querySelector('[data-remove-api-key]');if(remove)remove.hidden=rows.length===1&&!row.dataset.slotId;
  });
}
function addSavedAICodeWithKeySlot(slot){
  const list=$('cmUpstreamAPIKeyList');if(!list||!slot?.slot_id)return;
  const row=document.createElement('div');row.className='cm-upstream-key-row saved';row.dataset.slotId=slot.slot_id;
  const number=document.createElement('span');number.dataset.apiKeyNumber='';
  const name=document.createElement('input');name.type='text';name.maxLength=64;name.autocomplete='off';name.className='cm-upstream-api-key-name';name.placeholder='名称，例如：主账号';name.value=slot.name||'';name.dataset.originalName=slot.name||'';name.setAttribute('aria-label',`${slot.label||'已保存 Key'} 的名称`);
  const label=document.createElement('span');label.className='cm-upstream-saved-key';label.textContent=`${slot.label||'已保存 Key'} · ${slot.status==='ok'?'同步正常':slot.status==='error'?'同步异常':'等待同步'}`;
  const remove=document.createElement('button');remove.type='button';remove.dataset.removeApiKey='';remove.textContent='删除';
  row.append(number,name,label,remove);list.append(row);refreshAICodeWithKeyRows();
}
function addAICodeWithKeyInput(value='',keyName=''){
  const list=$('cmUpstreamAPIKeyList');if(!list)return;
  const row=document.createElement('div');row.className='cm-upstream-key-row';
  const number=document.createElement('span');number.dataset.apiKeyNumber='';
  const name=document.createElement('input');name.type='text';name.maxLength=64;name.autocomplete='off';name.placeholder='名称，例如：Claude 主线路';name.className='cm-upstream-api-key-name';name.value=keyName;
  const input=document.createElement('input');input.type='password';input.autocomplete='new-password';input.placeholder='sk-acw-...';input.className='cm-upstream-api-key';input.value=value;
  const remove=document.createElement('button');remove.type='button';remove.dataset.removeApiKey='';remove.textContent='删除';
  row.append(number,name,input,remove);list.append(row);refreshAICodeWithKeyRows();
  if(value===''&&keyName==='')name.focus();
}
function resetAICodeWithKeyInputs(){
  const list=$('cmUpstreamAPIKeyList');if(!list)return;
  cm.removedAICodeWithKeyIDs.clear();list.replaceChildren();addAICodeWithKeyInput('');
}
function renderAICodeWithKeySlots(slots){
  const list=$('cmUpstreamAPIKeyList');if(!list)return;
  cm.removedAICodeWithKeyIDs.clear();list.replaceChildren();
  (slots||[]).forEach(addSavedAICodeWithKeySlot);addAICodeWithKeyInput('');
}
function readAICodeWithKeyChanges(){
  const additions=[],renames=[];
  document.querySelectorAll('.cm-upstream-key-row').forEach(row=>{
    const name=(row.querySelector('.cm-upstream-api-key-name')?.value||'').trim();
    if(row.dataset.slotId){
      const original=row.querySelector('.cm-upstream-api-key-name')?.dataset.originalName||'';
      if(name!==original)renames.push({slot_id:row.dataset.slotId,name});
      return;
    }
    const apiKey=(row.querySelector('.cm-upstream-api-key')?.value||'').trim();
    if(apiKey||name)additions.push({name,api_key:apiKey});
  });
  return {additions,renames};
}
function updateAICodeWithKeyHelp(account){
  const help=$('cmUpstreamAPIKeysHelp');if(!help)return;
  const count=Number(account?.api_key_count)||0;
  help.textContent=count>0
    ?`已加密保存 ${count} 把 Key。名称可随时修改且不访问上游；可单独追加或删除，无需重新输入已保存 Key，页面永不回显密钥。`
    :'为每把 Key 填写名称和密钥，可按实际数量继续添加。每把 Key 只读自身消费，Monitor 完整取数后原子求和。';
}
function syncUpstreamFields(){
  const provider=$('cmUpstreamProvider')?.value||'newapi';
  document.querySelectorAll('.cm-upstream-newapi').forEach(el=>el.hidden=provider!=='newapi');
  document.querySelectorAll('.cm-upstream-sub2api').forEach(el=>el.hidden=provider!=='sub2api');
  document.querySelectorAll('.cm-upstream-aicodewith').forEach(el=>el.hidden=provider!=='aicodewith');
  const sub2AuthMode=$('cmUpstreamAuthMode')?.value||'password';
  document.querySelectorAll('.cm-upstream-sub2-password').forEach(el=>el.hidden=provider!=='sub2api'||sub2AuthMode!=='password');
  document.querySelectorAll('.cm-upstream-sub2-refresh').forEach(el=>el.hidden=provider!=='sub2api'||sub2AuthMode!=='refresh_token');
  const usageSupported=provider==='newapi'||provider==='sub2api'||provider==='aicodewith';
  const usageOption=document.querySelector('.cm-upstream-usage-option');
  if(usageOption)usageOption.hidden=!usageSupported;
  if($('cmUpstreamUsageLabel'))$('cmUpstreamUsageLabel').textContent=provider==='aicodewith'?'同步按 Key 消费账单':provider==='sub2api'?'同步消费汇总（Sub2API）':'同步消费日志（NewAPI）';
  if($('cmUpstreamUsageHelp'))$('cmUpstreamUsageHelp').textContent=provider==='aicodewith'
    ?'每 30 分钟同步当天累计；历史每批最多 31 个中国自然日，按天口径展示，不伪造小时明细'
    :provider==='sub2api'
      ?'优先使用小时汇总接口，旧版站点自动降级为单日汇总；当天追平与历史补数独立，不保存原始日志'
      :'默认每 30 分钟分页串行汇总；完整取得时间窗口后才原子替换本地汇总，不保存上游原始内容';
  if(provider==='aicodewith'&&!document.querySelector('.cm-upstream-api-key'))resetAICodeWithKeyInputs();
}
function upstreamState(status,enabled=true){
  if(!enabled)return {label:'已停用',level:'neutral'};
  if(status==='ok')return {label:'正常',level:'ok'};
  if(status==='error')return {label:'异常',level:'bad'};
  if(status==='reconnect')return {label:'需重新连接',level:'bad'};
  if(status==='unsupported')return {label:'待适配',level:'bad'};
  if(status==='stale')return {label:'数据陈旧',level:'warn'};
  if(status==='global_off')return {label:'灰度关闭',level:'neutral'};
  if(status==='disabled')return {label:'未开启',level:'neutral'};
  if(status==='queued')return {label:'等待首次同步',level:'warn'};
  if(status==='complete')return {label:'已完成',level:'ok'};
  if(status==='retry')return {label:'退避重试',level:'warn'};
  if(status==='blocked')return {label:'同步受阻',level:'bad'};
  if(status==='paging'||status==='backfilling')return {label:'历史补全',level:'warn'};
  return {label:'等待同步',level:'warn'};
}
function renderUpstreamStatus(account){
  const el=$('cmUpstreamStatus');if(!el)return;
  if(!account?.configured){el.hidden=true;el.innerHTML='';return}
  const balanceState=upstreamState(account.status,account.enabled);
  const usageState=account.usage_sync_enabled?upstreamState(account.usage_tail_phase||account.usage_effective_status||account.usage_status,account.enabled):{label:'未开启',level:'neutral'};
  const historyState=account.usage_sync_enabled?upstreamState(account.usage_history_phase,account.enabled):{label:'未开启',level:'neutral'};
  const balance=account.balance_usd==null?'尚未取得余额':`当前余额 ${usd(account.balance_usd)}`;
  const adapter=account.usage_adapter_name||'尚未确定同步适配器';
  const granularity=account.usage_granularity==='day'?'按天汇总':'按小时汇总';
  const backfill=account.usage_history_phase==='complete'?'历史补数已完成':account.usage_history_phase==='queued'?'历史补数等待首次调度':account.usage_history_phase==='retry'?'历史补数退避重试':account.usage_history_phase==='blocked'?'历史补数受阻':account.usage_backfill_progress?'历史补数断点续传':'历史补数中';
  const balanceError=account.last_error?`<em class="cm-upstream-status-error">余额错误：${esc(account.last_error)}</em>`:'';
  const usageError=account.usage_last_error?`<em class="cm-upstream-status-error">当天追平：${esc(account.usage_last_error)}</em>`:'';
  const historyError=account.usage_backfill_last_error?`<em class="cm-upstream-status-error">历史补数：${esc(account.usage_backfill_last_error)}</em>`:'';
  const historyProgress=account.usage_backfill_progress?`<span>历史补数：${esc(account.usage_backfill_progress)}</span>`:'';
  el.className=`cm-upstream-status cm-upstream-status-v2 ${esc(account.status||'pending')}`;
  el.innerHTML=`<div class="cm-upstream-status-head"><div><small>${esc(account.provider_name||account.provider||'上游账户')}</small><b>${esc(account.account_masked||'已配置账户')}</b></div><span>${account.enabled?'后台自动同步已开启':'后台自动同步已停用'}</span></div>
    <div class="cm-upstream-status-grid">
      <section class="cm-upstream-status-lane"><header><b>余额快照</b><span class="cm-upstream-status-chip ${balanceState.level}">${balanceState.label}</span></header><strong>${esc(balance)}</strong><span>最近成功 ${esc(dateTime(account.last_success_at))}<br>下次计划 ${esc(dateTime(account.next_sync_at))}</span>${account.unit_assumed?'<em>NewAPI 未公开换算单位，暂按默认 500,000 quota / USD 展示</em>':''}${balanceError}</section>
      <section class="cm-upstream-status-lane"><header><b>上游消费账单</b><span class="cm-upstream-status-chip ${usageState.level}">${usageState.label}</span></header>${account.usage_sync_enabled&&!account.usage_worker_enabled?'<strong>全局灰度闸门已关闭</strong><span>配置已保留，当前不会访问上游；余额同步不受影响。</span>':account.usage_sync_enabled?`<strong>${esc(adapter)} · ${esc(granularity)}</strong><span>当天水位 ${esc(dateTime(account.usage_data_until))}<br>最近成功 ${esc(dateTime(account.usage_last_success_at))} · 下次 ${esc(dateTime(account.usage_next_sync_at))}</span><div class="cm-upstream-status-progress"><span><small>当天追平</small><b>${esc(usageState.label)}</b></span><span><small>历史任务</small><b class="${esc(historyState.level)}">${esc(backfill)}</b></span></div>${usageError}${historyError}${historyProgress}`:'<strong>消费同步未开启</strong><span>开启后才会低频读取该上游；页面查询始终只读 Monitor 本地 SQLite。</span>'}</section>
    </div>`;
  el.hidden=false;
}
async function openUpstream(domainKey){
  if(!cm.report?.finance?.can_edit)return;
  const domain=(cm.report.domains||[]).find(item=>item.key===domainKey&&item.configured);
  if(!domain)return;
  cm.upstreamDomain=domain;cm.upstreamConfig=null;
  $('cmUpstreamTitle').textContent=`${domain.domain} · 余额自动同步`;
  $('cmUpstreamSubtitle').textContent='正在读取当前配置…';
  $('cmUpstreamStatus').hidden=true;
  $('cmUpstreamProvider').value='newapi';$('cmUpstreamBaseURL').value=`https://${domain.domain}`;
  $('cmUpstreamUserID').value='';$('cmUpstreamAccessToken').value='';$('cmUpstreamEmail').value='';$('cmUpstreamAuthMode').value='password';$('cmUpstreamPassword').value='';$('cmUpstreamRefreshToken').value='';resetAICodeWithKeyInputs();updateAICodeWithKeyHelp(null);$('cmUpstreamEnabled').checked=true;$('cmUpstreamUsageEnabled').checked=false;
  $('cmUpstreamSync').hidden=true;$('cmUpstreamUsageSync').hidden=true;showUpstreamMessage('');syncUpstreamFields();
  $('cmUpstreamMask').hidden=false;$('cmUpstreamDialog').classList.add('show');$('cmUpstreamDialog').setAttribute('aria-hidden','false');
  document.body.classList.add('cm-dialog-open');
  try{
    const res=await fetch('/channels/upstream?domain='+encodeURIComponent(domain.domain),{cache:'no-store',headers:{Accept:'application/json'}});
    if(res.status===401){location.href='/login';return}
    const data=await res.json();if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    if(cm.upstreamDomain!==domain)return;
    cm.upstreamConfig=data;
    $('cmUpstreamProvider').value=data.provider||'newapi';$('cmUpstreamBaseURL').value=data.base_url||`https://${domain.domain}`;
    $('cmUpstreamUserID').value=data.user_id||'';$('cmUpstreamEmail').value=data.email||'';$('cmUpstreamEnabled').checked=data.enabled!==false;$('cmUpstreamUsageEnabled').checked=!!data.usage_sync_enabled;
    $('cmUpstreamSubtitle').textContent=data.account?.configured
      ?'令牌已加密保存；敏感字段留空表示保持原连接不变。'
      :'选择中转站类型并填写首次连接参数。';
    $('cmUpstreamSync').hidden=!data.account?.configured||data.enabled===false;
    $('cmUpstreamUsageSync').hidden=!data.account?.configured||data.enabled===false||!data.account?.usage_sync_enabled;
    if(data.provider==='aicodewith')renderAICodeWithKeySlots(data.account?.api_key_slots||[]);
    renderUpstreamStatus(data.account);updateAICodeWithKeyHelp(data.account);syncUpstreamFields();showUpstreamMessage('');
    setTimeout(()=>$('cmUpstreamBaseURL')?.focus(),20);
  }catch(error){showUpstreamMessage(error.message||'读取配置失败。',true)}
}
function closeUpstream(){
  if(!cm.upstreamDomain)return;
  cm.upstreamDomain=null;cm.upstreamConfig=null;
  $('cmUpstreamAccessToken').value='';$('cmUpstreamPassword').value='';$('cmUpstreamRefreshToken').value='';resetAICodeWithKeyInputs();updateAICodeWithKeyHelp(null);
  $('cmUpstreamMask').hidden=true;$('cmUpstreamDialog').classList.remove('show');$('cmUpstreamDialog').setAttribute('aria-hidden','true');
  document.body.classList.remove('cm-dialog-open');showUpstreamMessage('');
}
async function saveUpstream(){
  if(!cm.upstreamDomain||!cm.report?.finance?.can_edit)return;
  const provider=$('cmUpstreamProvider').value,baseURL=$('cmUpstreamBaseURL').value.trim();
  if(!baseURL){showUpstreamMessage('请填写站点地址。',true);return}
  const usageSupported=provider==='newapi'||provider==='sub2api'||provider==='aicodewith';
  const payload={domain:cm.upstreamDomain.domain,provider,base_url:baseURL,enabled:$('cmUpstreamEnabled').checked,usage_sync_enabled:usageSupported&&$('cmUpstreamUsageEnabled').checked};
  if(provider==='newapi'){
    payload.user_id=Number($('cmUpstreamUserID').value);payload.access_token=$('cmUpstreamAccessToken').value.trim();
    if(!Number.isInteger(payload.user_id)||payload.user_id<=0){showUpstreamMessage('请填写有效的 NewAPI 用户 ID。',true);return}
  }else if(provider==='sub2api'){
    const authMode=$('cmUpstreamAuthMode').value;payload.email=$('cmUpstreamEmail').value.trim();
    if(authMode==='refresh_token')payload.refresh_token=$('cmUpstreamRefreshToken').value.trim();else payload.password=$('cmUpstreamPassword').value;
    if(!payload.email){showUpstreamMessage('请填写 Sub2API 登录邮箱。',true);return}
    if(authMode==='refresh_token'&&!payload.refresh_token&&!cm.upstreamConfig?.account?.configured){showUpstreamMessage('首次连接时请填写上游浏览器会话中的 Refresh Token。',true);return}
  }else{
    const keyChanges=readAICodeWithKeyChanges();payload.add_api_key_slots=keyChanges.additions;payload.rename_api_key_slots=keyChanges.renames;payload.remove_api_key_ids=[...cm.removedAICodeWithKeyIDs];
    if(payload.add_api_key_slots.some(item=>!item.api_key)){showUpstreamMessage('填写 Key 名称后，还需要填写对应的 AICodeWith API Key。',true);return}
    if(payload.add_api_key_slots.some(item=>!item.api_key.startsWith('sk-acw-'))){showUpstreamMessage('AICodeWith API Key 格式应为 sk-acw-...，请每把 Key 单独填写一行。',true);return}
  }
  const button=$('cmUpstreamSave');button.disabled=true;$('cmUpstreamSync').disabled=true;$('cmUpstreamUsageSync').disabled=true;showUpstreamMessage(payload.enabled?'正在安全连接并读取余额…':'正在停用自动同步…');
  try{
    const res=await fetch('/channels/upstream',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify(payload)});
    $('cmUpstreamAccessToken').value='';$('cmUpstreamPassword').value='';$('cmUpstreamRefreshToken').value='';payload.access_token='';payload.password='';payload.refresh_token='';payload.add_api_key_slots=[];payload.rename_api_key_slots=[];payload.remove_api_key_ids=[];
    if(res.status===401){location.href='/login';return}
    const data=await res.json();if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    cm.upstreamConfig={...(cm.upstreamConfig||{}),provider,base_url:baseURL,enabled:payload.enabled,account:data.account};
    if(provider==='aicodewith')renderAICodeWithKeySlots(data.account?.api_key_slots||[]);
    renderUpstreamStatus(data.account);updateAICodeWithKeyHelp(data.account);$('cmUpstreamSync').hidden=!data.account?.configured||!data.account?.enabled;$('cmUpstreamUsageSync').hidden=!data.account?.configured||!data.account?.enabled||!data.account?.usage_sync_enabled;
    cm.loaded=false;await loadReport();
    if(data.sync_error){showUpstreamMessage(`配置已保存，但本次同步失败：${data.sync_error}`,true);return}
    closeUpstream();
  }catch(error){$('cmUpstreamAccessToken').value='';$('cmUpstreamPassword').value='';$('cmUpstreamRefreshToken').value='';if(provider==='aicodewith')renderAICodeWithKeySlots(cm.upstreamConfig?.account?.api_key_slots||[]);showUpstreamMessage(error.message||'保存失败，请稍后重试。',true)}finally{button.disabled=false;$('cmUpstreamSync').disabled=false;$('cmUpstreamUsageSync').disabled=false}
}
async function syncUpstreamNow(){
  if(!cm.upstreamDomain)return;
  const button=$('cmUpstreamSync');button.disabled=true;$('cmUpstreamSave').disabled=true;$('cmUpstreamUsageSync').disabled=true;showUpstreamMessage('正在读取最新余额…');
  try{
    const res=await fetch('/channels/upstream/sync',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify({domain:cm.upstreamDomain.domain})});
    if(res.status===401){location.href='/login';return}
    const data=await res.json();if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    renderUpstreamStatus(data.account);cm.loaded=false;await loadReport();
    showUpstreamMessage(data.sync_error?`同步失败：${data.sync_error}`:'余额已更新。',!!data.sync_error);
  }catch(error){showUpstreamMessage(error.message||'同步失败，请稍后重试。',true)}finally{button.disabled=false;$('cmUpstreamSave').disabled=false;$('cmUpstreamUsageSync').disabled=false}
}

async function syncUpstreamUsageNow(){
  if(!cm.upstreamDomain)return;
  const button=$('cmUpstreamUsageSync');button.disabled=true;$('cmUpstreamSave').disabled=true;$('cmUpstreamSync').disabled=true;showUpstreamMessage('正在同步当天消费水位，并推进一个历史批次…');
  try{
    const res=await fetch('/channels/upstream/usage-sync',{method:'POST',headers:{Accept:'application/json','Content-Type':'application/json'},body:JSON.stringify({domain:cm.upstreamDomain.domain})});
    if(res.status===401){location.href='/login';return}
    const data=await res.json();if(!res.ok)throw new Error(data.error||`HTTP ${res.status}`);
    renderUpstreamStatus(data.account);cm.loaded=false;await loadReport();
    showUpstreamMessage(data.sync_error?`消费同步未完成：${data.sync_error}`:'当天水位已更新，历史补数按保护频率继续。',!!data.sync_error);
  }catch(error){showUpstreamMessage(error.message||'消费同步失败，请稍后重试。',true)}finally{button.disabled=false;$('cmUpstreamSave').disabled=false;$('cmUpstreamSync').disabled=false}
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
  // 上游账单和余额都按归并后的主域名账户计算，不从分组或渠道
  // 明细反向求和，避免同一渠道关联多个服务分组时被重复累计。
  const upstreamAccounts=domains.filter(domain=>domain.upstream?.configured);
  const upstreamUsageDomains=upstreamAccounts.filter(domain=>domain.upstream_usage?.available);
  const upstreamSpend=upstreamUsageDomains.reduce((sum,domain)=>sum+(+domain.upstream_usage.cost_usd||0),0);
	const adjustedUsageDomains=upstreamUsageDomains.filter(domain=>domain.upstream_usage.adjusted_cost_available);
	const adjustedUpstreamSpend=adjustedUsageDomains.reduce((sum,domain)=>sum+(+domain.upstream_usage.adjusted_cost_usd||0),0);
  const upstreamUsageComplete=upstreamUsageDomains.filter(domain=>domain.upstream_usage.complete).length;
  const upstreamBalanceDomains=upstreamAccounts.filter(domain=>domain.upstream.balance_usd!=null&&Number.isFinite(Number(domain.upstream.balance_usd)));
  const upstreamBalance=upstreamBalanceDomains.reduce((sum,domain)=>sum+Number(domain.upstream.balance_usd),0);
  const upstreamSpendReady=upstreamUsageDomains.length>0&&upstreamUsageComplete===upstreamUsageDomains.length;
  const exact=cm.economics?.totals||null,exactCoverage=cm.economics?.coverage||null;
  const exactKPIs=exact?`<article class="economics"><small>精确修正成本</small><b>${economicsMoneyLabel(exact.corrected_cost,exact.corrected_cost_known)}</b><span>${esc(economicsCoverageLabel(exactCoverage))}</span></article><article class="economics ${exact.profit_known?'':'warn'}"><small>精确毛利润</small><b>${economicsMoneyLabel(exact.profit,exact.profit_known)}</b><span>白名单域名 · 不随前端筛选重算</span></article><article class="economics ${exact.profit_known?'':'warn'}"><small>精确毛利率</small><b>${exact.profit_known?esc(exact.margin_display||'不可判定'):'不可判定'}</b><span>${exact.profit_known?'证据已闭合':esc(economicsReason(exact.unknown_reason))}</span></article>`:'';
  const summary=$('cmSummary');
  if(summary){summary.innerHTML=`<section class="cm-kpis">
    <article><small>已配置主域名</small><b>${nfmt(configuredDomains)}</b><span>当前显示 ${nfmt(domains.length)} 个归并项</span></article>
    <article><small>当前实际渠道</small><b>${nfmt(currentChannels.length)}</b><span>${nfmt(enabled)} 启用 · ${nfmt(currentChannels.length-enabled)} 停用${historical?' · '+nfmt(historical)+' 历史':''}</span></article>
    <article class="${unconfigured?'warn':''}"><small>未归并渠道</small><b>${nfmt(unconfigured)}</b><span>${unconfigured?'尚未配置主地址':'当前渠道均已归并'}</span></article>
    <article><small>渠道请求数</small><b>${nfmt(filteredUsage.requests)}</b><span>${cm.report.meta.from} 至 ${cm.report.meta.to}</span></article>
    <article><small>区间 Tokens</small><b title="${nfmt(filteredUsage.tokens)}">${compact(filteredUsage.tokens)}</b><span>prompt + completion</span></article>
    <article class="accent"><small>用户侧消费</small><b>${usd(filteredUsage.cost_usd)}</b><span>NewAPI logs.quota</span></article>
    <article class="upstream ${upstreamUsageDomains.length&&!upstreamSpendReady?'warn':''}"><small>区间上游消费汇总</small><b>${upstreamUsageDomains.length?usd(upstreamSpend):'—'}</b><span>${upstreamUsageDomains.length?`${nfmt(upstreamUsageComplete)}/${nfmt(upstreamUsageDomains.length)} 个账单完整${upstreamSpendReady?'':' · 补全中'}`:`${nfmt(upstreamAccounts.length)} 个账户尚无消费数据`}</span></article>
	<article class="adjusted ${adjustedUsageDomains.length<upstreamUsageDomains.length?'warn':''}"><small>上游修正消费汇总</small><b>${adjustedUsageDomains.length?usd(adjustedUpstreamSpend):'—'}</b><span>${nfmt(adjustedUsageDomains.length)}/${nfmt(upstreamUsageDomains.length)} 个账户已配置充值比例</span></article>
    <article class="balance ${upstreamBalanceDomains.length<upstreamAccounts.length?'warn':''}"><small>上游当前余额汇总</small><b>${upstreamBalanceDomains.length?usd(upstreamBalance):'—'}</b><span>${nfmt(upstreamBalanceDomains.length)}/${nfmt(upstreamAccounts.length)} 个账户已取得余额</span></article>
    ${exactKPIs}
    ${filtered?`<article><small>筛选${esc(metricLabel())}占比</small><b>${share.toFixed(1)}%</b><span>相对当前日期全部渠道</span></article>`:''}
  </section>`;
    const kpis=summary.querySelector('.cm-kpis');
    if(kpis){
      const count=kpis.children.length;
      kpis.dataset.count=String(count);
      kpis.style.setProperty('--cm-kpi-columns',String(Math.max(1,Math.ceil(count/2))));
    }
    summary.removeAttribute('aria-busy')}
  $('cmBody').innerHTML=`<section class="cm-list-head"><div><h3>渠道排名</h3><p>共 ${nfmt(domains.length)} 个归并项 · 按 ${esc(metricLabel())} 从高到低排序，逐级展开厂商类型、实际渠道与服务分组。</p></div><div class="cm-fresh">${freshness(cm.report.meta)}<small>渠道配置快照 ${esc(dateTime(cm.report.meta.channel_config_updated_at))}</small></div></section>
  <div class="cm-domain-list">${domains.map((domain,index)=>domainCard(domain,index,filteredUsage,filtered)).join('')||'<div class="cm-empty"><b>当前筛选没有匹配渠道</b><p>请重置筛选条件或更换日期范围。</p></div>'}</div>`;
}

document.addEventListener('DOMContentLoaded',()=>{if($('tab-channels')&&!cm.inited)init()});
})();
