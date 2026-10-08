(function(){
'use strict';
// An on-demand, local-only reader. No polling, sync requests or mutation API.
const panels=new Map();
const esc=value=>String(value??'').replace(/[&<>"']/g,char=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const time=ts=>ts>0?new Date(ts*1000).toLocaleString('zh-CN',{hour12:false,timeZone:'Asia/Shanghai'}):'—';
const keyOf=domain=>String(domain.key||domain.domain);
const labels={same:'与当前配置吻合',different:'与当前配置不同，待核对',mixed_rates:'同小时多种倍率，不能自动判断',not_configured:'未找到同名分组配置',configuration_conflict:'配置存在不同倍率，待核对',discount_only:'仅有折扣，不能确认完整倍率',unverified:'证据未核验，不能判断'};
const evidenceLabels={recent:'近3小时已核验',historical:'历史观测，非当前状态',unverified:'尚未核验'};

function observationRow(row,data){
  const stored=data.configurations&&Object.hasOwn(data.configurations,row.source_group)?data.configurations[row.source_group]:null;
  const configs=Array.isArray(stored)?stored:[];
  const configText=configs.slice(0,12).map(config=>`<span>#${esc(config.channel_id)} ${esc(config.channel_name)} · ${esc(config.local_group)}：${esc(config.base_multiplier||'—')}× × ${esc(config.discount_factor||'—')} = ${esc(config.effective_multiplier||'无效')}×</span>`).join('')||'—';
  const rates=(row.rates||[]).map(value=>esc(value)+'×').join(' / ')||((row.discounts||[]).length?'折扣 '+row.discounts.map(esc).join(' / '):'—');
  const recent=row.evidence_status==='recent',warn=recent&&row.comparison==='different';
  return `<tr><td>${esc(row.source_group||'未返回分组')}<small>${esc(row.model_name||'未返回模型')}</small></td><td>${configText}${configs.length>12?`<small>共 ${configs.length} 条配置，仅展示前12条</small>`:''}</td><td>${rates}</td><td class="${warn?'cm-pricing-difference':''}">${esc(labels[row.comparison]||'不能判断')}<small>${esc(evidenceLabels[row.evidence_status]||'尚未核验')}</small></td><td>${esc(time(row.hour_ts))}<small>${esc(row.requests)} 次请求</small></td></tr>`;
}

function render(domain,canEdit){
  if(!canEdit||!domain.upstream?.configured)return'';
  const key=keyOf(domain),panel=panels.get(key);
  if(!panel)return `<section class="cm-cost-ledger-closed"><button type="button" data-cm-pricing-toggle="${esc(key)}">日志倍率核对</button><span>按账号、上游分组和模型对比 · 只读，不自动改价</span></section>`;
  let body='<p class="cm-cost-ledger-loading">正在读取本地日志计价证据…</p>';
  if(panel.error)body=`<p class="cm-cost-ledger-error">${esc(panel.error)}</p>`;
  else if(panel.data){
    const data=panel.data,rows=data.rows||[],changes=data.changes||[];
    body=`<div class="cm-cost-ledger-block"><p class="cm-pricing-note">${data.collection_active?'复用现有计价采集，非实时探测':'当前计价采集未启用，仅展示已有记录'} · 最近采集小时：${esc(time(data.as_of_hour))}。展示最近24个采集小时中每个分组／模型的最后一个请求小时，不代表所有模型均有证据。比较的是当前人工配置与日志时点倍率；差异不证明上游改价原因或精确生效时间。</p>${data.truncated?'<p class="cm-cost-ledger-error">证据或配置超过安全展示上限，本次不发布局部倍率判断。</p>':`<div class="cm-pricing-table"><table><thead><tr><th>上游分组 / 模型</th><th>配置：基础倍率 × 折扣</th><th>日志请求计价倍率</th><th>核对结果</th><th>日志小时 / 请求数</th></tr></thead><tbody>${rows.map(row=>observationRow(row,data)).join('')||'<tr><td colspan="5">最近24个采集小时没有可展示的请求计价证据；不代表倍率未变化。</td></tr>'}</tbody></table></div>`}<p class="cm-pricing-note">配置按同名上游分组列出，不证明请求令牌归属于某条渠道。仅折扣证据不反推基础倍率；充值支付／到账比例不从使用日志推算，也不参与此倍率对比。</p></div><div class="cm-cost-ledger-block"><h4>最近日志观测变化</h4>${changes.map(change=>`<p>${esc(change.source_group)} / ${esc(change.model_name)}：${esc(change.previous)} → ${esc(change.current)} <small>${esc(time(change.hour_ts))}</small></p>`).join('')||'<p class="cm-cost-empty">没有已记录的变化事件；不代表历史倍率一直相同。</p>'}${data.changes_truncated?'<p class="cm-pricing-note">仅展示最近20条观测变化。</p>':''}</div>`;
  }
  return `<section class="cm-cost-ledger cm-pricing-observations"><header><div><b>日志倍率核对</b><small>账号级计价证据，不修改配置、账单或经营核算金额</small></div><div class="cm-cost-ledger-actions"><button type="button" data-cm-pricing-refresh="${esc(key)}" ${panel.loading?'disabled':''}>刷新本地证据</button><button type="button" data-cm-pricing-toggle="${esc(key)}">收起</button></div></header>${body}</section>`;
}

async function load(domain,rerender){
  const key=keyOf(domain);panels.get(key)?.abort?.abort();
  const panel={loading:true,abort:new AbortController()};panels.set(key,panel);rerender();
  let timedOut=false;
  const timer=setTimeout(()=>{timedOut=true;panel.abort.abort()},5000);
  try{
    const response=await fetch('/channels/upstream/pricing-observations?'+new URLSearchParams({domain:domain.domain}),{cache:'no-store',headers:{Accept:'application/json'},signal:panel.abort.signal});
    if(panels.get(key)!==panel)return;
    if(response.status===401){location.href='/login';return}
    let data;
    try{data=await response.json()}catch{throw new Error(response.ok?'本地证据响应格式异常':`本地证据读取失败（HTTP ${response.status}）`)}
    if(panels.get(key)!==panel)return;
    if(!response.ok)throw new Error(data.error||`本地证据读取失败（HTTP ${response.status}）`);
    panel.data=data;
  }catch(error){
    if(panels.get(key)!==panel)return;
    panel.error=timedOut?'本地证据读取超时，请稍后重试':error.message||'本地证据读取失败';
  }finally{
    clearTimeout(timer);
    if(panels.get(key)===panel){panel.loading=false;rerender()}
  }
}

function toggle(domain,rerender){
  const key=keyOf(domain),panel=panels.get(key);
  if(panel){panel.abort?.abort();panels.delete(key);rerender();return}
  return load(domain,rerender);
}
function reset(){for(const panel of panels.values())panel.abort?.abort();panels.clear()}
window.channelPricingObservations={render,toggle,load,reset,hasOpen:()=>panels.size>0};
}());
