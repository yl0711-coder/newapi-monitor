/* Shared presentation policy: business values stay numeric; diagnostic details
 * live on the sync page. Never promote partial coverage to complete. */
(()=>{
'use strict';
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const time=value=>value?new Date(Number(value)*1000).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}):'未知';
const known=value=>value!==null&&value!==undefined&&value!==''&&Number.isFinite(Number(value));
const integrityReasons={
  overlapping_buckets:'账单时间桶重叠，金额未计入汇总',
  invalid_amount:'账单金额非法，金额未计入汇总',
  window_mismatch:'自然日账单无法精确拆分至所选区间；请选已结束的完整自然日核对',
};
function issues(report){
  const rows=[],coverage=report?.meta?.data_coverage;
  if(!coverage||coverage.complete!==true||coverage.provisional_seconds>0){
    const detail=coverage?`已确认 ${coverage.completed_hours||0}/${coverage.expected_hours||0} 小时；缺少 ${coverage.missing_hours||0} 小时`:'覆盖状态未返回';
    const pending=coverage?.latest_hour_pending?`；最新小时 ${time(coverage.pending_hour_ts)} 尚在汇总，并非已确认丢失`:'';
    rows.push({scope:'用户侧用量',detail:detail+pending+'。请求数、Tokens、消费及占比按现有记录计算，可能低于最终值。'});
  }
  for(const domain of report?.domains||[]){
    const account=domain.upstream||{},usage=domain.upstream_usage||{},reasons=[];
    if(!account.configured){
      if(domain.enabled_channels>0)rows.push({scope:domain.domain||'未归并渠道',detail:`${domain.enabled_channels} 个启用渠道未配置上游账户，无法同步余额与账单。`});
      continue;
    }
    if(domain.missing_rate_channels>0)reasons.push(`${domain.missing_rate_channels} 个启用渠道缺少倍率配置，不能据此核验渠道成本`);
    if(!known(account.balance_usd))reasons.push('余额未取得，未计入余额汇总');
    else if(['error','reconnect','stale'].includes(account.status))reasons.push('余额同步异常或已陈旧，当前展示最近一次已取得余额');
    if(account.usage_sync_enabled){
      if(!usage.available)reasons.push(integrityReasons[usage.integrity_status]||'所选区间暂无消费账单，未计入消费汇总');
      else{
        const integrity=usage.integrity_status||'complete';
        if(integrity!=='complete')reasons.push(integrityReasons[integrity]||'账单校验未通过，金额未计入汇总');
        else{
          if(!known(usage.cost_usd))reasons.push('消费金额未返回，未计入消费汇总');
          if(!usage.complete)reasons.push(`账单已覆盖 ${usage.completed_hours||0}/${usage.expected_hours||0} 小时，汇总仅含已校验金额`);
          if(!usage.adjusted_cost_available)reasons.push(usage.adjusted_cost_status==='bucket_boundary_ambiguous'?'充值比例在账单桶中途变化，修正消费无法精确拆分':'缺少对应时段充值比例证据，未计入修正消费汇总');
          else if(!known(usage.adjusted_cost_usd))reasons.push('修正消费金额未返回，未计入修正消费汇总');
        }
      }
    }
    else if(domain.enabled_channels>0)reasons.push('消费日志同步未开启，仅有余额不能核验成本');
    if(account.usage_status==='reconnect')reasons.push('认证已失效，请在账户配置中重新连接；自动重试不能恢复失效凭证');
    if(reasons.length)rows.push({scope:domain.domain,detail:reasons.join('；')});
  }
  return rows;
}
function note(report){
  const warning=issues(report).length?'金额及用量按现有记录统计，后续可能更新。':'';
  return `<p class="cm-data-note">${warning}<a href="#tab=sync">查看数据同步状态</a></p>`;
}
function render(report){
  if(report?.enabled===false)return '<p class="muted">渠道用量未启用，无法核验区间覆盖。</p>';
  const rows=issues(report),meta=report?.meta||{};
  return `<p class="muted">渠道管理当前日期范围（全部账户）：${esc(time(meta.from_ts))} → ${esc(time(meta.to_ts))}（结束时间不含）。仅检查本地数据，不触发补数。</p>`+
    (rows.length?`<div class="sync-status bad">${rows.length} 项数据异常 / 待核验</div><div class="sync-upstream-list">${rows.map(row=>`<article class="sync-upstream-account"><b class="sync-upstream-error">${esc(row.scope)}</b><p class="sync-upstream-error">${esc(row.detail)}</p></article>`).join('')}</div>`:'<span class="sync-status ok">区间覆盖与账户金额校验通过</span>');
}
window.channelDataStatus={issues,note,render,known};
})();
