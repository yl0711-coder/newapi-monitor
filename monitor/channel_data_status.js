/* Business values stay numeric. Quiet freshness notes accompany upstream
 * amounts; diagnostic details live on the sync page. Never promote partial coverage to complete. */
(()=>{
'use strict';
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const time=value=>value?new Date(Number(value)*1000).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}):'未知';
const known=value=>value!==null&&value!==undefined&&value!==''&&Number.isFinite(Number(value));
// Select a separately scoped bill only for an unsupported time boundary, never
// to conceal an invalid amount or overlapping buckets in the exact report.
function billView(domain){
  const exact=domain.upstream_usage||{};
  const daily=exact.integrity_status==='window_mismatch'?domain.natural_day_bill:null;
  return {usage:daily?.usage||exact,daily};
}
function billRangeNote(daily){
  if(!daily)return '';
  const usage=daily.usage||{},until=usage.data_until||daily.to_ts;
  return `${time(daily.from_ts)} → ${time(until)}（北京时间）；${usage.complete?'':'已取得账单合计；'}非所选小时区间金额`;
}
const billSyncLabels={
  reconnect:'消费同步需重新连接',error:'消费同步失败',stale:'消费同步延迟',
  global_off:'消费同步已暂停',disabled:'消费同步已停用',unsupported:'暂不支持消费同步',
  queued:'消费待更新',pending:'消费待更新',paging:'消费待更新',backfilling:'消费待更新',
};
function billSyncTime(value){
  const ts=Number(value),date=new Date(ts*1000);
  if(!Number.isFinite(ts)||ts<=0||!Number.isFinite(date.getTime()))return '';
  return date.toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false});
}
function billSyncNote(domain){
  const account=domain.upstream||{},usage=billView(domain).usage;
  if(!account.configured&&!usage.available)return '';
  const phase=account.usage_effective_status||account.usage_tail_phase||account.usage_status;
  let label=billSyncLabels[phase]||'';
  if(!account.configured)label='消费同步未配置';
  else if(account.usage_sync_enabled===false)label='消费同步未开启';
  else if(account.usage_worker_enabled===false)label='消费同步已暂停';
  if(!label){
    if(!usage.available)label='暂无区间消费账单';
    else if(usage.provisional)label='消费待日账单核对';
    else if(usage.complete!==true)label='区间消费尚未同步完整';
    else if(account.usage_fresh===false)label='消费同步延迟';
    else if(account.usage_fresh!==true)label='消费同步时效待确认';
  }
  if(!label)return '';
  // Account watermark describes recent synchronization, even for a completed
  // historical selection. Never substitute that selection's end timestamp.
  const until=billSyncTime(account.usage_data_until),synced=billSyncTime(account.usage_last_success_at);
  const stamp=until?`消费截至 ${until}（北京）`:synced?`最近同步 ${synced}（北京）`:'';
  return [label,stamp,'近期消费请到上游官网核对'].filter(Boolean).join(' · ');
}
const integrityReasons={
  overlapping_buckets:'账单时间桶重叠，金额未计入汇总',
  invalid_amount:'账单金额非法，金额未计入汇总',
  window_mismatch:'自然日账单无法精确拆分至所选区间；请选已结束的完整自然日核对',
};
const usageSyncReasons={
  reconnect:'认证已失效，请在账户配置中重新连接；自动重试不能恢复失效凭证',
  stale:'消费同步水位已陈旧，已有历史账单不代表后续数据仍在更新',
  error:'消费同步失败，请检查上游错误；已有历史账单不代表同步恢复',
  global_off:'消费同步采集器未开启，当前仅有历史数据',
  disabled:'该账户消费同步已停用，当前仅有历史数据',
  unsupported:'当前上游接口不支持消费同步',
};
function issues(report){
  const rows=[],coverage=report?.meta?.data_coverage;
  if(!coverage||coverage.complete!==true||coverage.provisional_seconds>0){
    const detail=coverage?`已确认 ${coverage.completed_hours||0}/${coverage.expected_hours||0} 小时；缺少 ${coverage.missing_hours||0} 小时`:'覆盖状态未返回';
    const pending=coverage?.latest_hour_pending?`；最新小时 ${time(coverage.pending_hour_ts)} 尚在汇总，并非已确认丢失`:'';
    rows.push({scope:'用户侧用量',detail:detail+pending+'。请求数、Tokens、消费及占比按现有记录计算，可能低于最终值。'});
  }
  for(const domain of report?.domains||[]){
    const account=domain.upstream||{},usage=billView(domain).usage,reasons=[];
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
          if(usage.provisional)reasons.push('春秋当前日小时金额来自已采集明细，尚待跨日后与日账单总数核对；迟到或修正记录可能更新金额');
          if(!usage.complete&&(!usage.provisional||usage.completed_hours<usage.expected_hours))reasons.push(`账单已覆盖 ${usage.completed_hours||0}/${usage.expected_hours||0} 小时，汇总仅含已校验金额`);
          if(!usage.adjusted_cost_available)reasons.push(usage.adjusted_cost_status==='bucket_boundary_ambiguous'?'充值比例在账单桶中途变化，修正消费无法精确拆分':'缺少对应时段充值比例证据，未计入修正消费汇总');
          else if(!known(usage.adjusted_cost_usd))reasons.push('修正消费金额未返回，未计入修正消费汇总');
        }
      }
    }
    else if(domain.enabled_channels>0)reasons.push('消费日志同步未开启，仅有余额不能核验成本');
    if(account.usage_sync_enabled){
      const syncReason=usageSyncReasons[account.usage_effective_status||account.usage_status];
      if(syncReason)reasons.push(syncReason);
    }
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
  const dailyNotes=(report?.domains||[]).filter(domain=>billView(domain).daily).map(domain=>`<p class="muted">${esc(domain.domain)}：${esc(billRangeNote(billView(domain).daily))}。上游按自然日出账，渠道卡片单独展示，不计入精确区间汇总。</p>`).join('');
  return `<p class="muted">渠道管理当前日期范围（全部账户）：${esc(time(meta.from_ts))} → ${esc(time(meta.to_ts))}（结束时间不含）。仅检查本地数据，不触发补数。</p>`+
    (rows.length?`<div class="sync-status bad">${rows.length} 项数据异常 / 待核验</div><div class="sync-upstream-list">${rows.map(row=>`<article class="sync-upstream-account"><b class="sync-upstream-error">${esc(row.scope)}</b><p class="sync-upstream-error">${esc(row.detail)}</p></article>`).join('')}</div>`:'<span class="sync-status ok">用量覆盖与可用账单校验通过</span>')+dailyNotes;
}
window.channelDataStatus={issues,note,render,known,billView,billRangeNote,billSyncNote};
})();
