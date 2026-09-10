/* Read-only ECS log status. Resource controls remain in infra_assets.js. */
(() => {
  'use strict';
  const TIMEOUT_MS = 10000;
  const labels = Object.freeze({
    reporting:'近期有上报', heartbeating_no_log_batches:'代理在线，尚无日志批次',
    starting:'启动宽限中', awaiting_first_report:'等待首次上报', lease_expired:'注册租约已过期',
    report_stale:'上报中断', revoked:'来源已撤销', stopped_delivery_unconfirmed:'已停止，最终交付待确认',
    delivery_gap_unresolved:'退出交付缺口待核实', discovered:'AWS 发现正常',
    discovery_failed:'AWS 发现失败', discovery_stale:'AWS 发现已过期', awaiting_discovery:'等待 AWS 发现',
    discovery_degraded:'AWS 发现异常', collection_degraded:'采集异常', awaiting_sources:'尚无运行来源登记',
    not_configured:'已不在授权配置中',
    stopped_archive_drained:'已停止，采集批次已交付', stopped_archives_drained:'任务已停止，采集批次已交付',
    collector_chain_closed:'采集批次链已闭合（非业务完整性证明）', awaiting_closure:'等待采集链结束标记',
    archive_delivery_conflict:'归档存在冲突或拒收，交付待核实',
    retained_files_verified:'保留日志文件与最终采集偏移已核对', awaiting_final_boundary:'最终日志文件边界待核对',
    scanned:'本轮归档检查完成', scan_progressing:'归档分段检查中', scan_stale:'归档检查已过期', scan_failed:'归档读取失败',
    recovery_pending:'归档补传有待处理项', awaiting_first_scan:'等待首次归档检查', unavailable:'归档恢复状态不可用',
  });
  const esc = value => String(value ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const stamp = seconds => seconds > 0 ? new Date(seconds*1000).toLocaleString() : '尚无记录';
  const short = arn => String(arn || '').split('/').at(-1);
  const number = value => Number.isSafeInteger(value) && value >= 0 ? value : 0;
  const root = () => document.getElementById('ecsLogSources');
  let selection = {service:'', phase:'active', page:1}, sequence = 0, controller = null, lastData = null, navigating = false;
  const opened = new Set();
  const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
  const archiveHint = s => s.archive_status ? `<div class="muted" style="font-size:11px">${esc(labels[s.archive_status]||'归档状态待核实')}${s.archive_closed_at>0?' · '+esc(stamp(s.archive_closed_at)):''}${s.final_boundary_status?'<br>'+esc(labels[s.final_boundary_status]||'最终边界待核实'):''}</div>` : '';
  function valid(data, page) {
    return record(data) && typeof data.enabled === 'boolean' && Array.isArray(data.sources)
      && data.sources.every(s=>record(s)&&['service_arn','task_arn','node','container','lane','status'].every(k=>typeof s[k]==='string'))
      && record(data.health) && data.health.available === true && Array.isArray(data.health.services)
      && data.health.services.every(s=>record(s)&&typeof s.service_arn==='string'&&typeof s.status==='string')
      && Number.isSafeInteger(data.total) && data.total >= 0 && data.page === page && typeof data.has_more === 'boolean';
  }

  function rows(sources) {
    if (!sources.length) return '<p class="muted">此筛选下暂无来源；不能据此判断日志完整或服务已下线。</p>';
    const tasks = new Map();
    for (const source of sources) {
      const key = source.service_arn + '\n' + source.task_arn;
      if (!tasks.has(key)) tasks.set(key, []);
      tasks.get(key).push(source);
    }
    let html = '';
    for (const items of tasks.values()) {
      const taskKey = encodeURIComponent(items[0].service_arn+'\n'+items[0].task_arn);
      html += `<details data-task="${esc(taskKey)}" ${opened.has(taskKey)?'open':''}><summary style="cursor:pointer;padding:8px 0">${esc(short(items[0].service_arn))} · Task ${esc(short(items[0].task_arn))} · 本页 ${items.length} 条采集链路</summary><div class="monitor-table-scroll"><table><thead><tr><th>容器 / 链路</th><th>采集状态</th><th>更新时间</th></tr></thead><tbody>`;
      for (const s of items) html += `<tr><td>${esc(s.container)} / ${esc(s.lane)}<div class="muted" style="font-size:11px;overflow-wrap:anywhere">${esc(s.node)}<br>Runtime ${esc(s.runtime_id)}</div></td><td>${esc(labels[s.status] || '状态待核实')}${s.stopped_at ? '<div class="muted">停止时间：'+esc(stamp(s.stopped_at))+'</div>' : ''}${archiveHint(s)}</td><td class="muted">心跳：${esc(stamp(s.last_heartbeat))}<br>批次接收：${esc(stamp(s.last_report))}<br>租约到期：${esc(stamp(s.lease_until))}</td></tr>`;
      html += '</tbody></table></div></details>';
    }
    return html;
  }

  function render(data) {
    if (data.enabled === false) {
      root().textContent = 'ECS 动态日志接入未启用；现有 Lightsail / 静态采集路径不受影响。';
      return;
    }
    const h = data.health, services = h.services;
    let html = '<p class="muted">CPU / 内存监控与日志采集独立；心跳在线不代表区间数据完整。任务记录按链路计数，不计作请求数。</p>';
    if (record(h.archive)) {
      const a=h.archive;
      html += `<p class="muted" style="font-size:12px">任务外归档：${esc(labels[a.status]||'状态待核实')} · 已确认 ${number(a.accepted_batches)} 批 · 待补传 ${number(a.pending_batches)} 批 · 冲突 ${number(a.conflict_batches)} 批 · 拒收/过期 ${number(a.rejected_batches)} 批 · 采集链结束 ${number(a.collector_closures)} 条。最后扫描 ${esc(stamp(a.last_scan))}${a.last_progress>0?' · 最近推进 '+esc(stamp(a.last_progress)):''}${a.last_failure>0?' · 最近失败 '+esc(stamp(a.last_failure)):''}；日志最终边界尚待校验，不计作完整覆盖。</p>`;
    }
    html += '<div class="monitor-table-scroll"><table><thead><tr><th>服务</th><th>当前来源</th><th>采集 / 发现</th><th>退出交付</th></tr></thead><tbody>';
    for (const s of services) html += `<tr><td>${esc(short(s.service_arn))}<div class="muted" style="font-size:11px;overflow-wrap:anywhere">${esc(s.service_arn)}</div></td><td>${number(s.active_tasks)} 个任务 / ${number(s.active_sources)} 条链路<div class="muted">启动中 ${number(s.starting_sources)} · 累计 ${number(s.task_count)} 个任务</div></td><td>${esc(labels[s.status]||'状态待核实')}<div class="muted">${esc(labels[s.discovery_status]||'状态待核实')} · 异常 ${number(s.unhealthy_sources)}</div></td><td>待确认 ${number(s.stopped_pending)} · 采集已交付 ${number(s.stopped_drained)}<div class="muted">缺口待核实 ${number(s.delivery_gaps)}</div></td></tr>`;
    html += '</tbody></table></div>';
    html += '<label>服务 <select data-select="service"><option value="">全部服务</option>' + services.map(s=>`<option value="${esc(s.service_arn)}" ${selection.service===s.service_arn?'selected':''}>${esc(short(s.service_arn))}</option>`).join('')+'</select></label> ';
    html += '<label>任务阶段 <select data-select="phase">'+[['active','运行 / 待核实'],['stopped','已停止记录'],['all','全部记录']].map(([value,label])=>`<option value="${value}" ${selection.phase===value?'selected':''}>${label}</option>`).join('')+'</select></label>';
    html += `<p class="muted">当前筛选 ${number(data.total)} 条链路 · 第 ${selection.page} 页 <button class="btn" data-page="prev" ${selection.page===1?'disabled':''}>上一页</button> <button class="btn" data-page="next" ${data.has_more?'':'disabled'}>下一页</button></p>`;
    html += rows(data.sources);
    root().innerHTML = html;
    root().querySelectorAll('details[data-task]').forEach(el=>el.addEventListener('toggle',()=>{if(el.open)opened.add(el.dataset.task);else opened.delete(el.dataset.task);}));
    root().querySelectorAll('select[data-select]').forEach(el => el.addEventListener('change', () => load({...selection,[el.dataset.select]:el.value,page:1})));
    root().querySelectorAll('button[data-page]').forEach(el => el.addEventListener('click', () => {
      if (!el.disabled) return load({...selection,page:selection.page+(el.dataset.page==='next'?1:-1)});
    }));
  }

  async function load(candidate = null) {
    if (!root() || navigating) return;
    navigating = candidate !== null;
    const current = ++sequence, chosen = candidate || selection;
    if (controller) controller.abort();
    const request = controller = new AbortController();
    const timeout = setTimeout(() => request.abort(), TIMEOUT_MS);
    root().querySelectorAll('select[data-select], button[data-page]').forEach(el=>{el.disabled=true;});
    try {
      const query = new URLSearchParams({service:chosen.service,phase:chosen.phase,page:String(chosen.page)});
      const response = await fetch('/infra/ecs-log-sources?'+query, {headers:{Accept:'application/json'},cache:'no-store',signal:request.signal});
      if (current !== sequence) return;
      if (response.status === 401) {location.href='/login';return;}
      const data = await response.json();
      if (current !== sequence) return;
      if (!response.ok) throw Error(data?.error || '日志来源状态读取失败');
      if (!valid(data, chosen.page)) throw Error('日志来源响应不完整');
      selection = {...chosen}; lastData = data;
      render(data);
    } catch (error) {
      if (current !== sequence) return;
      if (lastData) {
        render(lastData);
        root().insertAdjacentHTML('afterbegin','<p role="alert">刷新失败，以下是上次读取结果，不代表当前状态：'+esc(error.message)+'</p>');
      } else root().textContent='ECS 日志来源不可用：'+error.message+'。不能据此判断采集正常或任务已退出。';
    } finally {
      clearTimeout(timeout);
      if (current === sequence) {controller = null; navigating = false;}
    }
  }
  window.MonitorECSLogSources = {load};
})();
