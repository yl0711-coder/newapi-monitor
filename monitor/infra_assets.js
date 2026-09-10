/* Resource membership UI. No production controls; actions only change Monitor. */
(() => {
  'use strict';
  let generation = 0, busy = false, paging = false, lastData = null;
  const opened = new Set();
  const cursors = {active:[''], archived:['']};
  const REQUEST_TIMEOUT_MS = 15000;
  const labels = Object.freeze({
    reporting: '上报正常', report_lost: '上报中断，待排查', missing: '上次完整查询未发现，待核实',
    sample_unconfirmed: '正在上报，采样时间无效或已过期',
    stopped: '已停止，待归档', discovery_stale: '云端发现状态已过期', awaiting_report: '等待首次上报',
    cloud_running: 'AWS 确认运行（不代表日志采集成功）',
    cloud_idle: '服务存在，当前没有运行任务',
  });
  const escapeHTML = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const formatTime = seconds => seconds > 0 ? new Date(seconds * 1000).toLocaleString() : '尚无记录';
  const root = () => document.getElementById('infraAssets');
  const isRoot = () => typeof IS_ROOT !== 'undefined' && IS_ROOT;

  function buttons(asset, allowed) {
    if (!allowed) return '<span class="muted">仅超级管理员可操作</span>';
    const button = (action, label, disabled = false) => `<button type="button" class="btn" data-asset="${escapeHTML(asset.id)}" data-action="${action}" data-revision="${Number(asset.revision)}" ${disabled || busy ? 'disabled' : ''}>${label}</button>`;
    if (asset.state === 'active') return button('archive', '下线归档');
    return button('restore', '恢复', !asset.can_restore) + ' ' + button('remove', '移除', !asset.can_remove);
  }

  function assetRow(asset, allowed) {
    const recovery = asset.state === 'archived'
      ? (asset.can_restore ? '<span style="color:var(--green)">检测到新的实时更新，可恢复</span>' : '归档后尚未确认恢复')
      : escapeHTML(labels[asset.status] || '状态待确认');
    return `<tr><td><b>${escapeHTML(asset.resource)}</b><div class="muted" style="font-size:11px;overflow-wrap:anywhere">${escapeHTML(asset.platform)} · ${escapeHTML(asset.identity)}</div></td><td>${recovery}</td><td class="muted">主机上报：${escapeHTML(formatTime(asset.last_report))}<br>AWS 发现：${escapeHTML(formatTime(asset.last_discovered))}<br>AWS 校验：${escapeHTML(formatTime(asset.last_checked))}</td><td>${buttons(asset, allowed)}</td></tr>`;
  }

  function table(assets, allowed) {
    if (!assets.length) return '<p class="muted">暂无资源</p>';
    return '<div class="monitor-table-scroll"><table><thead><tr><th>资源 / 独立身份</th><th>当前观察</th><th>最后更新时间</th><th>操作</th></tr></thead><tbody>' + assets.map(a => assetRow(a, allowed)).join('') + '</tbody></table></div>';
  }

  function render(data, allowed = isRoot()) {
    lastData = data;
    const active = Array.isArray(data.active) ? data.active : [];
    const archived = Array.isArray(data.archived) ? data.archived : [];
    const services = new Map();
    const hosts = [];
    for (const asset of active) {
      if (asset.kind === 'ecs_task' || asset.kind === 'ecs_service') {
        const key = asset.parent || asset.resource;
        if (!services.has(key)) services.set(key, []);
        services.get(key).push(asset);
      } else hosts.push(asset);
    }
    const pager = kind => `<div class="muted" style="padding:8px 0">${kind==='active'?'当前资源':'归档资源'} · 第 ${cursors[kind].length} 页 <button class="btn" data-page="${kind}" data-direction="prev" ${busy||cursors[kind].length===1?'disabled':''}>上一页</button> <button class="btn" data-page="${kind}" data-direction="next" data-next="${escapeHTML(data[kind+'_next']||'')}" ${busy||!data[kind+'_next']?'disabled':''}>下一页</button></div>`;
    let html = pager('active') + table(hosts, allowed);
    for (const [name, assets] of [...services].sort(([a], [b]) => a.localeCompare(b))) {
      html += `<details data-group="${escapeHTML(name)}" ${opened.has(name) ? 'open' : ''}><summary style="cursor:pointer;padding:12px 0">${escapeHTML(name)} · 本页 ${assets.filter(a => a.kind === 'ecs_task').length} 个任务记录</summary>${table(assets, allowed)}</details>`;
    }
    html += `<details data-group="archive" ${opened.has('archive') ? 'open' : ''}><summary style="cursor:pointer;padding:12px 0">下线归档（${archived.length}）· 本页</summary><p class="muted">归档后停止该资源的基础设施告警；日志链路告警独立管理。收到归档后的有效实时更新才可恢复；移除后不再显示，但保留历史统计。</p>${pager('archived')}${table(archived, allowed)}</details>`;
    root().innerHTML = html;
    root().querySelectorAll('details[data-group]').forEach(el => el.addEventListener('toggle', () => {
      if (el.open) opened.add(el.dataset.group); else opened.delete(el.dataset.group);
    }));
    root().querySelectorAll('button[data-action]').forEach(el => el.addEventListener('click', () => action(el.dataset)));
    root().querySelectorAll('button[data-page]').forEach(el => el.addEventListener('click', () => {
      if (busy || paging) return;
      const candidate = {active:[...cursors.active], archived:[...cursors.archived]};
      const stack = candidate[el.dataset.page];
      if (el.dataset.direction==='prev' && stack.length>1) stack.pop();
      else if (el.dataset.direction==='next' && el.dataset.next && el.dataset.next!==stack.at(-1)) stack.push(el.dataset.next);
      else return;
      return load(candidate);
    }));
  }

  async function load(candidate = null) {
    // A timer refresh must not overtake a navigation or commit its cursor.
    if (paging) return;
    if (candidate) {
      paging = true;
      root().querySelectorAll('button[data-page]').forEach(el => {el.disabled = true;});
    }
    const current = ++generation;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const selected = candidate || cursors;
      const query = `?active_after=${encodeURIComponent(selected.active.at(-1))}&archived_after=${encodeURIComponent(selected.archived.at(-1))}`;
      const response = await fetch('/infra/assets'+query, {headers: {'Accept': 'application/json'}, cache: 'no-store', signal:controller.signal});
      if (response.status === 401) {location.href = '/login'; return;}
      const data = await response.json();
      if (current !== generation) return;
      if (!response.ok) throw new Error(data.error || '资源目录读取失败');
      if (!data || !Array.isArray(data.active) || !Array.isArray(data.archived)) throw new Error('资源目录响应格式错误');
      if (candidate) {
        cursors.active = candidate.active;
        cursors.archived = candidate.archived;
      }
      paging = false;
      render(data);
    } catch (error) {
      if (current === generation) {
        paging = false;
        if (candidate && lastData) {
          render(lastData);
          root().insertAdjacentHTML('afterbegin', '<p role="alert">翻页失败，仍显示原页：'+escapeHTML(error.message)+'</p>');
        } else root().textContent = '资源目录不可用：' + error.message + '。不能据此判断实例已下线。';
      }
    } finally {
      clearTimeout(timeout);
      if (current === generation) paging = false;
    }
  }

  async function action(dataset) {
    if (busy || paging || !isRoot()) return;
    const messages = {
      archive: '确认将此资源下线归档？只停止 Monitor 当前展示和该资源基础设施告警，不停止 AWS 实例、日志接收或日志链路告警。',
      restore: '确认恢复到当前监控列表？',
      remove: '确认从监控及归档列表移除？同一身份不会被自动重新加入；历史请求、消费、稳定性统计不删除。',
    };
    if (!messages[dataset.action] || !window.confirm(messages[dataset.action])) return;
    busy = true;
    ++generation;
    root().querySelectorAll('button[data-action]').forEach(el => {el.disabled = true;});
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    let confirmed = false;
    try {
      const response = await fetch('/infra/assets/action', {
        method: 'POST', headers: {'Accept':'application/json', 'Content-Type':'application/json'},
        body: JSON.stringify({id:dataset.asset, action:dataset.action, revision:Number(dataset.revision)}),
        signal: controller.signal,
      });
      if (response.status === 401) {location.href = '/login'; return;}
      const result = await response.json();
      if (!response.ok) {
        window.alert(result?.error || '操作未完成，请刷新核对后重试');
      } else if (result?.ok === true) confirmed = true;
      else throw new Error('操作响应格式错误');
    } catch (error) {
      // A timeout/lost ACK does not prove the server rolled back. Never
      // automatically retry a write; reconcile using the revisioned list.
      window.alert('操作结果暂未确认，请刷新核对，勿重复提交：' + error.message);
    } finally {
      clearTimeout(timeout);
      busy = false;
      await load();
    }
    // A committed action must not retain the UI lock while the independent
    // infrastructure dashboard refreshes (or its network request stalls).
    if (confirmed && typeof loadInfra === 'function') {
      Promise.resolve().then(() => loadInfra()).catch(() => {
        window.alert('资源操作已完成，但监控概览刷新失败，请刷新页面核对。');
      });
    }
  }

  window.MonitorInfraAssets = {load, render};
})();
