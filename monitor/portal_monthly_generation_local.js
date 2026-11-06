// Explicit opt-in generation UI, also reused by local acceptance. No amounts,
// publication flags or task IDs here; production wiring replaces the title.
(() => {
  const useCalendar = /*MONTHLY_CALENDAR_ENABLED*/ false;
  const panel = document.createElement('div');
  panel.id = 'monthlyGenerationLocal';
  panel.className = 'card';
  panel.style.cssText = 'padding:16px;margin-bottom:16px;display:none';
  panel.innerHTML = '<strong>生成账单 · 本机验收</strong><p>选择已结束的月份，点击生成后由后台处理，无需一直等待。生成成功后保存，可随时查看、下载；已有账单不重复生成。</p><button type="button" class="btn p" id="monthlyGenerate" disabled>生成账单</button> <button type="button" class="btn" id="monthlyGenerationCheck" disabled>刷新进度</button><p id="monthlyGenerationStatus" role="status" aria-live="polite"></p>';
  $('monthlyStatus').before(panel);
  const submit = $('monthlyGenerate'), check = $('monthlyGenerationCheck'), status = $('monthlyGenerationStatus');
  const messages = {
    available: '账单已生成并保存，可在列表中查看和下载。',
    not_prepared: '该月账单暂未准备好，请稍后查看。',
    not_requested: '该月尚未提交生成申请。',
    queued: '生成申请已排队。',
    processing: '账单正在生成，请稍后查看。',
    finalizing: '账单正在核对和保存，完成后可查看和下载。',
    pending_verification: '请在账单列表查看生成结果；如需帮助请联系客服。',
    needs_preparation: '该月账单暂未生成，请稍后查看或联系客服。',
    needs_review: '该月账单暂未生成，请联系客服协助处理。',
    temporarily_unavailable: '账单暂时未能生成，请稍后查看。'
  };
  let epoch = 0, selected = '', timer = null, controller = null;
  let state = '', busy = false, posting = false, polls = 0, suspended = false, refreshed = '';
  let calendar = null, calendarLoading = false, calendarFailed = false, calendarUnauthorized = false, calendarController = null, calendarEpoch = 0;
  const validMonth = value => typeof value === 'string' && /^\d{4}-(0[1-9]|1[0-2])$/.test(value) && value >= '2020-01';
  function calendarMessage() {
    if (calendarUnauthorized) return '登录状态已失效，请重新登录。';
    if (calendarFailed) return '暂时无法读取可生成月份，请点击刷新进度重试。';
    if (!calendar) return '正在读取可生成月份…';
    if (!calendar.months.length) return calendar.first_month > calendar.last_month ? '首份月度账单可于 ' + calendar.first_available_at + ' 起生成。' : '目前暂无开放生成的月份，已保存账单仍可查看和下载。';
    return $('monthlyMonth').value ? '所选月份不在可生成范围内，已保存账单仍可查看。' : '请选择一个已结束的月份。';
  }
  async function loadCalendar() {
    if (!useCalendar || calendar || calendarLoading || suspended || document.hidden) return;
    const token = ++calendarEpoch, ctrl = new AbortController();
    calendarController = ctrl; calendarLoading = true; calendarFailed = false; calendarUnauthorized = false; buttons();
    const timeout = setTimeout(() => ctrl.abort(), 10000);
    const current = () => token === calendarEpoch && !suspended && !document.hidden && $('monthlyView').style.display !== 'none' && $('monthlyListBar').style.display !== 'none';
    try {
      const response = await fetch('/api/monthly-statement/calendar', {method: 'GET', credentials: 'same-origin', cache: 'no-store', signal: ctrl.signal});
      if (!current()) return;
      if (response.status === 401) { calendarFailed = true; calendarUnauthorized = true; return; }
      if (!response.ok) throw new Error('calendar unavailable');
      const value = await response.json();
      if (!current()) return;
      if (!validMonth(value.first_month) || !validMonth(value.last_month) || !/^\d{4}-(0[1-9]|1[0-2])-01$/.test(value.first_available_at) || !Array.isArray(value.months) || value.months.length > 120) throw new Error('invalid calendar');
      let previous = '';
      for (const month of value.months) {
        if (!validMonth(month) || month < value.first_month || month > value.last_month || month <= previous) throw new Error('invalid calendar range');
        previous = month;
      }
      calendar = value;
      $('monthlyMonth').min = value.first_month;
      $('monthlyMonth').max = value.last_month;
      $('monthlyMonth').monthlyGenerationMaxMonth = value.last_month;
      if (!value.months.includes($('monthlyMonth').value)) $('monthlyMonth').value = value.months.at(-1) || '';
      // Updating the input in code emits no change event. Refresh the saved
      // list explicitly; generation amounts never come from this metadata.
      if (typeof loadMonthlyList === 'function') void loadMonthlyList();
    } catch (_) {
      if (current()) calendarFailed = true;
    } finally {
      clearTimeout(timeout);
      if (token === calendarEpoch) { calendarLoading = false; calendarController = null; sync(); }
    }
  }
  function activeMonth() {
    if (suspended || document.hidden || $('monthlyView').style.display === 'none' || $('monthlyListBar').style.display === 'none') return '';
    const month = $('monthlyMonth').value;
    return validMonth(month) && (useCalendar ? calendar?.months.includes(month) : month <= lastClosedMonth()) ? month : '';
  }
  function buttons() {
    submit.disabled = !selected || busy || posting || state !== 'not_requested';
    check.disabled = busy || posting || (useCalendar && !calendar ? calendarLoading : !selected);
  }
  function stop() {
    epoch++;
    clearTimeout(timer); timer = null;
    if (controller) controller.abort();
    controller = null; busy = false; state = ''; polls = 0; refreshed = '';
  }
  function sync() {
    const month = activeMonth();
    panel.style.display = !suspended && !document.hidden && $('monthlyView').style.display !== 'none' && $('monthlyListBar').style.display !== 'none' ? 'block' : 'none';
    if (useCalendar && !calendar && !calendarLoading && !calendarFailed && panel.style.display === 'block') void loadCalendar();
    if (month === selected) { if (!month) status.textContent = useCalendar ? calendarMessage() : '请选择一个已结束的月份。'; buttons(); return; }
    stop(); selected = month; buttons();
    status.textContent = month ? '正在读取生成进度…' : (useCalendar ? calendarMessage() : '请选择一个已结束的月份。');
    if (month) void request(false);
  }
  async function request(write) {
    if (!selected || selected !== activeMonth() || busy || posting || (write && state !== 'not_requested')) return;
    clearTimeout(timer); timer = null;
    const month = selected, version = epoch, ctrl = new AbortController();
    controller = ctrl; busy = true; if (write) posting = true;
    buttons(); status.textContent = write ? '正在提交申请…' : '正在读取生成进度…';
    let timedOut = false;
    const timeout = setTimeout(() => { timedOut = true; ctrl.abort(); }, 10000);
    const current = () => version === epoch && month === activeMonth();
    try {
      const url = '/api/monthly-statement/generation' + (write ? '' : '?month=' + encodeURIComponent(month));
      const options = {method: write ? 'POST' : 'GET', credentials: 'same-origin', cache: 'no-store', signal: ctrl.signal};
      if (write) { options.headers = {'Content-Type': 'application/json', 'X-Usage-Bill-Request': '1'}; options.body = JSON.stringify({month}); }
      const response = await fetch(url, options);
      if (!current()) return;
      if (response.status === 401) { state = ''; status.textContent = '登录状态已失效，请重新登录。'; return; }
      if (!response.ok) {
        state = '';
        // Render only known business codes, never upstream/raw error text.
        // A month outside prepared rollout is not a network/server outage.
        if (response.status === 409) {
          const problem = await response.json();
          if (!current()) return;
          if (problem.code === 'not_prepared' || problem.code === 'needs_review') {
            state = problem.code; status.textContent = messages[state]; return;
          }
        }
        status.textContent = response.status === 429 ? '服务暂忙，已停止自动刷新；请稍后刷新进度。' : '暂未确认生成状态，请刷新进度；已发布账单不受影响。';
        return;
      }
      const data = await response.json();
      if (!current()) return;
      if (data.month !== month || !Object.hasOwn(messages, data.state)) throw new Error('invalid status');
      state = data.state; status.textContent = messages[state];
      if (state === 'available' && refreshed !== month) {
        // The list/detail/download endpoints remain authoritative for amounts,
        // scope and version. Never enable download from a task response alone.
        refreshed = month;
        if (typeof loadMonthlyList === 'function') void loadMonthlyList();
      }
      if (state === 'queued' || state === 'processing' || state === 'finalizing') {
        if (++polls < 120) timer = setTimeout(() => { timer = null; void request(false); }, 5000);
        else status.textContent += ' 已暂停自动刷新，可手动刷新进度。';
      }
    } catch (_) {
      if (current()) { state = ''; status.textContent = timedOut ? '请求超时，请刷新进度确认是否已受理；不会自动重复提交。' : '暂时无法读取进度，请稍后刷新；不会自动重复提交。'; }
    } finally {
      clearTimeout(timeout);
      if (write) posting = false;
      if (version === epoch) { busy = false; controller = null; }
      buttons();
      if (write && version !== epoch && selected && !busy) void request(false);
    }
  }
  submit.addEventListener('click', () => { void request(true); });
  check.addEventListener('click', () => {
    if (useCalendar) {
      if (busy || posting || calendarLoading) return;
      // Also refresh the server month ceiling after a month boundary. Merely
      // polling an old month's task must not require a full page reload to
      // discover a newly closed month. This never submits a generation job.
      calendar = null; calendarFailed = false; stop(); selected = '';
      status.textContent = '正在读取可生成月份…';
      void loadCalendar(); return;
    }
    polls = 0; void request(false);
  });
  $('monthlyMonth').addEventListener('change', sync);
  $('monthlyReset').addEventListener('click', sync);
  // Observe only navigation bars, not our own status rendering.
  const observer = new MutationObserver(sync);
  for (const id of ['monthlyView', 'monthlyListBar']) observer.observe($(id), {attributes: true, attributeFilter: ['style']});
  document.addEventListener('visibilitychange', sync);
  window.addEventListener('pagehide', () => { suspended = true; calendarEpoch++; if (calendarController) calendarController.abort(); calendarController = null; calendarLoading = false; calendarFailed = false; calendarUnauthorized = false; calendar = null; stop(); selected = ''; buttons(); });
  window.addEventListener('pageshow', () => { suspended = false; sync(); });
  sync();
})();
