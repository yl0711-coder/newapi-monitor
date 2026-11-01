import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../../monitor/portal_monthly_generation_local.js', import.meta.url), 'utf8');
const flush = async () => { for (let i = 0; i < 8; i++) await Promise.resolve(); };
function fixture(month = '2026-09', calendar = false) {
  const nodes = {}, calls = [], timers = new Map(), docEvents = {}, winEvents = {}, refreshed = [];
  let next = 0, observer;
  const node = id => nodes[id] ||= {style: {display: 'block'}, value: '', disabled: false, textContent: '', events: {}, before() {}, addEventListener(k, fn) { this.events[k] = fn; }};
  node('monthlyMonth').value = month;
  const document = {hidden: false, createElement: () => node('panel'), addEventListener(k, fn) { docEvents[k] = fn; }};
  const ctx = vm.createContext({document, window: {addEventListener(k, fn) { winEvents[k] = fn; }}, $: node,
    lastClosedMonth: () => '2026-09', AbortController,
    loadMonthlyList: () => { refreshed.push(node('monthlyMonth').value); },
    MutationObserver: class {constructor(fn) { observer = fn; } observe() {}},
    setTimeout(fn, delay) { const id = ++next; timers.set(id, {fn, delay}); return id; }, clearTimeout(id) { timers.delete(id); },
    fetch(url, options) { return new Promise((resolve, reject) => calls.push({url, options, resolve, reject})); }
  });
  vm.runInContext(calendar ? source.replace('/*MONTHLY_CALENDAR_ENABLED*/ false', '/*MONTHLY_CALENDAR_ENABLED*/ true') : source, ctx);
  const reply = async (index, state, status = 200, returnedMonth) => {
    const call = calls[index];
    const m = returnedMonth || (call.options.body ? JSON.parse(call.options.body).month : new URL(call.url, 'http://local').searchParams.get('month'));
    call.resolve({status, ok: status >= 200 && status < 300, json: async () => ({month: m, state, message: '<sensitive raw error>'})}); await flush();
  };
  const fire = async delay => { const pair = [...timers].find(([, t]) => t.delay === delay); assert.ok(pair, `timer ${delay}`); timers.delete(pair[0]); pair[1].fn(); await flush(); };
  return {nodes, node, calls, timers, document, docEvents, winEvents, refreshed, reply, fire, observe: () => observer()};
}

test('read first; only explicit click posts once; draft readiness never publishes or downloads', async () => {
  const f = fixture(); assert.equal(f.calls.length, 1); assert.equal(f.calls[0].options.method, 'GET');
  await f.reply(0, 'not_requested'); assert.equal(f.node('monthlyGenerate').disabled, false);
  f.node('monthlyDownload').disabled = true; f.node('monthlyContent').textContent = 'published content';
  f.node('monthlyGenerate').events.click(); f.node('monthlyGenerate').events.click();
  assert.equal(f.calls.length, 2); assert.equal(f.calls[1].options.body, '{"month":"2026-09"}');
  assert.equal(f.calls[1].options.headers['X-Usage-Bill-Request'], '1');
  await f.reply(1, 'queued', 202); await f.fire(5000); await f.reply(2, 'pending_verification');
  assert.equal(f.node('monthlyGenerationStatus').textContent, '请在账单列表查看生成结果；如需帮助请联系客服。');
  assert.equal(f.node('monthlyDownload').disabled, true); assert.equal(f.node('monthlyContent').textContent, 'published content');
  assert.equal(f.timers.size, 0); assert.equal(f.node('monthlyGenerate').disabled, true);
});

test('completed task does not contradict an independently published bill or change its availability', async () => {
  const f = fixture();
  f.node('monthlyDownload').disabled = false;
  f.node('monthlyContent').textContent = '可查看 / 下载';
  await f.reply(0, 'pending_verification');
  assert.match(f.node('monthlyGenerationStatus').textContent, /账单列表查看生成结果/);
  assert.doesNotMatch(f.node('monthlyGenerationStatus').textContent, /待核验后可查看/);
  assert.doesNotMatch(f.node('monthlyGenerationStatus').textContent, /尚未完成|暂不能|暂未生成/);
  assert.equal(f.node('monthlyDownload').disabled, false);
  assert.equal(f.node('monthlyContent').textContent, '可查看 / 下载');
  assert.equal(f.calls.length, 1);
  assert.equal(f.timers.size, 0);
});

test('saved bill refreshes the list once, never regenerates or enables download from task state', async () => {
  const f = fixture();
  f.node('monthlyDownload').disabled = true;
  await f.reply(0, 'available');
  assert.deepEqual(f.refreshed, ['2026-09']);
  assert.match(f.node('monthlyGenerationStatus').textContent, /已生成并保存/);
  assert.equal(f.node('monthlyGenerate').disabled, true);
  assert.equal(f.node('monthlyDownload').disabled, true);
  assert.equal(f.timers.size, 0);
  f.node('monthlyGenerate').events.click();
  assert.equal(f.calls.length, 1);
  f.node('monthlyGenerationCheck').events.click(); await f.reply(1, 'available');
  assert.deepEqual(f.refreshed, ['2026-09']);
  assert.ok(f.calls.every(c => c.options.method === 'GET'));
});

test('background completion reloads only the selected month, stale completion never reloads', async () => {
  const f = fixture(); await f.reply(0, 'not_requested');
  f.node('monthlyGenerate').events.click(); await f.reply(1, 'queued', 202);
  await f.fire(5000); await f.reply(2, 'available');
  assert.deepEqual(f.refreshed, ['2026-09']); assert.equal(f.timers.size, 0);
  f.node('monthlyGenerationCheck').events.click();
  f.node('monthlyMonth').value = '2026-08'; f.node('monthlyMonth').events.change();
  await f.reply(3, 'available'); assert.deepEqual(f.refreshed, ['2026-09']);
  await f.reply(4, 'not_requested');
  assert.equal(f.node('monthlyGenerate').disabled, false);
});

test('automatic verification keeps polling; paused verification never advertises a saved bill', async () => {
  const f = fixture();
  await f.reply(0, 'finalizing');
  assert.match(f.node('monthlyGenerationStatus').textContent, /核对和保存/);
  assert.equal(f.node('monthlyGenerate').disabled, true);
  assert.deepEqual(f.refreshed, []);
  await f.fire(5000); await f.reply(1, 'needs_review');
  assert.equal(f.timers.size, 0);
  assert.deepEqual(f.refreshed, []);
  f.node('monthlyGenerationCheck').events.click(); await f.reply(2, 'available');
  assert.deepEqual(f.refreshed, ['2026-09']);
});

test('month changes abort stale reads and stale post cannot update new month or duplicate admission', async () => {
  const f = fixture(); f.node('monthlyMonth').value = '2026-08'; f.node('monthlyMonth').events.change();
  assert.equal(f.calls[0].options.signal.aborted, true); await f.reply(1, 'not_requested'); await f.reply(0, 'needs_review');
  assert.match(f.node('monthlyGenerationStatus').textContent, /尚未/);
  f.node('monthlyGenerate').events.click();
  f.node('monthlyMonth').value = '2026-07'; f.node('monthlyMonth').events.change();
  assert.equal(f.calls[2].options.signal.aborted, true); f.node('monthlyGenerate').events.click(); assert.equal(f.calls.length, 3);
  await f.reply(2, 'queued', 202); assert.equal(f.calls.length, 4); assert.match(f.calls[3].url, /2026-07/);
  await f.reply(3, 'not_requested'); assert.equal(f.node('monthlyGenerate').disabled, false);
});

test('leaving view, opening detail, hiding tab and pagehide stop polling; returning only reads', async () => {
  for (const kind of ['view', 'detail', 'hidden', 'pagehide']) {
    const f = fixture(); await f.reply(0, 'processing');
    if (kind === 'view') { f.node('monthlyView').style.display = 'none'; f.observe(); }
    if (kind === 'detail') { f.node('monthlyListBar').style.display = 'none'; f.observe(); }
    if (kind === 'hidden') { f.document.hidden = true; f.docEvents.visibilitychange(); }
    if (kind === 'pagehide') f.winEvents.pagehide();
    assert.equal(f.timers.size, 0);
    f.node('monthlyView').style.display = f.node('monthlyListBar').style.display = 'block'; f.document.hidden = false;
    if (kind === 'pagehide') f.winEvents.pageshow(); else f.observe();
    assert.equal(f.calls.length, 2); assert.equal(f.calls[1].options.method, 'GET');
  }
});

test('errors stop automatic retries; unknown or mismatched states never enable generation', async () => {
  for (const code of [401, 409, 429, 503]) {
    const f = fixture(); await f.reply(0, 'not_requested', code);
    assert.equal(f.timers.size, 0); assert.equal(f.node('monthlyGenerate').disabled, true);
    assert.doesNotMatch(f.node('monthlyGenerationStatus').textContent, /sensitive/);
    f.node('monthlyGenerationCheck').events.click(); assert.equal(f.calls.length, 2);
  }
  for (const [state, month] of [['published', '2026-09'], ['__proto__', '2026-09'], ['not_requested', '2026-08']]) {
    const f = fixture(); await f.reply(0, state, 200, month); assert.equal(f.node('monthlyGenerate').disabled, true); assert.equal(f.timers.size, 0);
  }
});

test('lost submission response never automatically POSTs again; status check recovers', async () => {
  const f = fixture(); await f.reply(0, 'not_requested'); f.node('monthlyGenerate').events.click();
  await f.fire(10000); assert.equal(f.calls[1].options.signal.aborted, true);
  f.calls[1].reject(new Error('aborted')); await flush(); assert.match(f.node('monthlyGenerationStatus').textContent, /超时/);
  assert.equal(f.timers.size, 0); assert.equal(f.calls.length, 2);
  f.node('monthlyGenerationCheck').events.click(); await f.reply(2, 'queued'); assert.equal(f.calls[2].options.method, 'GET');
});

test('closed-month validation and bounded polling', async () => {
  for (const month of ['', '2026-10', '2019-12', '2026-9', '2026-00', '2025-13']) { const f = fixture(month); assert.equal(f.calls.length, 0); assert.equal(f.node('monthlyGenerate').disabled, true); }
  const f = fixture(); for (let i = 0; i < 120; i++) { await f.reply(i, 'processing'); if (i < 119) await f.fire(5000); }
  assert.equal(f.calls.length, 120); assert.equal(f.timers.size, 0); assert.match(f.node('monthlyGenerationStatus').textContent, /暂停自动/);
});

test('customer copy matches the HTTP allowlist and paused bills never imply success', async () => {
  const server = fs.readFileSync(new URL('../../monitor/portal_monthly_generation_http.go', import.meta.url), 'utf8');
  const states = ['available', 'not_prepared', 'not_requested', 'queued', 'processing', 'finalizing', 'pending_verification', 'needs_preparation', 'needs_review', 'temporarily_unavailable'];
  for (const state of states) {
    const expected = server.match(new RegExp(`case "${state}":\\s*return "([^"\\n]+)"`))?.[1];
    assert.ok(expected, `missing server message: ${state}`);
    const f = fixture();
    f.node('monthlyDownload').disabled = true;
    f.node('monthlyContent').textContent = 'existing saved bill';
    await f.reply(0, state);
    assert.equal(f.node('monthlyGenerationStatus').textContent, expected, state);
    // This is also asserted against the real authenticated first GET in the
    // Go customer journey. Missing inputs alone must not become a blanket
    // client-side permission to generate an unsupported/withdrawn month.
    assert.equal(f.node('monthlyGenerate').disabled, state !== 'not_requested', state);
    assert.doesNotMatch(expected, /生成任务|尚未发布|未发布的账单|重新准备/);
    if (['pending_verification', 'needs_preparation', 'needs_review'].includes(state)) {
      assert.doesNotMatch(expected, /已生成|已完成|可查看和下载|正在/);
      assert.equal(f.node('monthlyGenerate').disabled, true);
      assert.equal(f.node('monthlyDownload').disabled, true);
      assert.equal(f.node('monthlyGenerationCheck').disabled, false);
      assert.equal(f.timers.size, 0);
      assert.equal(f.refreshed.length, 0);
      assert.equal(f.node('monthlyContent').textContent, 'existing saved bill');
      assert.ok(f.calls.every(c => c.options.method === 'GET'));
    }
  }
});

test('known unprepared/review conflicts show safe business status, not a server outage', async () => {
  for (const code of ['not_prepared', 'needs_review']) {
    const f = fixture(); await f.reply(0, 'not_requested');
    f.node('monthlyContent').textContent = 'existing saved bill';
    f.node('monthlyDownload').disabled = false;
    f.node('monthlyGenerate').events.click();
    f.calls[1].resolve({status: 409, ok: false, json: async () => ({code, error: '<private source path / credential>'})});
    await flush();
    assert.match(f.node('monthlyGenerationStatus').textContent, /暂未准备好|联系客服/);
    assert.doesNotMatch(f.node('monthlyGenerationStatus').textContent, /暂未确认|private|credential/);
    assert.equal(f.node('monthlyGenerate').disabled, true);
    assert.equal(f.node('monthlyContent').textContent, 'existing saved bill');
    assert.equal(f.node('monthlyDownload').disabled, false);
    assert.equal(f.calls.length, 2);
    assert.equal(f.timers.size, 0);
  }
});

test('unknown conflict details are not reflected and a stale conflict cannot replace another month', async () => {
  const f = fixture(); await f.reply(0, 'not_requested');
  f.node('monthlyGenerate').events.click();
  f.calls[1].resolve({status: 409, ok: false, json: async () => ({code: 'unknown', error: 'private'})});
  await flush();
  assert.match(f.node('monthlyGenerationStatus').textContent, /暂未确认/);
  assert.doesNotMatch(f.node('monthlyGenerationStatus').textContent, /private/);
  f.node('monthlyGenerationCheck').events.click();
  f.node('monthlyMonth').value = '2026-08'; f.node('monthlyMonth').events.change();
  await f.reply(3, 'not_requested');
  f.calls[2].resolve({status: 409, ok: false, json: async () => ({code: 'not_prepared'})});
  await flush();
  assert.equal(f.node('monthlyGenerate').disabled, false);
  assert.match(f.node('monthlyGenerationStatus').textContent, /尚未提交/);
});

const septemberCalendar = {first_month: '2026-09', last_month: '2026-09', first_available_at: '2026-10-01', months: ['2026-09']};
async function calendarReply(f, index, value = septemberCalendar, status = 200) {
  f.calls[index].resolve({status, ok: status === 200, json: async () => value});
  await flush();
}

test('registration calendar chooses latest closed month then reads real generation state before enabling a click', async () => {
  const f = fixture('', true);
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].url, '/api/monthly-statement/calendar');
  assert.equal(f.node('monthlyGenerate').disabled, true);
  await calendarReply(f, 0);
  assert.equal(f.node('monthlyMonth').min, '2026-09');
  assert.equal(f.node('monthlyMonth').max, '2026-09');
  assert.equal(f.node('monthlyMonth').value, '2026-09');
  assert.equal(f.calls.length, 2);
  assert.equal(f.calls[1].options.method, 'GET');
  assert.equal(f.node('monthlyGenerate').disabled, true);
  await f.reply(1, 'not_requested');
  assert.equal(f.node('monthlyGenerate').disabled, false);
  f.node('monthlyGenerate').events.click(); f.node('monthlyGenerate').events.click();
  assert.equal(f.calls.length, 3);
  assert.equal(f.calls[2].options.body, '{"month":"2026-09"}');
  await f.reply(2, 'queued', 202);
  await f.fire(5000); await f.reply(3, 'processing');
  assert.equal(f.calls.filter(c => c.url === '/api/monthly-statement/calendar').length, 1);
});

test('new current-month customer sees first bill date and never gets a pre-registration zero bill', async () => {
  const f = fixture('', true);
  await calendarReply(f, 0, {first_month: '2026-10', last_month: '2026-09', first_available_at: '2026-11-01', months: []});
  assert.match(f.node('monthlyGenerationStatus').textContent, /2026-11-01/);
  assert.equal(f.node('monthlyGenerate').disabled, true);
  assert.equal(f.calls.length, 1);
  f.node('monthlyMonth').value = '2026-08'; f.node('monthlyMonth').events.change();
  f.node('monthlyGenerate').events.click();
  assert.equal(f.calls.length, 1);
});

test('registration range rejects earlier, current and rollout-gap months without trusting input min/max', async () => {
  const f = fixture('2026-09', true);
  await calendarReply(f, 0); await f.reply(1, 'not_requested');
  for (const month of ['2026-08', '2026-10', '2026-11']) {
    f.node('monthlyMonth').value = month; f.node('monthlyMonth').events.change();
    assert.equal(f.node('monthlyGenerate').disabled, true);
    f.node('monthlyGenerate').events.click();
    assert.equal(f.calls.length, 2);
  }
});

test('calendar failure never guesses dates, loops, posts or leaks source errors and can be refreshed', async () => {
  const f = fixture('', true);
  await calendarReply(f, 0, {error: 'private SQL/source/path'}, 503);
  assert.equal(f.calls.length, 1);
  assert.equal(f.node('monthlyGenerate').disabled, true);
  assert.equal(f.node('monthlyGenerationCheck').disabled, false);
  assert.doesNotMatch(f.node('monthlyGenerationStatus').textContent, /SQL|source|path/);
  f.node('monthlyGenerationCheck').events.click();
  await calendarReply(f, 1); await f.reply(2, 'not_requested');
  assert.equal(f.node('monthlyGenerate').disabled, false);
  const expired = fixture('', true);
  await calendarReply(expired, 0, {error: 'private error'}, 401);
  assert.match(expired.node('monthlyGenerationStatus').textContent, /重新登录/);
  assert.equal(expired.node('monthlyGenerate').disabled, true);
});

test('invalid or stale calendar never enables generation or navigates out of an opened bill', async () => {
  for (const value of [{...septemberCalendar, months: ['2026-09', '2026-09']}, {...septemberCalendar, months: ['2026-08']}, {...septemberCalendar, first_available_at: '<script>'}]) {
    const f = fixture('', true); await calendarReply(f, 0, value);
    assert.equal(f.node('monthlyGenerate').disabled, true); assert.equal(f.calls.length, 1);
  }
  const f = fixture('', true);
  f.node('monthlyListBar').style.display = 'none'; f.observe();
  await calendarReply(f, 0);
  assert.equal(f.calls.length, 1); assert.equal(f.refreshed.length, 0);
  f.node('monthlyListBar').style.display = 'block'; f.observe();
  assert.equal(f.calls.length, 2);
});

test('refresh discovers the new server month without a reload, new generation or stale task polling', async () => {
  const f = fixture('2026-09', true);
  await calendarReply(f, 0); await f.reply(1, 'not_requested');
  f.node('monthlyGenerationCheck').events.click();
  assert.equal(f.calls[2].url, '/api/monthly-statement/calendar');
  await calendarReply(f, 2, {...septemberCalendar, last_month: '2026-10', months: ['2026-09', '2026-10']});
  await f.reply(3, 'not_requested');
  assert.equal(f.node('monthlyMonth').max, '2026-10');
  assert.equal(f.node('monthlyMonth').value, '2026-09');
  f.node('monthlyMonth').value = '2026-10'; f.node('monthlyMonth').events.change();
  await f.reply(4, 'not_requested');
  assert.equal(f.node('monthlyGenerate').disabled, false);
  assert.ok(f.calls.every(c => c.options.method === 'GET'));
});
