// 真实 Chrome/CDP 验收本轮两项改动：FRT 平均值阈值筛选、两页紧凑时间选择器、
// Token 页简化与空态具体原因。接口全部为回环合成数据，不访问 8204、线上或真实日志。
// 用法: node dev/test-frt-threshold-token-simplify-browser.mjs --browser <chrome> --out <目录>
import assert from 'node:assert/strict';
import {readFileSync, writeFileSync, mkdirSync, mkdtempSync} from 'node:fs';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {once} from 'node:events';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2), option = name => args[args.indexOf(name) + 1];
assert.ok(args.includes('--browser') && args.includes('--out'), '--browser 与 --out 必填');
const out = path.resolve(option('--out'));
mkdirSync(out, {recursive: true});
const profile = mkdtempSync(path.join(out, 'profile-'));
const browserTemp = path.join(out, 'temp');
mkdirSync(browserTemp, {recursive: true});

const html = readFileSync(path.join(root, 'monitor/page.html'), 'utf8');
const embedSource = readFileSync(path.join(root, 'monitor/server.go'), 'utf8');
const embedMap = new Map([...embedSource.matchAll(/\/\/go:embed ([^\r\n]+)\r?\nvar (\w+)/g)].map(m => [m[2], m[1]]));
const assets = new Map([...embedSource.matchAll(/r\.GET\("([^"\n]+\.(?:js|css))",[\s\S]*?c\.Data\(http\.StatusOK, "[^"]+", (?:\[\]byte\()?([\w]+)\)?\)/g)].map(m => [m[1], embedMap.get(m[2])]));

const requests = [], checks = [], exceptions = [], external = [], consoleErrors = [];
const end = Date.UTC(2026, 9, 16, 8, 20) / 1000;

// 三个渠道：均值 5 秒 / 1 秒 / 不可计算。阈值筛选必须据此严格区分。
const metric = (meanMs, reason = '') => ({
  mean_ms: meanMs, mean_coverage_complete: meanMs != null,
  mean_unavailable_reason: meanMs == null ? reason : '',
  observed: 100, p50_ms: 500, p95_ms: 9000, p99_ms: 9500, max_ms: 9900,
  over_3s: 10, over_3s_pct: 10, coverage_complete: true, unavailable_reason: '',
});
const allChannels = [
  {channel_id: 7, name: 'slow-channel', mean: 5000},
  {channel_id: 8, name: 'fast-channel', mean: 1000},
  {channel_id: 9, name: 'unknown-mean-channel', mean: null},
];
function frtReport(url) {
  const raw = url.searchParams.get('mean_gt_s');
  const applied = raw !== null && raw !== '';
  const thresholdMs = applied ? Math.round(Number(raw) * 1000) : null;
  let excluded = 0;
  let picked = allChannels;
  if (applied) {
    picked = [];
    for (const c of allChannels) {
      if (c.mean == null) {excluded++; continue;}
      if (c.mean > thresholdMs) picked.push(c);
    }
  }
  const to = new Date(end * 1000).toISOString();
  return {
    filter: {
      channel_id: null, group: url.searchParams.get('group'), model: url.searchParams.get('model'),
      mean_gt_ms: thresholdMs,
    },
    range: {mode: 'rolling', requested_from: new Date((end - 86400) * 1000).toISOString(), requested_to: to,
      available_through: to, retained_from: '2026-10-06T16:00:00Z', max_days: 7, today_open: false, has_window: true, note: ''},
    window: url.searchParams.get('window') || '24h',
    from: new Date((end - 86400) * 1000).toISOString(), to, target: to, generated_at: to, lag_seconds: 180,
    coverage: {status: 'complete', complete: true, from: null, through: null, note: '本地覆盖已核验。'},
    mean_filter_applied: applied, mean_filter_excluded: excluded,
    channels: picked.map(c => ({
      channel_id: c.channel_id, name: c.name,
      frt: metric(c.mean, 'historical_sum_unavailable'),
      groups: [{group: 'actual-group', frt: metric(c.mean, 'historical_sum_unavailable'),
        models: [{model: 'fixture-model', frt: metric(c.mean, 'historical_sum_unavailable')}]}],
    })),
  };
}

// Token 场景：采集未启用（空结果），用于验空态原因。
let tokenScenario = 'facts-disabled';
const tokenHealth = () => ({
  'facts-disabled': {enabled: false, source_available: false, state: {}, facts: 0, pending_links: 0, pending_windows: 0, link_states: {}, gap_reasons: []},
  'source-down': {enabled: true, source_available: false, state: {}, facts: 0, pending_links: 0, pending_windows: 0, link_states: {}, gap_reasons: []},
  'all-on': {enabled: true, source_available: true, state: {}, facts: 5, pending_links: 0, pending_windows: 0, link_states: {}, gap_reasons: []},
}[tokenScenario]);
const tokenDetectionHealth = () => ({
  enabled: tokenScenario === 'all-on', policy_configured: true,
  state: {}, pending_sample_count: 0, pending_sample_limit: 50, active_references: 0, deferred_requests: 0,
});
const emptyTokenReport = () => ({
  range: {from: end - 86400, to: end, source_epoch: 'epoch'},
  summary: {unique_candidate_dimensions: {customers: 0, channels: 0, groups: 0, models: 0},
    source_complete: true, unprocessed_requests: 0, observed_results: 0, candidate_requests: 0,
    amounts: {}, unavailable_by_method_and_reason: {}, gaps: []},
  analysis: {status_counts: {candidate: 0, insufficient: 0, not_hit: 0},
    calculable_candidates: {historical_formula_v1: 0, input_upper_reference_v1: 0}, breakdown: []},
  items: [], has_more: false, generated_at: end,
});

function json(res, status, data) {
  res.writeHead(status, {'Content-Type': 'application/json', 'Cache-Control': 'no-store'});
  res.end(JSON.stringify(data));
}
const server = createServer((req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1');
  requests.push({method: req.method, path: url.pathname, query: url.search});
  // 真实路由里 health / access 是 GET，只有 query / detail / exports 是 POST。
  if (req.method === 'POST') {
    if (url.pathname === '/anomalies/token/detection/query') {json(res, 200, emptyTokenReport()); return;}
    json(res, 405, {error: 'read only fixture'}); return;
  }
  if (url.pathname === '/anomalies/token/health') {json(res, 200, tokenHealth()); return;}
  if (url.pathname === '/anomalies/token/detection/health') {json(res, 200, tokenDetectionHealth()); return;}
  if (url.pathname === '/anomalies/token/notifications/health') {
    json(res, 200, {evaluation_enabled: false, delivery_enabled: false, state: {}, counts: [], recent: []}); return;
  }
  if (url.pathname === '/anomalies/token/access') {
    json(res, 200, {schema_version: 'token-delivery-v1', is_root: false, csrf_token: 'x', export_enabled: false,
      export_configured: false, max_requests: 100, max_bytes: 1024, ttl_seconds: 3600, export_targets: []}); return;
  }
  if (url.pathname === '/') {res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'}); res.end(html); return;}
  if (assets.has(url.pathname)) {
    res.writeHead(200, {'Content-Type': url.pathname.endsWith('.css') ? 'text/css' : 'application/javascript'});
    res.end(readFileSync(path.join(root, 'monitor', assets.get(url.pathname)))); return;
  }
  if (url.pathname === '/me') {json(res, 200, {name: '本地合成', is_root: false}); return;}
  if (url.pathname === '/api/brand') {json(res, 200, {name: '本地合成验收'}); return;}
  if (url.pathname === '/data') {json(res, 200, {enabled: false}); return;}
  if (url.pathname === '/model-statistics/frt') {json(res, 200, frtReport(url)); return;}
  if (url.pathname === '/model-statistics/report') {
    const to = new Date(end * 1000).toISOString();
    json(res, 200, {range: {mode: 'rolling', requested_from: new Date((end - 86400) * 1000).toISOString(),
      requested_to: to, available_through: to, retained_from: '2026-10-06T16:00:00Z', max_days: 7, today_open: false, has_window: true, note: ''},
      window: '24h', generated_at: end, from_ts: end - 86400, to_ts: end, target_ts: end, lag_seconds: 0, group_count: 1,
      source: {facts_complete: true, requests_complete: true, frt_complete: true, coverage_status: 'complete', note: ''},
      models: [{model: 'fixture-model', requests: 100, routed_requests: 100, unavailable_channel_requests: 0,
        groups: [{group: 'actual-group', requests: 100, routed_requests: 100, unavailable_channel_requests: 0,
          channels: [{channel_id: 7, channel_name: 'slow-channel', requests: 100, routed_requests: 100, unavailable_channel_requests: 0}],
          customers: [{customer_id: 31, customer_name: 'fixture-customer', requests: 7}]}]}]});
    return;
  }
  json(res, 200, {});
});
server.listen(0, '127.0.0.1');
await once(server, 'listening');
const origin = 'http://127.0.0.1:' + server.address().port;

const browser = spawn(option('--browser'), ['--headless=new', '--remote-debugging-pipe', '--no-first-run',
  '--no-default-browser-check', '--disable-background-networking', '--disable-component-update', '--disable-sync',
  '--disable-default-apps', '--disable-client-side-phishing-detection', '--metrics-recording-only', '--lang=zh-CN',
  '--window-size=1280,950', `--user-data-dir=${profile}`, 'about:blank'],
  {stdio: ['ignore', 'pipe', 'pipe', 'pipe', 'pipe'], windowsHide: true, env: {...process.env, TEMP: browserTemp, TMP: browserTemp}});

let seq = 0, received = '', sessionId;
const pending = new Map();
browser.stderr.on('data', () => {});
browser.stdio[4].on('data', chunk => {
  received += chunk.toString();
  for (let at; (at = received.indexOf('\0')) >= 0;) {
    const t = received.slice(0, at); received = received.slice(at + 1);
    if (!t) continue;
    const msg = JSON.parse(t), task = pending.get(msg.id);
    if (task) {pending.delete(msg.id); clearTimeout(task.timer); msg.error ? task.reject(new Error(JSON.stringify(msg.error))) : task.resolve(msg.result);}
    else if (msg.method === 'Runtime.exceptionThrown') exceptions.push(msg.params.exceptionDetails);
    else if (msg.method === 'Runtime.consoleAPICalled' && msg.params.type === 'error') consoleErrors.push(msg.params.args);
    else if (msg.method === 'Fetch.requestPaused') {
      const allowed = msg.params.request.url.startsWith(origin + '/');
      if (!allowed) external.push(msg.params.request.url);
      call(allowed ? 'Fetch.continueRequest' : 'Fetch.failRequest',
        {requestId: msg.params.requestId, ...(allowed ? {} : {errorReason: 'BlockedByClient'})}).catch(() => {});
    }
  }
});
function call(method, params = {}, session = sessionId) {
  return new Promise((resolve, reject) => {
    const id = ++seq, timer = setTimeout(() => {pending.delete(id); reject(new Error('CDP 超时 ' + method));}, 15000);
    pending.set(id, {resolve, reject, timer});
    browser.stdio[3].write(JSON.stringify({id, method, params, ...(session ? {sessionId: session} : {})}) + '\0');
  });
}
async function evaluate(expression) {
  const r = await call('Runtime.evaluate', {expression, returnByValue: true, awaitPromise: true});
  if (r.exceptionDetails) throw new Error(JSON.stringify(r.exceptionDetails));
  return r.result.value;
}
const pause = ms => new Promise(r => setTimeout(r, ms));
async function waitFor(expression) {
  const until = Date.now() + 8000;
  do {if (await evaluate(expression)) return; await pause(60);} while (Date.now() < until);
  throw new Error('waitFor 超时: ' + expression);
}
async function screenshot(name) {
  const r = await call('Page.captureScreenshot', {format: 'png'});
  writeFileSync(path.join(out, name + '.png'), Buffer.from(r.data, 'base64'));
}
async function click(selector) {
  const p = await evaluate(`(()=>{const e=document.querySelector(${JSON.stringify(selector)});if(!e)throw Error('缺少点击目标 ${selector}');e.scrollIntoView({block:'center',inline:'nearest'});const r=e.getBoundingClientRect(),x=r.x+r.width/2,y=r.y+r.height/2;const hit=document.elementFromPoint(x,y);return {x,y,width:r.width,height:r.height,hit:hit===e||e.contains(hit),disabled:e.matches(':disabled')};})()`);
  assert.ok(p.width && p.height && p.hit && !p.disabled, '可点击性 ' + selector + JSON.stringify(p));
  await call('Input.dispatchMouseEvent', {type: 'mousePressed', x: p.x, y: p.y, button: 'left', clickCount: 1});
  await call('Input.dispatchMouseEvent', {type: 'mouseReleased', x: p.x, y: p.y, button: 'left', clickCount: 1});
}
// 按索引点击同名元素中的第 n 个：page.html 有侧栏和小屏两套导航，
// 窄屏下侧栏那个副本宽高为 0，必须点可见的那一个。
async function clickNth(selector, index) {
  const p = await evaluate(`(()=>{const e=document.querySelectorAll(${JSON.stringify(selector)})[${index}];if(!e)throw Error('缺少点击目标');e.scrollIntoView({block:'center',inline:'nearest'});const r=e.getBoundingClientRect(),x=r.x+r.width/2,y=r.y+r.height/2;const hit=document.elementFromPoint(x,y);return {x,y,width:r.width,height:r.height,hit:hit===e||e.contains(hit)};})()`);
  assert.ok(p.width && p.height && p.hit, '可点击性 ' + selector + '[' + index + ']' + JSON.stringify(p));
  await call('Input.dispatchMouseEvent', {type: 'mousePressed', x: p.x, y: p.y, button: 'left', clickCount: 1});
  await call('Input.dispatchMouseEvent', {type: 'mouseReleased', x: p.x, y: p.y, button: 'left', clickCount: 1});
}
const codes = {Tab: 9, Enter: 13, a: 65};
async function key(k, modifiers = 0) {
  const params = {key: k, code: k, windowsVirtualKeyCode: codes[k] || k.toUpperCase().charCodeAt(0), modifiers};
  await call('Input.dispatchKeyEvent', {type: 'keyDown', ...params});
  await call('Input.dispatchKeyEvent', {type: 'keyUp', ...params});
}
// 真实键盘输入，不用 input.value 赋值。
async function typeInto(selector, value) {
  await click(selector);
  await key('a', 2);
  await call('Input.insertText', {text: value});
}
async function check(name, fn) {
  const start = Date.now();
  try {const evidence = await fn(); checks.push({name, status: 'pass', ms: Date.now() - start, evidence}); console.log('PASS ' + name);}
  catch (e) {checks.push({name, status: 'fail', error: e.stack}); throw e;}
}

let version, failure;
const watchdog = setTimeout(() => browser.kill(), 240000);
try {
  version = await call('Browser.getVersion', {}, null);
  const {targetId} = await call('Target.createTarget', {url: 'about:blank'}, null);
  ({sessionId} = await call('Target.attachToTarget', {targetId, flatten: true}, null));
  await call('Page.enable'); await call('Runtime.enable');
  await call('Fetch.enable', {patterns: [{urlPattern: 'http*'}]});
  await call('Emulation.setDeviceMetricsOverride', {width: 1280, height: 950, deviceScaleFactor: 1, mobile: false});
  await call('Page.addScriptToEvaluateOnNewDocument', {source: "{const N=Date;window.Date=class extends N{constructor(...a){super(...(a.length?a:[Date.UTC(2026,9,16,8,23)]))}static now(){return N.UTC(2026,9,16,8,23)}}}"});

  await call('Page.navigate', {url: origin + '/#tab=model-statistics&view=frt'});
  await waitFor("document.querySelector('.frt-channel')!==null");

  await check('紧凑时间选择器：七个动作全在且压成一行，进页面即可见标题', async () => {
    const actions = await evaluate("[...document.querySelectorAll('#frtDates [data-md-action]')].map(b=>b.dataset.mdAction)");
    for (const a of ['previous', 'next', 'today', 'yesterday', 'start', 'end', 'single', 'reset']) {
      assert.ok(actions.includes(a), '缺少动作 ' + a);
    }
    // 前后天已图标化
    const stepText = await evaluate("[...document.querySelectorAll('#frtDates .md-step')].map(b=>b.textContent.trim())");
    assert.deepEqual(stepText, ['‹', '›'], '前后天应为 ‹ ›：' + JSON.stringify(stepText));
    // 单行：表单总高度必须明显小于旧版三行块（旧版 >150px）
    const box = await evaluate("(()=>{const r=document.getElementById('frtDates').getBoundingClientRect();return {h:Math.round(r.height),top:Math.round(r.top)};})()");
    assert.ok(box.h < 110, '时间选择器应紧凑，实测高度 ' + box.h);
    // 进页面时标题在视口内
    const titleTop = await evaluate("Math.round(document.querySelector('#msFRTView h2').getBoundingClientRect().top)");
    assert.ok(titleTop >= 0 && titleTop < 950, '进页面应看到最上方，标题 top=' + titleTop);
    await screenshot('01-frt-compact-datepicker');
    return {actions, box, titleTop};
  });

  await check('阈值筛选：严格大于，命中渠道子树保留，不可计算渠道单独提示', async () => {
    await typeInto('#frtMeanGt', '2');
    await click('#frtFilters button[type=submit]');
    await waitFor("document.getElementById('frtTree').innerText.includes('slow-channel')");
    const tree = await evaluate("document.getElementById('frtTree').innerText");
    assert.match(tree, /slow-channel/, '均值 5 秒应命中');
    assert.doesNotMatch(tree, /fast-channel/, '均值 1 秒不应命中');
    assert.doesNotMatch(tree, /unknown-mean-channel/, '不可计算渠道不应作为命中出现');
    // 命中渠道的分组与模型原样保留
    await click('.frt-channel>summary');
    await waitFor("document.getElementById('frtTree').innerText.includes('actual-group')");
    assert.match(await evaluate("document.getElementById('frtTree').innerText"), /actual-group/);
    // 不可计算渠道必须有明确提示，不能静默消失
    const coverage = await evaluate("document.getElementById('frtCoverage').innerText");
    assert.match(coverage, /1 个渠道的平均 FRT 不可计算/, '覆盖说明应报数：' + coverage);
    assert.match(coverage, /既不表示达标也不表示超标/);
    assert.match(coverage, /平均 FRT 严格大于 2 秒/, '筛选范围应写明：' + coverage);
    await screenshot('02-frt-threshold-hit');
    return {coverage};
  });

  await check('阈值请求参数按秒传递，小数生效，清除筛选后恢复全部渠道', async () => {
    const last = requests.filter(r => r.path === '/model-statistics/frt').at(-1);
    assert.equal(new URLSearchParams(last.query).get('mean_gt_s'), '2');
    await typeInto('#frtMeanGt', '0.5');
    await click('#frtFilters button[type=submit]');
    await waitFor("document.getElementById('frtTree').innerText.includes('fast-channel')");
    assert.equal(new URLSearchParams(requests.filter(r => r.path === '/model-statistics/frt').at(-1).query).get('mean_gt_s'), '0.5');
    await click('#frtReset');
    await waitFor("document.getElementById('frtTree').innerText.includes('unknown-mean-channel')");
    assert.equal(await evaluate("document.getElementById('frtMeanGt').value"), '', '清除筛选应清空阈值输入');
    const coverage = await evaluate("document.getElementById('frtCoverage').innerText");
    assert.doesNotMatch(coverage, /不可计算，未参与/, '未筛选时不应报告剔除');
    await screenshot('03-frt-threshold-cleared');
    return {cleared: true};
  });

  await check('模型统计原页同样使用紧凑选择器，切页回到顶部', async () => {
    await click('#msRequestsTab');
    await waitFor("!document.getElementById('msRequestsView').hidden");
    const h = await evaluate("Math.round(document.getElementById('msDates').getBoundingClientRect().height)");
    assert.ok(h < 110, '原页选择器应同样紧凑，实测 ' + h);
    const stepText = await evaluate("[...document.querySelectorAll('#msDates .md-step')].map(b=>b.textContent.trim())");
    assert.deepEqual(stepText, ['‹', '›']);
    await screenshot('04-ms-compact-datepicker');
    return {h};
  });

  await check('Token 页默认只展示摘要与统计，其余收进折叠区', async () => {
    // 先滚到页面中部，验证切页会回到顶部
    await evaluate("window.scrollTo({top:600})");
    await click('[data-tab="anomalies"]');
    await waitFor("document.getElementById('taBreakdown')!==null && document.getElementById('taCoverage').innerText.length>0");
    assert.equal(await evaluate("Math.round(window.scrollY)"), 0, '进入 Token 页应回到顶部');
    // 默认展开的只有统计面板；健康、金额、明细、导出、通知均折叠
    const open = await evaluate("[...document.querySelectorAll('#tokenAnomalyView details.ta-panel')].map(d=>({t:d.querySelector('summary').textContent.trim(),open:d.open}))");
    assert.ok(open.length >= 5, '应有 5 个折叠面板，实为 ' + open.length);
    for (const d of open) assert.equal(d.open, false, '默认应折叠：' + d.t);
    const titles = open.map(d => d.t).join('|');
    for (const want of ['健康', '金额', '请求明细', '导出', '通知']) {
      assert.ok(titles.includes(want), '折叠区应含 ' + want + '：' + titles);
    }
    // 统计面板本身不是折叠的
    assert.ok(await evaluate("document.getElementById('taBreakdown').closest('details')===null"), '统计表不应被折叠');
    // 默认维度为渠道
    assert.equal(await evaluate("document.getElementById('taDimension').value"), 'channel_id');
    await screenshot('05-token-simplified');
    return {open};
  });

  await check('Token 空态显示采集未启用的具体原因', async () => {
    const text = await evaluate("document.getElementById('taBreakdown').innerText");
    assert.match(text, /采集未启用/, '空态应指出采集未启用：' + text);
    assert.doesNotMatch(text, /当前窗口无结果/);
    await screenshot('06-token-empty-facts-disabled');
    return {text};
  });

  await check('Token 空态随健康状态变化给出不同原因', async () => {
    tokenScenario = 'source-down';
    await click('#taRefresh');
    await waitFor("document.getElementById('taBreakdown').innerText.includes('来源不可用')");
    const down = await evaluate("document.getElementById('taBreakdown').innerText");
    assert.doesNotMatch(down, /采集未启用/);
    tokenScenario = 'all-on';
    await click('#taRefresh');
    await waitFor("document.getElementById('taBreakdown').innerText.includes('当前窗口无结果')");
    const on = await evaluate("document.getElementById('taBreakdown').innerText");
    assert.doesNotMatch(on, /采集未启用|来源不可用|检测未启用/);
    await screenshot('07-token-empty-window-no-result');
    return {down, on};
  });

  await check('更多筛选折叠区可展开，原有筛选项都在', async () => {
    assert.equal(await evaluate("document.querySelector('.ta-more-filters').open"), false);
    await click('.ta-more-filters>summary');
    await waitFor("document.querySelector('.ta-more-filters').open===true");
    for (const id of ['taStatusFilter', 'taRule', 'taEvidence', 'taMethod']) {
      assert.ok(await evaluate(`!!document.getElementById('${id}')&&document.getElementById('${id}').offsetParent!==null`), id + ' 展开后应可见');
    }
    await screenshot('08-token-more-filters');
    return {ok: true};
  });

  await check('窄屏 375px 下两处选择器与折叠区仍可用', async () => {
    await call('Emulation.setDeviceMetricsOverride', {width: 375, height: 760, deviceScaleFactor: 2, mobile: true});
    await call('Page.navigate', {url: origin + '/#tab=model-statistics&view=frt'});
    await waitFor("document.querySelector('.frt-channel')!==null");
    const overflow = await evaluate("(()=>{const e=document.getElementById('frtDates');return {scroll:e.scrollWidth,client:document.documentElement.clientWidth};})()");
    assert.ok(overflow.scroll <= overflow.client + 1, '窄屏不应横向溢出：' + JSON.stringify(overflow));
    await screenshot('09-narrow-frt');
    // 窄屏隐藏侧栏，必须点小屏 nav 里的那个副本（page.html 有两套导航）。
    const visibleTab = await evaluate("(()=>{const all=[...document.querySelectorAll('[data-tab=\"anomalies\"]')];return all.findIndex(e=>e.getBoundingClientRect().width>0);})()");
    assert.ok(visibleTab >= 0, '窄屏下应有一个可见的异常监控入口（两套导航都不可见即为移动端进不去）');
    await clickNth('[data-tab="anomalies"]', visibleTab);
    await waitFor("document.getElementById('taBreakdown')!==null");
    await screenshot('10-narrow-token');
    return overflow;
  });

} catch (e) {
  failure = e;
} finally {
  clearTimeout(watchdog);
  try {browser.kill();} catch {}
  server.close();
}

const result = {
  generated_at: new Date().toISOString(),
  browser: version?.product, origin,
  checks, exceptions, consoleErrors, external,
  request_paths: [...new Set(requests.map(r => r.path))],
  failure: failure ? failure.stack : null,
};
writeFileSync(path.join(out, 'result.json'), JSON.stringify(result, null, 2));
console.log('\n外部请求=' + external.length + ' JS异常=' + exceptions.length + ' console错误=' + consoleErrors.length);
console.log('通过=' + checks.filter(c => c.status === 'pass').length + ' 失败=' + checks.filter(c => c.status === 'fail').length);
if (failure) {console.error(failure); process.exit(1);}
assert.equal(external.length, 0, '不得有外部请求');
assert.equal(exceptions.length, 0, '不得有未捕获 JS 异常');
assert.equal(consoleErrors.length, 0, '不得有 console error');
console.log('全部通过');
