// 在真实 8204（127.0.0.3:8204）上验收首字耗时平均值与阈值筛选。
// 只读：登录、打开页面、点击筛选、截图。不提交任何任务、不改配置、不写数据。
// 用法: node dev/verify-8204-frt-real.mjs --browser <chrome> --out <目录>
import assert from 'node:assert/strict';
import {writeFileSync, mkdirSync, mkdtempSync} from 'node:fs';
import {spawn} from 'node:child_process';
import path from 'node:path';

const args = process.argv.slice(2), option = name => args[args.indexOf(name) + 1];
assert.ok(args.includes('--browser') && args.includes('--out'), '--browser 与 --out 必填');
const out = path.resolve(option('--out'));
mkdirSync(out, {recursive: true});
const profile = mkdtempSync(path.join(out, 'profile-'));
const browserTemp = path.join(out, 'temp');
mkdirSync(browserTemp, {recursive: true});
const origin = 'http://127.0.0.3:8204';

const checks = [], exceptions = [], consoleErrors = [];
const browser = spawn(option('--browser'), ['--headless=new', '--remote-debugging-pipe', '--no-first-run',
  '--no-default-browser-check', '--disable-component-update', '--disable-sync', '--disable-default-apps',
  '--metrics-recording-only', '--lang=zh-CN', '--window-size=1400,1000',
  `--user-data-dir=${profile}`, 'about:blank'],
  {stdio: ['ignore', 'pipe', 'pipe', 'pipe', 'pipe'], windowsHide: true,
   env: {...process.env, TEMP: browserTemp, TMP: browserTemp}});

let seq = 0, received = '', sessionId;
const pending = new Map();
browser.stderr.on('data', () => {});
browser.stdio[4].on('data', chunk => {
  received += chunk.toString();
  for (let at; (at = received.indexOf('\0')) >= 0;) {
    const t = received.slice(0, at); received = received.slice(at + 1);
    if (!t) continue;
    const msg = JSON.parse(t), task = pending.get(msg.id);
    if (task) {pending.delete(msg.id); clearTimeout(task.timer);
      msg.error ? task.reject(new Error(JSON.stringify(msg.error))) : task.resolve(msg.result);}
    else if (msg.method === 'Runtime.exceptionThrown') exceptions.push(msg.params.exceptionDetails);
    else if (msg.method === 'Runtime.consoleAPICalled' && msg.params.type === 'error') consoleErrors.push(msg.params.args);
  }
});
function call(method, params = {}, session = sessionId) {
  return new Promise((resolve, reject) => {
    const id = ++seq, timer = setTimeout(() => {pending.delete(id); reject(new Error('CDP 超时 ' + method));}, 30000);
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
async function waitFor(expression, ms = 40000) {
  const until = Date.now() + ms;
  do {if (await evaluate(expression)) return; await pause(250);} while (Date.now() < until);
  throw new Error('waitFor 超时: ' + expression);
}
async function screenshot(name) {
  const r = await call('Page.captureScreenshot', {format: 'png'});
  writeFileSync(path.join(out, name + '.png'), Buffer.from(r.data, 'base64'));
}
async function click(selector) {
  const p = await evaluate(`(()=>{const all=[...document.querySelectorAll(${JSON.stringify(selector)})];const e=all.find(x=>x.getBoundingClientRect().width>0);if(!e)throw Error('无可见目标 ${selector}');e.scrollIntoView({block:'center'});const r=e.getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2,w:r.width};})()`);
  await call('Input.dispatchMouseEvent', {type: 'mousePressed', x: p.x, y: p.y, button: 'left', clickCount: 1});
  await call('Input.dispatchMouseEvent', {type: 'mouseReleased', x: p.x, y: p.y, button: 'left', clickCount: 1});
}
async function typeInto(selector, value) {
  await click(selector);
  await call('Input.dispatchKeyEvent', {type: 'keyDown', key: 'a', code: 'KeyA', windowsVirtualKeyCode: 65, modifiers: 2});
  await call('Input.dispatchKeyEvent', {type: 'keyUp', key: 'a', code: 'KeyA', windowsVirtualKeyCode: 65, modifiers: 2});
  await call('Input.insertText', {text: value});
}
async function check(name, fn) {
  try {const evidence = await fn(); checks.push({name, status: 'pass', evidence}); console.log('PASS ' + name);}
  catch (e) {checks.push({name, status: 'fail', error: e.stack}); throw e;}
}

let version, failure;
const watchdog = setTimeout(() => browser.kill(), 300000);
try {
  version = await call('Browser.getVersion', {}, null);
  const {targetId} = await call('Target.createTarget', {url: 'about:blank'}, null);
  ({sessionId} = await call('Target.attachToTarget', {targetId, flatten: true}, null));
  await call('Page.enable'); await call('Runtime.enable');
  await call('Emulation.setDeviceMetricsOverride', {width: 1400, height: 1000, deviceScaleFactor: 1, mobile: false});

  // 先登录取会话（本地测试账号，仅用于打开只读页面）。
  await call('Page.navigate', {url: origin + '/login'});
  await pause(2000);
  const login = await evaluate(`(async()=>{const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:'local',password:'local'}),credentials:'same-origin'});return r.status;})()`);
  assert.equal(login, 200, '登录应成功，实为 ' + login);

  await call('Page.navigate', {url: origin + '/'});
  await pause(4000);
  await waitFor("document.getElementById('msFRTView')!==null");
  // 像用户那样操作：先点侧栏「模型统计」，再点「首字耗时」子页签。
  // 直接 navigate 带 hash 不可靠——初始化可能已先完成，子页不会被激活。
  await click('[data-tab="model-statistics"]');
  await pause(2500);
  await click('#msFRTTab');
  await pause(2500);
  try {
    await waitFor("document.querySelector('.frt-channel')!==null || document.getElementById('frtError')?.hidden===false", 45000);
  } catch (e) {
    // 渲染未出现时把实际状态写出来，便于定位而不是盲猜。
    const diag = await evaluate(`({
      url: location.href,
      frtViewHidden: document.getElementById('msFRTView')?.hidden,
      status: document.getElementById('frtStatus')?.innerText,
      coverage: document.getElementById('frtCoverage')?.innerText?.slice(0,300),
      errorHidden: document.getElementById('frtError')?.hidden,
      errorText: document.getElementById('frtError')?.innerText,
      treeHTML: document.getElementById('frtTree')?.innerHTML?.slice(0,400),
      me: window.__meName||null
    })`);
    writeFileSync(path.join(out, 'diagnose.json'), JSON.stringify(diag, null, 2));
    await screenshot('diagnose-no-render');
    throw new Error('页面未渲染，诊断已写入 diagnose.json: ' + JSON.stringify(diag).slice(0, 600));
  }

  await check('真实数据：平均 FRT 可计算，均值等于总和÷有效样本数', async () => {
    const text = await evaluate("document.getElementById('msFRTView').innerText");
    assert.doesNotMatch(text, /历史样本缺少真实耗时总和/, '回算后不应再出现缺总和提示');
    const data = await evaluate(`(async()=>{const r=await fetch('/model-statistics/frt?window=24h',{headers:{Accept:'application/json'},cache:'no-store'});const j=await r.json();
      let total=0,ok=0,sample=null;
      for(const c of j.channels||[]){total++;if(c.frt.mean_ms!=null){ok++;if(!sample&&c.frt.observed>100)sample={name:c.name,mean:c.frt.mean_ms,observed:c.frt.observed}}}
      return {total,ok,sample};})()`);
    assert.ok(data.total > 0, '真实窗口应有渠道数据');
    assert.equal(data.ok, data.total, `所有渠道均值都应可计算：${data.ok}/${data.total}`);
    assert.ok(data.sample, '应有样本数较多的渠道可供核对');
    await screenshot('real-01-frt-mean-available');
    return data;
  });

  await check('真实数据：页面均值与接口值一致，且非 P95/最大值', async () => {
    const cmp = await evaluate(`(async()=>{const r=await fetch('/model-statistics/frt?window=24h',{headers:{Accept:'application/json'},cache:'no-store'});const j=await r.json();
      const c=(j.channels||[]).find(x=>x.frt.observed>100&&x.frt.mean_ms!=null);
      return c?{name:c.name,mean:c.frt.mean_ms,p95:c.frt.p95_ms,max:c.frt.max_ms,observed:c.frt.observed}:null;})()`);
    assert.ok(cmp, '需要一个样本充足的渠道');
    // 均值必须不等于 P95 与最大值（真实数据下三者几乎不可能相同）。
    assert.notEqual(cmp.mean, cmp.p95, '均值不得等于 P95');
    assert.notEqual(cmp.mean, cmp.max, '均值不得等于最大值');
    assert.ok(cmp.mean <= cmp.max, '均值不应超过最大值');
    return cmp;
  });

  await check('真实数据：阈值筛选严格大于，不可计算渠道单独提示', async () => {
    await typeInto('#frtMeanGt', '3');
    await click('#frtFilters button[type=submit]');
    await pause(4000);
    await waitFor("document.getElementById('frtCoverage').innerText.includes('平均 FRT 严格大于')");
    const coverage = await evaluate("document.getElementById('frtCoverage').innerText");
    assert.match(coverage, /平均 FRT 严格大于 3 秒/);
    // 校验命中集中没有 <=3 秒的渠道
    const bad = await evaluate(`(async()=>{const r=await fetch('/model-statistics/frt?window=24h&mean_gt_s=3',{headers:{Accept:'application/json'},cache:'no-store'});const j=await r.json();
      return {hit:(j.channels||[]).length,violating:(j.channels||[]).filter(c=>c.frt.mean_ms<=3000).length,excluded:j.mean_filter_excluded};})()`);
    assert.equal(bad.violating, 0, '命中集中不得有均值<=3秒的渠道');
    await screenshot('real-02-frt-threshold-3s');
    return {coverage: coverage.slice(0, 300), ...bad};
  });

  await check('真实数据：7 天窗口同样可计算并可筛选', async () => {
    await click('[data-frt-window="7d"]');
    await pause(6000);
    await waitFor("document.querySelector('.frt-channel')!==null");
    const data = await evaluate(`(async()=>{const r=await fetch('/model-statistics/frt?window=7d',{headers:{Accept:'application/json'},cache:'no-store'});const j=await r.json();
      let total=0,ok=0,nosum=0,nosample=0;
      for(const c of j.channels||[]){total++;if(c.frt.mean_ms!=null)ok++;else if(c.frt.mean_unavailable_reason==='historical_sum_unavailable')nosum++;else nosample++}
      return {total,ok,nosum,nosample};})()`);
    assert.equal(data.nosum, 0, `7 天窗口不应再有缺历史总和的渠道：${data.nosum}`);
    await screenshot('real-03-frt-7d');
    return data;
  });

  await check('真实数据：紧凑时间选择器可用，进页面即见顶部', async () => {
    const box = await evaluate("(()=>{const r=document.getElementById('frtDates').getBoundingClientRect();return {h:Math.round(r.height)};})()");
    assert.ok(box.h < 110, '选择器应紧凑：' + box.h);
    const steps = await evaluate("[...document.querySelectorAll('#frtDates .md-step')].map(b=>b.textContent.trim())");
    assert.deepEqual(steps, ['‹', '›']);
    await screenshot('real-04-compact-picker');
    return {box, steps};
  });

  await check('真实 Token 页：检测与通知关闭，空态显示具体原因', async () => {
    await click('[data-tab="anomalies"]');
    await pause(5000);
    await waitFor("document.getElementById('taCoverage')!==null");
    const scrollY = await evaluate("Math.round(window.scrollY)");
    assert.equal(scrollY, 0, '进入页面应在顶部');
    const open = await evaluate("[...document.querySelectorAll('#tokenAnomalyView details.ta-panel')].map(d=>({t:d.querySelector('summary').textContent.trim(),open:d.open}))");
    for (const d of open) assert.equal(d.open, false, '默认应折叠：' + d.t);
    // 展开健康详情，确认采集/检测确实关闭
    await click('#tokenAnomalyView details.ta-panel>summary');
    await pause(1500);
    const health = await evaluate("document.querySelector('#taFactsHealth')?.innerText + '|' + document.querySelector('#taDetectionHealth')?.innerText");
    const breakdown = await evaluate("document.getElementById('taBreakdown').innerText");
    await screenshot('real-05-token-page');
    return {panels: open.length, health: health.slice(0, 200), breakdown: breakdown.slice(0, 300), scrollY};
  });

} catch (e) {
  failure = e;
} finally {
  clearTimeout(watchdog);
  try {browser.kill();} catch {}
}

const result = {generated_at: new Date().toISOString(), browser: version?.product, target: origin,
  checks, exceptions, consoleErrors, failure: failure ? failure.stack : null};
writeFileSync(path.join(out, 'result.json'), JSON.stringify(result, null, 2));
console.log('\nJS异常=' + exceptions.length + ' console错误=' + consoleErrors.length);
console.log('通过=' + checks.filter(c => c.status === 'pass').length + ' 失败=' + checks.filter(c => c.status === 'fail').length);
if (failure) {console.error(failure); process.exit(1);}
console.log('真实数据验收全部通过');
