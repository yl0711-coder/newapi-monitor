// 验「无数据时必须显示具体原因」：采集未启用 / 来源未配置 / 检测未启用 /
// 规则未配置 / 当前窗口无结果，五种必须可区分，不得退化成"没有数据"。
import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {at, access, health, detectionHealth, notifyHealth, report} from '../token-anomaly-fixture.mjs';

const source = readFileSync(new URL('../../monitor/token_anomaly.js', import.meta.url), 'utf8');

function harness() {
  const elements = new Map();
  const requests = [];
  const location = {href: ''};
  const element = id => {
    if (!elements.has(id)) {
      const listeners = {};
      elements.set(id, {
        id, value: '', checked: false, disabled: false, hidden: false, open: false,
        innerHTML: '', textContent: '', attrs: {}, listeners,
        setAttribute(k, v) {this.attrs[k] = v;},
        addEventListener(k, f) {listeners[k] = f;},
        focus() {}, showModal() {this.open = true;}, close() {this.open = false;},
      });
    }
    return elements.get(id);
  };
  const context = vm.createContext({
    window: {logChainOpen: () => {}},
    document: {getElementById: element, querySelectorAll: () => []},
    location, URLSearchParams, AbortController, URL, setTimeout,
    crypto: {randomUUID: () => 'id'},
    fetch: (url, options) => new Promise((resolve, reject) => requests.push({url, options, resolve, reject})),
  });
  const end = source.lastIndexOf('})();');
  vm.runInContext(
    source.slice(0, end) + '\nglobalThis.testing={state,load,emptyReason};\n' + source.slice(end),
    context,
  );
  const h = {...context.testing, element, requests};
  h.init = () => {};
  h.state.active = true;
  h.state.query = {from: at - 86400, to: at + 3600, status: 'candidate'};
  element('taDimension').value = 'channel_id';
  return h;
}

const response = (v, status = 200) => ({ok: status === 200, status, json: async () => v});

// 按 load() 内 paths 的顺序解析：report, facts, detection, notify, access。
function settle(h, {reportValue = report(), facts = health, detection = detectionHealth} = {}) {
  const values = [reportValue, facts, detection, notifyHealth, access];
  for (const [i, v] of values.entries()) h.requests[i].resolve(response(v));
}

// 空结果：analysis.breakdown 与 items 均为空，summary 计数归零。
function emptyReport(summary = {}) {
  const r = report();
  r.items = [];
  r.analysis.breakdown = [];
  r.summary = {...r.summary, observed_results: 0, candidate_requests: 0, amounts: {}, gaps: [], ...summary};
  return r;
}

test('候选为空时仍分别展示最新请求的采集、关联和检测进度', async () => {
	for(const [state,label]of [['not_hit','已检测未命中'],['association_pending','关联待处理'],['detection_pending','检测排队中'],['partially_evaluated','部分规则已判断']]){
		const h=harness(),value=emptyReport({source_complete:false});
		value.latest_progress={sample_count:1,sample_limit:100,states:{[state]:1}};
		const pending=h.load();settle(h,{reportValue:value});await pending;
		assert.ok(h.emptyReason().includes(label),state);
		assert.ok(h.emptyReason().includes('全部结果'));
	}
	const h=harness(),value=emptyReport({source_complete:false});
	value.latest_progress={sample_count:0,sample_limit:100,states:{}};
	const pending=h.load();settle(h,{reportValue:value});await pending;
	assert.match(h.emptyReason(),/尚未采集到请求/);
});

test('采集未启用时，空态必须指出采集未启用而不是只说没有数据', async () => {
  const h = harness();
  const p = h.load();
  settle(h, {reportValue: emptyReport(), facts: {...health, enabled: false}});
  await p;
  const text = h.element('taBreakdown').innerHTML + h.element('taList').innerHTML;
  assert.match(text, /采集未启用/);
  assert.doesNotMatch(text, /当前窗口无结果/);
});

test('采集已启用但来源不可用时，空态必须指出来源不可用', async () => {
  const h = harness();
  const p = h.load();
  settle(h, {reportValue: emptyReport(), facts: {...health, enabled: true, source_available: false}});
  await p;
  const text = h.element('taBreakdown').innerHTML;
  assert.match(text, /来源不可用或未接入/);
  assert.doesNotMatch(text, /采集未启用/);
});

test('采集与来源正常但检测未启用时，空态必须指出检测未启用', async () => {
  const h = harness();
  const p = h.load();
  settle(h, {
    reportValue: emptyReport(),
    facts: {...health, enabled: true, source_available: true},
    detection: {...detectionHealth, enabled: false},
  });
  await p;
  const text = h.element('taBreakdown').innerHTML;
  assert.match(text, /检测未启用/);
  assert.doesNotMatch(text, /采集未启用/);
});

test('检测已启用但规则未配置时，空态必须指出规则未配置', async () => {
  const h = harness();
  const p = h.load();
  settle(h, {
    reportValue: emptyReport(),
    facts: {...health, enabled: true, source_available: true},
    detection: {...detectionHealth, enabled: true, policy_configured: false},
  });
  await p;
  assert.match(h.element('taBreakdown').innerHTML, /规则未配置/);
});

test('检测队列迁移中仍展示已提交进度，空结果不得表示无异常', async () => {
  const h = harness();
  const p = h.load();
  settle(h, {
    reportValue: emptyReport({source_complete: true, unprocessed_requests: 0, insufficient_requests: 0}),
    facts: {...health, available: true, enabled: true, source_available: true},
    detection: {...detectionHealth, available: true, enabled: true, policy_configured: true, queue_ready: false},
  });
  await p;
  assert.match(h.element('taList').innerHTML, /检测队列索引正在恢复/);
  assert.doesNotMatch(h.element('taList').innerHTML, /当前所选时间内没有 Token 异常情况/);
});

test('全部启用但来源未覆盖时，不能给出无异常结论', async () => {
  const h = harness();
  const p = h.load();
  settle(h, {
    reportValue: emptyReport({source_complete: false, gaps: ['source_range_incomplete']}),
    facts: {...health, enabled: true, source_available: true},
    detection: {...detectionHealth, enabled: true, policy_configured: true},
  });
  await p;
  const text = h.element('taBreakdown').innerHTML;
  assert.match(text, /正在采集/);
  // 覆盖未完整时不得被读成"已确认没有异常"。
  assert.match(text, /来源覆盖未完整/);
  assert.doesNotMatch(text, /当前所选时间内没有 Token 异常情况/);
  assert.match(h.element('taUpdated').textContent, /所选窗口采集未完整/);
});

test('只有完整采集、完整检测、零缺口的未收窄空列表才给出无异常文案', async () => {
  for (const [summary, expected] of [
    [{source_complete:true,unprocessed_requests:0,insufficient_requests:0}, /当前所选时间内没有 Token 异常情况/],
    [{source_complete:true,unprocessed_requests:1,insufficient_requests:0}, /检测中/],
    [{source_complete:true,unprocessed_requests:0,insufficient_requests:2}, /证据不足/],
    [{source_complete:true,unprocessed_requests:0,insufficient_requests:undefined}, /检测覆盖未知/],
    [{source_complete:true,unprocessed_requests:0,insufficient_requests:0,truncated:true}, /覆盖不完整/],
  ]) {
    const h=harness(),p=h.load();
    const selected = emptyReport(summary);
    // The selected window may be complete despite an earlier global gap.
    selected.source_complete_through = h.state.query.from - 300;
    settle(h,{reportValue:selected,facts:{...health,enabled:true,source_available:true},detection:{...detectionHealth,enabled:true,policy_configured:true}});
    await p;
    assert.match(h.element('taList').innerHTML,expected);
    assert.match(h.element('taUpdated').textContent,/所选窗口采集完整 · 最新已采集请求/);
    h.state.query.channel_id=99;
    assert.doesNotMatch(h.emptyReason(),/当前所选时间内没有 Token 异常情况/);
  }
});

test('健康状态读取失败时，空态必须说明原因未知，不得断言无异常', async () => {
  const h = harness();
  const p = h.load();
  h.requests[0].resolve(response(emptyReport()));
  h.requests[1].reject(new Error('facts down'));
  h.requests[2].reject(new Error('detection down'));
  h.requests[3].resolve(response(notifyHealth));
  h.requests[4].resolve(response(access));
  await p;
  const text = h.element('taBreakdown').innerHTML;
  assert.match(text, /状态读取失败/);
  assert.match(text, /不能据此确认无异常/);
});

test('健康接口明确不可用时，空态不能把空结果显示成无 Token 异常', async () => {
  const h = harness();
  const p = h.load();
  settle(h, {
    reportValue: emptyReport(),
    facts: {...health, enabled: true, source_available: true, available: false},
    detection: {...detectionHealth, enabled: true, policy_configured: true, available: true},
  });
  await p;
  const text = h.element('taBreakdown').innerHTML + h.element('taList').innerHTML;
  assert.match(text, /健康状态不可用/);
  assert.doesNotMatch(text, /当前所选时间内没有 Token 异常情况/);
});

// 回归：接口少返回开关字段时，不得把未知当成"已启用、确实没有异常"。
// 这是真实踩到的缺陷——浏览器验收时 fixture 把 health 挂在错误的 HTTP 方法上，
// 字段全为 undefined，页面却显示"采集与检测均已启用且来源可用"。
test('开关字段缺失时必须报状态不完整，不得冒充一切正常', async () => {
  for (const broken of [{}, {enabled: true}, {enabled: true, source_available: true}]) {
    const h = harness();
    const p = h.load();
    settle(h, {reportValue: emptyReport(), facts: broken, detection: {}});
    await p;
    const text = h.element('taBreakdown').innerHTML;
    assert.match(text, /状态不完整/, '缺字段 ' + JSON.stringify(broken) + ' 应报不完整：' + text);
    assert.match(text, /不能据此确认无异常/);
    assert.doesNotMatch(text, /均已启用且来源可用/);
  }
});

test('有数据时不得显示任何空态原因', async () => {
  const h = harness();
  const p = h.load();
  settle(h);
  await p;
  assert.match(h.element('taBreakdown').innerHTML, /渠道|3/);
  assert.doesNotMatch(h.element('taBreakdown').innerHTML, /采集未启用|当前窗口无结果/);
});
