import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const page = readFileSync(new URL('../../monitor/page.html', import.meta.url), 'utf8');
const esc = value => String(value).replaceAll('<', '&lt;').replaceAll('>', '&gt;');
function functionSource(start, end) {
  const from = page.indexOf(start), to = page.indexOf(end, from);
  assert.ok(from >= 0 && to > from);
  return page.slice(from, to);
}

test('infra refresh failure preserves the last display and clearly marks it stale', async () => {
  const error = { innerHTML: '' };
  let rendered = 'previous snapshot';
  const context = vm.createContext({
    window: {}, location: {}, esc,
    document: { getElementById: () => error },
    fetch: async () => ({ ok: false, status: 503, json: async () => ({ enabled: true, error: '<read timeout>' }) }),
    renderInfra: snapshot => { rendered = snapshot; },
  });
  vm.runInContext(functionSource('async function loadInfra()', 'function infraSchedule()'), context);
  await vm.runInContext('loadInfra()', context);
  assert.equal(rendered, 'previous snapshot');
  assert.match(error.innerHTML, /刷新失败/);
  assert.match(error.innerHTML, /上次成功结果，本次未更新/);
  assert.match(error.innerHTML, /&lt;read timeout&gt;/);
  context.fetch = async () => ({ ok: true, status: 200, json: async () => ({ snapshot: 'new snapshot' }) });
  await vm.runInContext('loadInfra()', context);
  assert.equal(rendered, 'new snapshot');
  assert.equal(error.innerHTML, '');
});

test('infra trend failure is not presented as insufficient history and keeps old chart', async () => {
  const wrap = {
    innerHTML: 'old chart',
    querySelector: () => null,
    insertAdjacentHTML(_position, html) { this.innerHTML = html + this.innerHTML; },
  };
  const context = vm.createContext({
    esc, location: {},
    fetch: async () => ({ ok: false, status: 503, json: async () => ({ error: 'read timeout' }) }),
    host: { querySelector: () => wrap },
    resource: { name: 'node', metrics: {} },
    group: { combined: true, label: 'CPU', metrics: [{ m: 'cpu' }] },
  });
  vm.runInContext(functionSource('async function drawGroup(', 'function renderInfra('), context);
  await vm.runInContext('drawGroup(host, resource, group)', context);
  assert.match(wrap.innerHTML, /趋势刷新失败/);
  assert.match(wrap.innerHTML, /old chart/);
  assert.doesNotMatch(wrap.innerHTML, /历史数据积累中/);
});

test('server metrics view hides only explicitly inactive cards and restores them as scope changes', () => {
  const ids = ['infraLockCard', 'infraDatabaseCard', 'infraLoadBalancerCard'];
  const cards = new Map(ids.map(id => [id, { hidden: false }]));
  for (const id of ids) assert.ok(page.includes(`id="${id}" hidden`), id);
  const context = vm.createContext({document: {getElementById: id => {
    assert.ok(cards.has(id), 'must not hide hosts, probes, ECS log state or alert history');
    return cards.get(id);
  }}});
  vm.runInContext(functionSource('function renderInfraPanelVisibility(', 'function renderInfra('), context);
  context.renderInfraPanelVisibility({panels: {origin_lock: false, database: false, load_balancer: false}});
  assert.deepEqual(ids.map(id => cards.get(id).hidden), [true, true, true]);
  context.renderInfraPanelVisibility({panels: {origin_lock: true, database: true, load_balancer: false}});
  assert.deepEqual(ids.map(id => cards.get(id).hidden), [false, false, true]);
  context.renderInfraPanelVisibility({}); // compatible with responses without visibility fields
  assert.deepEqual(ids.map(id => cards.get(id).hidden), [false, false, false]);
  assert.match(page, /renderInfraPanelVisibility\(s\);\s*renderInfraOverview\(s\);\s*renderProbes\(s\);/);
});
