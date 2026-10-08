import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';

function renderFacts(report) {
  const source = readFileSync(new URL('../../monitor/page.html', import.meta.url), 'utf8');
  const start = source.indexOf('function syncRenderFinanceFacts(');
  const end = source.indexOf('function syncRenderStability(', start);
  assert.ok(start >= 0 && end > start);
  const target = { innerHTML: '' };
  const context = vm.createContext({
    document: { getElementById(id) { assert.equal(id, 'syncFinanceFacts'); return target; } },
    fmtNum: value => String(value || 0),
    syncTime: value => String(value || 0),
    syncKV: (title, value, note) => `${title}: ${value}\n${note}\n`,
    report,
  });
  vm.runInContext(source.slice(start, end), context);
  const level = vm.runInContext('syncRenderFinanceFacts(report)', context);
  return { level, text: target.innerHTML };
}

test('finance hourly catchup does not claim report or profit completeness', () => {
  const result = renderFacts({ enabled: true, report_enabled: true, status: 'caught_up',
    expected_hours: 3000, completed_hours: 3000, report_refresh: { enabled: true } });
  assert.equal(result.level, 'ok'); // Healthy collection is not a financial publication proof.
  assert.match(result.text, /小时采集已追平/);
  assert.match(result.text, /小时采集覆盖: 3000\/3000 小时 · 100.0%/);
  assert.match(result.text, /小时采集 100% 不代表收入和利润已可发布/);
  assert.match(result.text, /历史赠送分组、上游修正依据、收入成本配对和 AWS 成本/);
  assert.match(result.text, /此处不触发报表重算/);
  assert.doesNotMatch(result.text, /安全定稿至/);
});

test('finance collection and report failures remain visible independently', () => {
  for (const report of [
    { enabled: true, status: 'error', failure_streak: 2 },
    { enabled: true, status: 'source_changed' },
    { enabled: true, status: 'caught_up', report_refresh: { enabled: true, unresolved_failures: 1 } },
  ]) {
    assert.equal(renderFacts(report).level, 'bad');
  }
  assert.equal(renderFacts({ enabled: false, status: 'disabled' }).level, 'neutral');
  assert.equal(renderFacts({ enabled: true, status: 'backfilling' }).level, 'warn');
});
