import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const page = fs.readFileSync(new URL('../../monitor/page.html', import.meta.url), 'utf8');

test('usage amounts use thousands separators and a leading minus sign', () => {
  const line = page.match(/^const fmtCost=.*$/m)?.[0];
  assert.ok(line, 'fmtCost definition not found');
  const ctx = vm.createContext({});
  vm.runInContext(line.replace('const fmtCost=', 'globalThis.fmtCost='), ctx);
  const f = ctx.fmtCost;
  assert.equal(f(1358212.87), '$1,358,212.87');
  assert.equal(f(0), '$0.00');
  assert.equal(f(-0.0336), '-$0.03');
  assert.equal(f(-1234.5), '-$1,234.50');
  assert.equal(f(0.0042), '$0.0042');
  assert.equal(f('bad'), '$0.00');
});

test('daily matrix cells use thousands separators', () => {
  const line = page.match(/const fmtCell=.*$/m)?.[0];
  assert.ok(line, 'fmtCell definition not found');
  const ctx = vm.createContext({});
  vm.runInContext(line.trim().replace('const fmtCell=', 'globalThis.fmtCell='), ctx);
  assert.equal(ctx.fmtCell(15525.13), '$15,525.13');
  assert.equal(ctx.fmtCell(0.003), '<$0.01');
  assert.equal(ctx.fmtCell(0), '$0.00');
});
