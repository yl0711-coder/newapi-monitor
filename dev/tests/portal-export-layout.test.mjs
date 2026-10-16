import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
test('export buttons stay together with a single toolbar auto margin',()=>{
  const pair=page.match(/<div class="export-buttons">([^]*?)<\/div>/)?.[1];
  assert.ok(pair);
  assert.match(pair,/id="expBtn"[^]*?id="billBtn"/);
  for(const id of ['expBtn','billBtn','billResumeBtn'])assert.equal(page.match(new RegExp(`id="${id}"`,'g')).length,1);
  assert.match(page,/\.export-actions\{[^}]*margin-left:auto/);
  assert.match(page,/\.export-buttons\{[^}]*gap:8px/);
  assert.match(page,/\.exp-btn\{[^}]*margin-left:0/);
  assert.match(page,/@media\(max-width:600px\)\{\.export-actions\{margin-left:0;flex-basis:100%\}\}/);
  assert.match(page,/\.exp-btn\[hidden\]\{display:none\}/);
  assert.match(page,/role="group" aria-label="日志与账单导出"/);
});
test('export contracts and task recovery bindings are unchanged',()=>{
  assert.match(page,/\$\('billBtn'\)\.onclick=\(\)=>startExport\('bill-v1'\)/);
  assert.match(page,/\$\('billResumeBtn'\)\.onclick=restoreBillExport/);
  assert.match(page,/固定 15 列/);
  assert.match(page,/四舍五入最多保留 3 位小数/);
});
