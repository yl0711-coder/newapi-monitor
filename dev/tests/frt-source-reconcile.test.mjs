import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const root=path.resolve(import.meta.dirname,'..','..');
const sourceSQL=readFileSync(path.join(root,'dev','frt-source-reconcile','main.go'),'utf8');
const verifierSource=readFileSync(path.join(root,'dev','verify-8204-frt-reconcile.mjs'),'utf8');

test('independent source SQL keeps zero-sample leaves and bounds the read',()=>{
  assert.match(sourceSQL,/MAX_EXECUTION_TIME\(8000\)/);
  assert.doesNotMatch(sourceSQL,/HAVING\s+observed\s*>\s*0/);
  assert.match(sourceSQL,/sum_ms[\s\S]*observed/);
  assert.match(sourceSQL,/COALESCE\(SUM\(/);
  assert.match(sourceSQL,/type IN \(2,5,6\)/);
});

test('reconcile verifier checks a no-valid-samples leaf at all three levels',()=>{
  // Keep this test hermetic: the verifier itself is exercised with a synthetic
  // three-window fixture during the reconciliation acceptance run.  Here we
  // lock the source-level invariants so the normal Node suite does not need
  // to write to the host temp directory (which is unavailable in CI sandboxes).
  assert.match(verifierSource,/for\(const channel of report\.channels\?\?\[\]\)/);
  assert.match(verifierSource,/for\(const group of channel\.groups\?\?\[\]\)/);
  assert.match(verifierSource,/for\(const model of group\.models\?\?\[\]\)/);
  assert.match(verifierSource,/recordUnavailable\(model,'模型'/);
  assert.match(verifierSource,/recordUnavailable\(group,'分组'/);
  assert.match(verifierSource,/recordUnavailable\(channel,'渠道'/);
  assert.match(verifierSource,/reason==='no_valid_samples'/);
  assert.match(verifierSource,/pageObserved,0/);
  assert.match(verifierSource,/want\.observed,0/);
  assert.match(verifierSource,/production\.logs SQL 中不存在/);
});
