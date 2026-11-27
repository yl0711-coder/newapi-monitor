import test from 'node:test';
import assert from 'node:assert/strict';
import {scanCaptureLines,functionName,executionRole} from './coverage-ledger.mjs';

const capture = rows => JSON.stringify({warning:'synthetic',test_exit:0,counts:{explained:rows.length},entries:rows},null,2).split('\n');
const row = {file:'monitor/example.go',function:'example/monitor.(*Monitor).read.func1',status:'explained',redacted_template:'SELECT \"x\"\nFROM t',plan:['SEARCH t']};
test('streaming parser returns complete entries and declared count',async()=>{
  const seen=[];const got=await scanCaptureLines(capture([row,row]),e=>seen.push(e));assert.equal(got.records,2);assert.deepEqual(seen,[row,row]);
});
test('empty artifact is valid but not positive execution evidence',async()=>{
  assert.equal((await scanCaptureLines(capture([]),()=>assert.fail())).records,0);
  assert.equal((await scanCaptureLines(capture([]).map(s=>s.replace('"entries": []','"entries": null')),()=>assert.fail())).records,0);
});
test('truncated artifact and count mismatch fail, not partial success',async()=>{
  await assert.rejects(scanCaptureLines(capture([row]).slice(0,-3),()=>{}),/incomplete capture/);
  const lines=capture([row]).map(s=>s.replace('"explained": 1','"explained": 2'));
  await assert.rejects(scanCaptureLines(lines,()=>{}),/expected 2/);
  await assert.rejects(scanCaptureLines(capture([row]).slice(0,-1),()=>{}),/incomplete capture/);
  await assert.rejects(scanCaptureLines([...capture([row]),'garbage'],()=>{}),/unexpected data/);
});
test('invalid entries and closure names are explicit',async()=>{
  await assert.rejects(scanCaptureLines(capture([{file:5}]),()=>{}),/invalid capture/);
  assert.equal(functionName(row.function),'read');assert.equal(functionName('(*Monitor).plain'),'plain');
});

test('generic runtime type qualifiers do not hide the actual function',()=>{
  assert.equal(functionName('example/monitor.incidentEncodeRows[github.com/example/monitor.Incident]'),'incidentEncodeRows');
  assert.equal(functionName('example.(*Holder[map[string][]example.Value]).Read[example.Other].func2.1'),'Read');
  assert.equal(functionName('example.read[unclosed.Type'),'__unresolved_generic_runtime_name__');
  assert.equal(functionName('example.read]'),'__unresolved_generic_runtime_name__');
});

test('builders, preparation, diagnostics and writes are not independent read executions',()=>{
  assert.equal(executionRole({kind:'raw_builder',api:'Raw',operation:'SELECT'}),'sql_builder_not_execution');
  assert.equal(executionRole({kind:'database_sql_candidate',api:'PrepareContext',operation:'SELECT'}),'statement_preparation_not_execution');
  assert.equal(executionRole({kind:'database_sql_candidate',api:'QueryContext',operation:'EXPLAIN'}),'diagnostic_query_execution');
  assert.equal(executionRole({kind:'database_sql_candidate',api:'QueryRowContext',operation:'unknown'}),'read_execution_candidate');
  assert.equal(executionRole({kind:'orm_read_candidate',api:'Scan'}),'read_execution_candidate');
  assert.equal(executionRole({kind:'execute_candidate',api:'Exec'}),'write_or_command_execution_candidate');
  assert.equal(executionRole({kind:'connection_access',api:'Raw'}),'connection_management');
  assert.equal(executionRole({kind:'unknown'}),'unresolved_role');
});
