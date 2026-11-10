import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const context=vm.createContext({window:{}});
vm.runInContext(readFileSync(new URL('../../monitor/view_refresh.js',import.meta.url),'utf8'),context);
const timing=context.window.MonitorView.timing;
const freshness=context.window.MonitorView.freshness;
const base={timing:{finalize_delay_seconds:120,alignment_seconds:60,collection_poll_seconds:60},actual_through_ts:1792700000,lag_seconds:0};
test('response time cannot substitute for an unknown data watermark',()=>{
 for(const through of [0,null,undefined]){
  const text=timing({...base,actual_through_ts:through,generated_at:1792700500,to_ts:1792700400});
  assert.match(text,/实际采集进度未知/);assert.doesNotMatch(text,/已追平/);
 }
});
test('freshness warning is independent of coverage and bounded by the configured poll',()=>{
 for(const lag_seconds of [0,1,60])assert.equal(freshness({...base,lag_seconds}).warning,false);
 assert.equal(freshness({...base,lag_seconds:61}).state,'backlog');
 assert.equal(freshness({...base,collection_failed:true}).state,'failed');
 assert.equal(freshness({...base,lag_seconds:null}).state,'unknown');
 assert.equal(freshness({...base,coverage:{complete:false}}).warning,false,'a gap is judged separately by the window coverage');
});
test('normal finalize waiting, backlog and collector failure stay distinct',()=>{
 assert.match(timing(base),/封口等待 120 秒.*已追平封口目标/);
 assert.match(timing({...base,lag_seconds:60}),/距封口目标 60 秒/);
 assert.match(timing({...base,lag_seconds:61}),/采集积压 61 秒/);
 assert.match(timing({...base,collection_failed:true}),/采集失败/);
 assert.match(timing({...base,lag_seconds:null}),/进度未知/);
 assert.equal(timing({}), '');
});
