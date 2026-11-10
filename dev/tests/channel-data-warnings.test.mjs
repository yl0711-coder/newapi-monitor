import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

test('sampler gaps are listed as data issues and noted on the channel page',()=>{
  const context=vm.createContext({window:{},esc:v=>String(v),Intl,Date,Number,String,Math,JSON});
  vm.runInContext(readFileSync(new URL('../../monitor/channel_data_status.js',import.meta.url),'utf8'),context);
  const report={meta:{data_coverage:{complete:true}},domains:[],data_warnings:[{kind:'internal_exceeds_sampled',channel_id:195,group:'internal',missing_requests:4,missing_tokens:16946,hours:[1794211200]}]};
  const rows=context.window.channelDataStatus.issues(report);
  const row=rows.find(r=>r.scope.includes('#195'));
  assert.ok(row);assert.match(row.detail,/采样缺失/);assert.match(row.detail,/4 次请求/);assert.match(row.detail,/缺口小时/);
  assert.match(context.window.channelDataStatus.note(report),/1 个渠道×分组的采样数据缺失/);
  assert.doesNotMatch(context.window.channelDataStatus.note({meta:{data_coverage:{complete:true}},domains:[]}),/采样数据缺失/);
});

test('too many gap hours: says so instead of listing a partial set',()=>{
  const context=vm.createContext({window:{},esc:v=>String(v),Intl,Date,Number,String,Math,JSON});
  vm.runInContext(readFileSync(new URL('../../monitor/channel_data_status.js',import.meta.url),'utf8'),context);
  const report={meta:{data_coverage:{complete:true}},domains:[],data_warnings:[{kind:'internal_exceeds_sampled',channel_id:7,group:'g',missing_requests:1,missing_tokens:1,hours_truncated:true}]};
  const row=context.window.channelDataStatus.issues(report).find(r=>r.scope.includes('#7'));
  assert.match(row.detail,/缺口小时较多，未逐一列出/);
});
