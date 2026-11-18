import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

function load(){
  const context=vm.createContext({window:{},esc:v=>String(v),Intl,Date,Number,String,Math,JSON});
  vm.runInContext(readFileSync(new URL('../../monitor/channel_data_status.js',import.meta.url),'utf8'),context);
  return context.window.channelDataStatus;
}
function domain(name,usage,provider='sub2api'){
  return {domain:name,enabled_channels:1,upstream:{configured:true,provider,usage_sync_enabled:true,balance_available:true,balance_fresh:true,balance_verified:true,balance_usd:5,status:'ok'},
    upstream_usage:{available:true,integrity_status:'complete',cost_usd:1,adjusted_cost_available:true,adjusted_cost_usd:1,internal_filter_status:'complete',...usage}};
}
const meta={data_coverage:{complete:true}};

test('a still-settling current-day bill is a note, not a data issue',()=>{
  const s=load();
  const report={meta,domains:[
    domain('settling.example',{provisional:true,complete:false,completed_hours:16,expected_hours:17}),
    domain('settled-tail.example',{provisional:true,complete:false,completed_hours:17,expected_hours:17}),
  ]};
  assert.equal(s.issues(report).length,0);
  assert.deepEqual(JSON.parse(JSON.stringify(s.pendingBills(report).map(r=>r.scope))),['settling.example','settled-tail.example']);
  const html=s.render(report);
  assert.match(html,/用量覆盖与可用账单校验通过/);
  assert.match(html,/2 个上游当日账单仍在结算，金额暂定、后续可能更新，不作为数据异常：settling\.example、settled-tail\.example/);
  assert.doesNotMatch(s.note(report),/后续可能更新/);
});

test('a provisional bill with a real gap or another problem stays an issue',()=>{
  const s=load();
  const gap=domain('gap.example',{provisional:true,complete:false,completed_hours:10,expected_hours:17});
  const other=domain('other.example',{provisional:true,complete:false,completed_hours:17,expected_hours:17,adjusted_cost_available:false});
  const settled=domain('settled.example',{complete:false,completed_hours:16,expected_hours:17});
  const report={meta,domains:[gap,other,settled]};
  const rows=s.issues(report);
  assert.deepEqual(JSON.parse(JSON.stringify(rows.map(r=>r.scope))),['gap.example','other.example','settled.example']);
  assert.match(rows[0].detail,/账单已覆盖 10\/17 小时/);
  assert.match(rows[1].detail,/当前日小时金额/);
  assert.match(rows[2].detail,/账单已覆盖 16\/17 小时/);
  assert.equal(s.pendingBills(report).length,0);
});
