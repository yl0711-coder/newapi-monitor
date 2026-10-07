import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source=readFileSync(new URL('../../monitor/channel_data_status.js',import.meta.url),'utf8');
function fixture(){const ctx=vm.createContext({window:{}});vm.runInContext(source,ctx);return ctx.window.channelDataStatus;}
const freshAccount={configured:true,enabled:true,balance_worker_enabled:true,balance_usd:0,balance_effective_status:'ok',balance_fresh:true,last_success_at:1800000000};
test('balance snapshot freshness never invents a zero or promotes stale/unknown data',()=>{
  const api=fixture();
  assert.equal(api.balanceSnapshot(freshAccount).verified,true);
  assert.equal(api.balanceSnapshot(freshAccount).note,'');
  for(const value of [null,undefined,'',NaN])assert.equal(api.balanceSnapshot({...freshAccount,balance_usd:value}).known,false);
  for(const patch of [
    {balance_fresh:false,balance_effective_status:'stale'},
    {balance_fresh:undefined},
    {balance_effective_status:'error'},
    {balance_effective_status:'paused'},
    {balance_effective_status:'global_off',balance_worker_enabled:false},
    {enabled:false,balance_effective_status:'disabled'},
  ]){
    const account={...freshAccount,...patch,balance_usd:-2};
    const snapshot=api.balanceSnapshot(account);
    assert.equal(snapshot.known,true);assert.equal(snapshot.verified,false);
    assert.match(snapshot.note,/上游官网核对/);
    assert.equal(account.balance_usd,-2);
  }
});
test('stale balance with last successful worker state is listed in data sync status',()=>{
  const api=fixture(),report={meta:{data_coverage:{complete:true}},domains:[{domain:'fixture.example',enabled_channels:0,upstream:{...freshAccount,status:'ok',balance_fresh:false,balance_effective_status:'stale'}}]};
  const issues=api.issues(report);
  assert.equal(issues.length,1);assert.match(issues[0].detail,/余额待更新/);
  report.domains[0].upstream=freshAccount;
  assert.equal(api.issues(report).length,0);
});
