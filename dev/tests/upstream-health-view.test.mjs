import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source=name=>readFileSync(new URL(`../../monitor/${name}`,import.meta.url),'utf8');
function fixture(){
  const elements=new Map();
  const element=id=>{
    if(!elements.has(id))elements.set(id,{innerHTML:'',hidden:false,value:'balance',options:[]});
    return elements.get(id);
  };
  const ctx=vm.createContext({window:{},document:{getElementById:element,addEventListener(){}},
    esc:value=>String(value??''),fmtNum:String,syncTime:value=>String(value||'未安排'),syncStateClass:x=>x,usageFactsDuration:x=>String(x)});
  vm.runInContext(source('channel_data_status.js'),ctx);
  const js=source('channel_management.js'),end=js.lastIndexOf('})();');
  vm.runInContext(js.slice(0,end)+'\nglobalThis.ui={renderUpstreamStatus,domainCard,cm};\n'+js.slice(end),ctx);
  const page=source('page.html'),start=page.indexOf('function syncRenderChannels('),stop=page.indexOf('function syncRenderCostClosure(',start);
  assert.ok(start>=0&&stop>start);
  vm.runInContext(page.slice(start,stop),ctx);
  return {ctx,ui:ctx.ui,element};
}
const account={configured:true,enabled:true,balance_usd:12.34,status:'ok',balance_fresh:false,balance_effective_status:'stale',last_success_at:1800000000,
  usage_sync_enabled:true,usage_worker_enabled:true,usage_status:'ok',usage_tail_phase:'ok',usage_history_phase:'paused',usage_backfill_next_sync_at:2**62};
test('channel card and account modal retain stale money with quiet, accurate status',()=>{
  const {ui,element}=fixture();
  const domain={key:'fixture',domain:'fixture.example',usage:{},vendors:[],upstream:account};
  const card=ui.domainCard(domain,0,{},false);
  assert.match(card,/<b>\$12\.34<\/b>/);
  assert.match(card,/<em class="cm-domain-metric-note neutral cm-balance-sync-note">余额待更新/);
  ui.renderUpstreamStatus(account);
  const modal=element('cmUpstreamStatus').innerHTML;
  assert.match(modal,/最近余额快照 \$12\.34/);
  assert.match(modal,/数据陈旧/);
  assert.match(modal,/历史补数已暂停，请检测并恢复/);
  assert.doesNotMatch(modal,/历史补数退避重试|Invalid Date/);
});
test('sync page separates a paused history task from a scheduled retry and stale balance',()=>{
  const {ctx,element}=fixture();
  ctx.report={enabled:true,domains:[{domain:'fixture.example',upstream:account}]};
  assert.equal(vm.runInContext('syncRenderChannels(report)',ctx),'bad');
  let html=element('syncChannels').innerHTML;
  assert.match(html,/余额同步正常<\/small><b>0\/1/);
  assert.match(html,/历史任务已暂停<\/small><b>1/);
  assert.match(html,/历史任务退避<\/small><b>0/);
  assert.match(html,/自动重试已暂停，请在账户配置中检测并恢复/);
  assert.doesNotMatch(html,/下次重试|461168601842738/);
  ctx.report.domains[0].upstream={...account,usage_history_phase:'retry',usage_backfill_next_sync_at:1800000100};
  assert.equal(vm.runInContext('syncRenderChannels(report)',ctx),'warn');
  html=element('syncChannels').innerHTML;
  assert.match(html,/历史任务退避<\/small><b>1/);
  assert.match(html,/下次重试 1800000100/);
});
test('Spring per-key history distinguishes pause, retry and published completion',()=>{
  const {ctx,element}=fixture();
  const slot={label:'本机 Key',status:'ok',backfill_last_error:'HTTP 403',backfill_paused:true};
  ctx.report={enabled:true,domains:[{domain:'fixture.example',upstream:{...account,api_key_slots:[slot]}}]};
  vm.runInContext('syncRenderChannels(report)',ctx);
  assert.match(element('syncChannels').innerHTML,/历史任务已暂停/);
  assert.doesNotMatch(element('syncChannels').innerHTML,/历史退避重试/);
  slot.backfill_paused=false;
  vm.runInContext('syncRenderChannels(report)',ctx);
  assert.match(element('syncChannels').innerHTML,/历史退避重试/);
  slot.backfill_done=true;
  vm.runInContext('syncRenderChannels(report)',ctx);
  assert.match(element('syncChannels').innerHTML,/历史已完成/);
  assert.doesNotMatch(element('syncChannels').innerHTML,/历史退避重试/);
});
