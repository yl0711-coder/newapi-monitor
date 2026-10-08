import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function fixture(){
  const elements=new Map();
  const element=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',querySelector(){return null},removeAttribute(){}});return elements.get(id)};
  const context=vm.createContext({window:{},document:{addEventListener(){},getElementById:element}});
  vm.runInContext(readFileSync(new URL('../../monitor/channel_data_status.js',import.meta.url),'utf8'),context);
  const js=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8'),end=js.lastIndexOf('})();');
  vm.runInContext(js.slice(0,end)+'\nglobalThis.api={cm,presentedBusinessCost,adjustedCostSummary,adjustedCostCoverageNote,upstreamCostBases,upstreamAggregateLabel,domainCard,render};\n'+js.slice(end),context);
  return {api:context.api,element};
}
function usage(overrides={}){return {available:true,complete:true,integrity_status:'complete',cost_usd:120,adjusted_cost_available:false,adjusted_cost_usd:0,adjusted_cost_status:'missing_history',
  adjusted_cost_breakdown:{known_cost_usd:2,unresolved_bill_usd:100,known_buckets:1,unresolved_buckets:1},...overrides}}
function domain(u=usage()){
  const localUsage={requests:1,tokens:1,cost_usd:5};
  return {key:'test',domain:'test.example',configured:true,
    vendors:[{name:'test',channels:[{id:1,name:'test-channel',current:true,status:1,configured_groups:['test-group'],groups:[],usage:localUsage}]}],
    usage:localUsage,upstream:{configured:true,usage_sync_enabled:true},upstream_usage:u};
}

test('partial subtotal is displayable only as raw cost; publication flags stay false',()=>{
  const {api}=fixture(),u=usage(),before=JSON.stringify(u);
  const shown=api.presentedBusinessCost(u,true,'raw');
  assert.equal(shown.available,true);assert.equal(shown.value,2);assert.equal(shown.partial,true);
  assert.equal(api.presentedBusinessCost(u,true,'business').available,false);
  assert.equal(JSON.stringify(u),before);
  assert.match(api.adjustedCostCoverageNote(u,'raw'),/已知部分，非完整成本.*\$100\.00.*历史比例缺失/);
  assert.equal(api.upstreamCostBases([domain(u)]).adjusted,'raw');
});
test('summary sums each account once and separates complete accounts from visible accounts',()=>{
  const {api}=fixture();
  const all=[domain(),domain(usage({adjusted_cost_available:true,adjusted_cost_usd:12,adjusted_cost_status:'complete',adjusted_cost_breakdown:null})),domain({available:false})];
  const visible=all.filter(d=>api.presentedBusinessCost(d.upstream_usage,true,'raw').available);
  assert.equal(api.upstreamAggregateLabel(visible,true,'raw'),'$14.00');
  assert.match(api.adjustedCostSummary(all,'raw'),/可显示 2\/3.*完整 1\/3.*已知部分.*\$100\.00/);
});
test('invalid integrity, unknown subscription and malformed/legacy metadata cannot produce a subtotal',()=>{
  const {api}=fixture();
  for(const changes of [{available:false},{integrity_status:'invalid_amount'},{integrity_status:'overlapping_buckets'},{integrity_status:'window_mismatch'},
    {adjusted_cost_status:'subscription_cost_unknown'},{adjusted_cost_breakdown:null},
    ...[null,undefined,NaN,Infinity,-1].map(v=>({adjusted_cost_breakdown:{...usage().adjusted_cost_breakdown,known_cost_usd:v}})),
    {adjusted_cost_breakdown:{...usage().adjusted_cost_breakdown,known_buckets:0}},
    {adjusted_cost_breakdown:{...usage().adjusted_cost_breakdown,known_buckets:1.5}}]){
    assert.equal(api.presentedBusinessCost(usage(changes),true,'raw').available,false,JSON.stringify(changes));
  }
});
test('verified zero differs from no known bucket and incomplete time coverage is not complete',()=>{
  const {api}=fixture();
  const zero=usage({adjusted_cost_breakdown:{...usage().adjusted_cost_breakdown,known_cost_usd:0}});
  assert.equal(api.presentedBusinessCost(zero,true).value,0);
  const sparse=usage({complete:false,adjusted_cost_available:true,adjusted_cost_usd:12,adjusted_cost_breakdown:null});
  assert.equal(api.presentedBusinessCost(sparse,true).partial,true);
  assert.match(api.adjustedCostSummary([domain(sparse)],'raw'),/完整 0\/1/);
  assert.match(api.adjustedCostCoverageNote(sparse,'raw'),/尚未齐全或未定稿/);
});
test('real card and summary render small partial notes without publishing profit',()=>{
  const {api,element}=fixture(),d=domain();
  api.cm.report={domains:[d],meta:{},finance:{},summary:{usage:d.usage}};
  const html=api.domainCard(d,0,d.usage,false);
  assert.match(html,/cm-domain-upstream-adjusted[^]*?<b>\$2\.00<\/b>/);
  assert.match(html,/<em class="cm-domain-metric-note neutral">[^<]*已知部分，非完整成本/);
  api.render();
  assert.match(element('cmSummary').innerHTML,/上游修正成本汇总<\/small><b>\$2\.00/);
  assert.match(element('cmSummary').innerHTML,/完整 0\/1/);
  assert.doesNotMatch(element('cmSummary').innerHTML,/精确毛利润/);
  api.cm.filters.group='test-group';api.render();
  assert.match(element('cmSummary').innerHTML,/上游修正成本汇总<\/small><b>—/);
});
test('natural-day fallback stays outside the selected-hour summary',()=>{
  const {api}=fixture();
  const d=domain({available:true,integrity_status:'window_mismatch'});
  d.natural_day_bill={from_ts:1788192000,to_ts:1788278400,usage:usage()};
  api.cm.report={domains:[d],finance:{}};
  assert.match(api.domainCard(d,0,d.usage,false),/上游修正成本<\/small><b>\$2\.00/);
  assert.equal(api.presentedBusinessCost(d.upstream_usage,true).available,false);
});
