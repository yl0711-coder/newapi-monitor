import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function fixture(fetchImpl = async()=>{throw new Error('offline');}) {
  const elements=new Map();
  const element=id=>{
    if(!elements.has(id)) {
      const classes=new Set();
      elements.set(id,{hidden:false,value:'',textContent:'',innerHTML:'',addEventListener(){},
        classList:{add(...names){names.forEach(n=>classes.add(n));},remove(...names){names.forEach(n=>classes.delete(n));},
          toggle(name,on){if(on)classes.add(name);else classes.delete(name);},contains(name){return classes.has(name);}},
        querySelector(selector){return element(`${id}:${selector}`);}});
    }
    return elements.get(id);
  };
  const timers=[];
  const context=vm.createContext({document:{getElementById:element},window:{},AbortController,URLSearchParams,
    setTimeout(fn,delay){timers.push({fn,delay});return timers.length;},clearTimeout(){},
    // Failed background refresh must keep the existing rendered report.
    fetch:fetchImpl});
  const source=readFileSync(new URL('../../monitor/finance.js',import.meta.url),'utf8');
  const end=source.lastIndexOf('}());');
  assert.ok(end>0,'finance module closure not found');
  vm.runInContext(source.slice(0,end)+'\nglobalThis.fixture={state,scheduleStaleRefresh,load,financeIsPriorStale,financeCacheRefreshNote,renderProfitBridge,operatingProfitBlockers,setGiftEvidence,renderPeriods,renderDays,internalCostDeductionNote,grossCorrectedCost,chartValue};\n'+source.slice(end),context);
  return {api:context.fixture,element,timers};
}

test('daily account bills remain distinct from paired cost and legacy responses',()=>{
  const {api,element}=fixture();
  const money=value=>({micro_usd:String(value*1000000)});
  const statement={upstream_billed_cost:money(11),known_upstream_billed_cost:money(11),paired_corrected_upstream_cost:money(3)};
  api.renderDays([{date:'2026-05-01',statement,bill_coverage:{complete:true},recharge_correction:{cost:null,known_cost:money(5.5)}}]);
  assert.match(element('finDailyRows').innerHTML,/\$11\.00/);
  assert.match(element('finDailyRows').innerHTML,/\$3\.00/);
  assert.match(element('finDailyRows').innerHTML,/\$5\.50/);
  assert.match(element('finDailyRows').innerHTML,/不代表全部成本/);
  assert.match(element('finDailyRows').innerHTML,/包含内部测试；不是已配对客户成本/);
  api.renderDays([{date:'2026-05-01',statement}]);
  assert.doesNotMatch(element('finDailyRows').innerHTML,/\$11\.00/);
  assert.doesNotMatch(element('finDailyRows').innerHTML,/\$5\.50/);
  api.renderDays([]);
  assert.match(element('finDailyRows').innerHTML,/colspan="17"/);
});

test('daily reconciled gross cost shows one total with source breakdown, never invents a total',()=>{
  const {api,element}=fixture();
  const money=value=>({micro_usd:String(value*1000000)});
  const row={date:'2026-05-01',statement:{},recharge_correction:{known_cost:money(5)},ledger_correction:{known_cost:money(7)}};
  api.renderDays([{...row,cost_reconciliation:{status:'reconciled',known_total:money(12)}}]);
  let html=element('finDailyRows').innerHTML;
  assert.match(html,/已取得.*\$12\.00/);
  assert.match(html,/充值 \$5\.00 · 账本 \$7\.00/);
  assert.equal((html.match(/<td[ >]/g)||[]).length,17);
  for(const value of [undefined,{status:'source_mismatch',known_total:money(12)}]) {
    api.renderDays([{...row,cost_reconciliation:value}]);
    html=element('finDailyRows').innerHTML;
    assert.doesNotMatch(html,/\$12\.00/);
    assert.match(html,/充值 \$5\.00 · 账本 \$7\.00/);
    if(value) assert.match(html,/日\/月来源待核对/);
  }
});

test('daily ledger fallback is separate, nullable and explains source mismatch',()=>{
  const {api,element}=fixture();
  const money=value=>({micro_usd:String(value*1000000)});
  api.renderDays([{date:'2026-05-01',statement:{},ledger_correction:{known_cost:money(7),cost:null,status:'incomplete'}}]);
  assert.match(element('finDailyRows').innerHTML,/\$7\.00/);
  assert.match(element('finDailyRows').innerHTML,/与充值修正不重复/);
  api.renderDays([{date:'2026-05-01',statement:{},ledger_correction:{status:'source_mismatch'}}]);
  assert.match(element('finDailyRows').innerHTML,/日\/月来源待核对/);
  assert.doesNotMatch(element('finDailyRows').innerHTML,/\$7\.00/);
  api.renderDays([{date:'2026-05-01',statement:{}}]);
  assert.doesNotMatch(element('finDailyRows').innerHTML,/日\/月来源待核对/);
});

test('daily internal evidence preserves unresolved counts and cannot masquerade as customer net cost',()=>{
  const {api,element}=fixture();
  const money={micro_usd:'4000000'};
  api.renderDays([{date:'2026-05-01',internal_cost_complete:false,statement:{known_internal_test_upstream_cost:money,internal_test_mixed_rows:2,internal_test_unverified_pairs:3,internal_cost_deduction_status:'cost_source_missing'}}]);
  let html=element('finDailyRows').innerHTML;
  assert.match(html,/\$4\.00<small>已识别/);
  assert.match(html,/混用待拆分 2 项/);
  assert.match(html,/未核验 3 项/);
  assert.match(html,/配对账本扣减待核对/);
  api.renderDays([{date:'2026-05-01',internal_cost_complete:true,statement:{internal_test_upstream_cost:money,known_internal_test_upstream_cost:money}}]);
  html=element('finDailyRows').innerHTML;
  assert.match(html,/\$4\.00/);
  assert.doesNotMatch(html,/已识别|待补|待拆分|扣减待核对/);
  api.renderDays([{date:'2026-05-01',internal_cost_complete:false,statement:{}}]);
  assert.match(element('finDailyRows').innerHTML,/内部成本证据待补/);
  api.renderDays([{date:'2026-05-01',internal_cost_complete:true,statement:{}}]);
  assert.match(element('finDailyRows').innerHTML,/已核验，未发现内部测试成本/);
  api.renderDays([{date:'2026-05-01',statement:{}}]);
  assert.doesNotMatch(element('finDailyRows').innerHTML,/已核验，未发现|证据待补/);
});

test('daily unresolved costs explain reasons without exposing raw unknown codes',()=>{
  const {api,element}=fixture();
  api.renderDays([{date:'2026-05-01',statement:{internal_test_unverified_pairs:4},internal_cost_complete:false,
    internal_cost_unverified_reasons:{channel_cost_not_published:2,upstream_cost_missing:1,'<img src=x onerror=alert(1)>':1}}]);
  const html=element('finDailyRows').innerHTML;
  assert.match(html,/尚无渠道成本发布 2 项/);
  assert.match(html,/渠道上游成本缺失 1 项/);
  assert.match(html,/其他原因待核对 1 项/);
  assert.doesNotMatch(html,/<img|onerror|\$0\.00/);
  api.renderDays([{date:'2026-05-01',statement:{internal_test_unverified_pairs:4}}]);
  assert.match(element('finDailyRows').innerHTML,/未核验 4 项/);
  assert.doesNotMatch(element('finDailyRows').innerHTML,/尚无渠道成本发布/);
});

test('finance polling stops honestly after bounded attempts, preserving report',()=>{
  const {api,element,timers}=fixture();
  element('finPeriodRows').innerHTML='previous verified data';
  api.state.stale=true;
  for(let n=0;n<6;n++)api.scheduleStaleRefresh();
  assert.deepEqual(timers.map(t=>t.delay),[1000,2000,4000,8000,16000]);
  assert.match(element('finCacheUpdate').textContent,/更新尚未完成/);
  assert.equal(element('finPeriodRows').innerHTML,'previous verified data');
});

test('prior-range finance snapshot is never presented as the current interval',()=>{
  const {api}=fixture();
  assert.equal(api.financeIsPriorStale({_cache_status:'persistent-prior-stale-refreshing'}),true);
  assert.equal(api.financeIsPriorStale({_cache_status:'persistent-hit'}),false);
  const note=api.financeCacheRefreshNote({_cache_status:'persistent-prior-stale-refreshing',to:1789959600});
  assert.match(note,/仅显示截至.*较早区间/);
  assert.match(note,/请勿当作当前结果/);
  assert.doesNotMatch(api.financeCacheRefreshNote({_cache_status:'persistent-hit',to:1789959600}),/较早区间/);
});

test('unmatched internal deductions explain missing net cost without hiding gross facts',()=>{
  const {api}=fixture();
  const statement={internal_cost_deduction_status:'cost_source_missing',known_raw_corrected_upstream_cost:{micro_usd:'33000000'}};
  assert.match(api.internalCostDeductionNote(statement),/原成本保留.*暂不发布/);
  assert.equal(statement.known_raw_corrected_upstream_cost.micro_usd,'33000000');
  assert.equal(api.internalCostDeductionNote({}),'');
  assert.equal(api.internalCostDeductionNote(undefined),'');
});

test('finance background failure preserves last report and bounded retry budget',async()=>{
  const {api,element,timers}=fixture();
  api.state.stale=true;api.state.refreshAttempts=4;
  element('finPeriodRows').innerHTML='previous verified data';
  await api.load(false,true);
  assert.equal(api.state.refreshAttempts,5);
  assert.equal(timers.length,1);
  assert.equal(element('finPeriodRows').innerHTML,'previous verified data');
  assert.match(element('finCacheUpdate').textContent,/更新失败.*offline/);
  await api.load(false,true);
  assert.equal(timers.length,1);
  assert.match(element('finCacheUpdate').textContent,/更新尚未完成/);
});

test('finance new user query receives a new retry budget',async()=>{
  const {api}=fixture();
  api.state.refreshAttempts=5;
  await api.load();
  assert.equal(api.state.refreshAttempts,0);
});

test('profit bridge deducts gross corrected cost, not customer-only cost',()=>{
  const {api,element}=fixture();
  const amount=value=>({micro_usd:String(Math.round(Number(value)*1000000))});
  const statement={operating_revenue:amount('100.00'),raw_corrected_upstream_cost:amount('60.00'),
    corrected_upstream_cost:amount('50.00'),aws_infrastructure_cost:amount('5.00'),operating_profit:amount('35.00')};
  api.renderProfitBridge({},statement);
  assert.match(element('finBridgeUpstream').textContent,/60\.00/);
  assert.match(element('finBridgeProfit').textContent,/35\.00/);
  assert.match(element('finBridgeFootnote').textContent,/不重复扣除/);
  assert.equal(api.operatingProfitBlockers({},statement).length,0);
  delete statement.raw_corrected_upstream_cost;
  assert.ok(api.operatingProfitBlockers({},statement).some(s=>s.includes('修正上游总成本未闭合')));
});

test('monthly report and trend use gross corrected cost including internal spend',()=>{
  const {api,element}=fixture();
  const amount=value=>({micro_usd:String(Math.round(Number(value)*1000000))});
  const statement={raw_corrected_upstream_cost:amount(60),known_raw_corrected_upstream_cost:amount(60),
    corrected_upstream_cost:amount(50),known_corrected_upstream_cost:amount(50),
    internal_test_upstream_cost:amount(10),known_internal_test_upstream_cost:amount(10)};
  api.renderPeriods([{period:'2026-09',statement}]);
  const html=element('finPeriodRows').innerHTML;
  assert.match(html,/>\$60\.00<\/td><td class="num">\$10\.00/);
  assert.doesNotMatch(html,/>\$50\.00<\/td>/);
  assert.equal(api.grossCorrectedCost(statement).micro_usd,amount(60).micro_usd);
});

test('monthly trend never converts missing money into zero',()=>{
  const {api}=fixture();
  assert.equal(api.chartValue(null),null);
  assert.equal(api.chartValue(undefined),null);
  assert.equal(api.chartValue({}),null);
  assert.equal(api.chartValue({micro_usd:null}),null);
  assert.equal(api.chartValue({micro_usd:''}),null);
  assert.equal(api.chartValue({micro_usd:'invalid'}),null);
  assert.equal(api.chartValue({micro_usd:'0'}),0);
  assert.equal(api.chartValue({micro_usd:'1250000'}),1.25);
});

test('gift evidence headline reflects settlement completeness, not just collected hours',()=>{
  const fullHours={expected_hours:10,user_completed_hours:10,credit_completed_hours:10};
  const cases=[
    {value:{...fullHours,complete:false,scope_unknown_events:2},label:'待补齐',state:'warn',note:/2 条历史分组依据待补/},
    {value:{...fullHours,complete:false,expected_boundary_user_hours:4,completed_boundary_user_hours:3},label:'待补齐',state:'warn',note:/顺序 3\/4/},
    {value:{...fullHours,complete:false,latest_hour_pending:true},label:'待补齐',state:'warn',note:/当前小时待闭合/},
    {value:{...fullHours,credit_completed_hours:5,complete:false},label:'待补齐',state:'warn',note:/小时覆盖 50\.0%/},
    {value:fullHours,label:'待补齐',state:'warn',note:/小时覆盖 100\.0%/},
    {value:{...fullHours,complete:true},label:'已完成',state:'ready',note:/小时覆盖 100\.0%/},
    {value:null,label:'—',state:'missing',note:/没有可核验证据/},
  ];
  const {api,element}=fixture();
  for(const {value,label,state,note} of cases){
    api.setGiftEvidence(value);
    assert.equal(element('finGiftEvidence:b').textContent,label);
    for(const cls of ['warn','ready','missing'])assert.equal(element('finGiftEvidence').classList.contains(cls),cls===state);
    assert.match(element('finGiftEvidence:span').textContent,note);
  }
});

test('consumption breakdown uses backend gross/refund values and never upgrades partial evidence',()=>{
  const {api,element}=fixture();
  const amount=n=>({micro_usd:String(n*1000000)});
  const statement={gross_user_consumption:amount(100),user_refunds:amount(20),user_consumption:amount(80)};
  api.renderProfitBridge({},statement);
  assert.match(element('finBridgeGross').textContent,/100\.00/);
  assert.match(element('finBridgeRefund').textContent,/20\.00/);
  assert.match(element('finBridgeConsumption').textContent,/80\.00/);
  assert.equal(element('finBridgeGrossState').textContent,'完整');
  statement.user_consumption=null;statement.known_user_consumption=amount(80);
  api.renderProfitBridge({},statement);
  assert.equal(element('finBridgeGrossState').textContent,'已知部分');
  assert.equal(element('finBridgeRefundState').textContent,'已知部分');
  assert.equal(element('finBridgeRevenue').textContent,'—');
  statement.gross_user_consumption=null;statement.user_refunds=null;
  api.renderProfitBridge({},statement);
  assert.equal(element('finBridgeGross').textContent,'—');
  statement.user_consumption=amount(0);statement.gross_user_consumption=amount(0);statement.user_refunds=amount(0);
  api.renderProfitBridge({},statement);
  assert.match(element('finBridgeGross').textContent,/0\.00/);
  assert.equal(element('finBridgeGrossState').textContent,'完整');
});

test('real local report responses refresh gift coverage and all visible statements together',
  {skip: !process.env.MONITOR_GIFT_REPORT_ACCEPTANCE_OUTPUT && 'requires offline Go report acceptance artifact'}, async()=>{
  const reports=JSON.parse(readFileSync(process.env.MONITOR_GIFT_REPORT_ACCEPTANCE_OUTPUT,'utf8'));
  const queue=['before','partial_stale','partial','complete_stale','complete','restarted'];
  const {api,element}=fixture(async()=>{
    const entry=reports[queue.shift()];
    assert.ok(entry,'missing backend acceptance response');
    return {ok:true,status:200,headers:{get:name=>name==='X-Monitor-Finance-Cache'?entry.cache:null},
      json:async()=>structuredClone(entry.report)};
  });
  for (let i=0;i<6;i++) {
    await api.load(false,i>0);
    const complete=i>=4;
    assert.equal(element('finGiftEvidence:b').textContent,complete?'已完成':'待补齐');
    assert.equal(element('finBridgeRevenue').textContent,complete?'$70.00':'—');
    assert.equal(element('finBridgeProfit').textContent,'—','missing costs must not become zero');
    assert.match(element('finBridgeConsumption').textContent,/165\.00/);
    assert.equal(api.state.stale,i===1||i===3);
    assert.equal(element('finStatus').innerHTML.includes('后台更新中'),i===1||i===3);
    for (const id of ['finPeriodRows','finDailyRows']) {
      assert.match(element(id).innerHTML,/165\.00/);
      assert.equal(element(id).innerHTML.includes('$95.00'),complete);
      assert.equal(element(id).innerHTML.includes('$70.00'),complete);
    }
    assert.match(element('finCostRows').innerHTML,/report-gift\.example/);
    assert.match(element('finCostRows').innerHTML,/165\.00/);
  }
  assert.equal(queue.length,0);
});
