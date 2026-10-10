import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function healthView() {
  const elements=new Map(), replies=[];
  const element=id=>{
    if(!elements.has(id)) elements.set(id,{textContent:'',innerHTML:'',hidden:false,addEventListener(){}});
    return elements.get(id);
  };
  const window={};
  const context=vm.createContext({window,document:{getElementById:element,hidden:false,addEventListener(){}},
    AbortController,setTimeout(){return 1},clearTimeout(){},
    async fetch(){assert.ok(replies.length,'unexpected fetch');return {ok:true,json:async()=>replies.shift()}}});
  const source=readFileSync(new URL('../../monitor/customer_health.js',import.meta.url),'utf8');
  const end=source.lastIndexOf('})();');
  vm.runInContext(source.slice(0,end)+'\nglobalThis.healthTest={render,ch,load};\n'+source.slice(end),context);
  const {render,ch,load}=context.healthTest;
  return {element,ch,window,replies,load,render(report){ch.report=report;render()}};
}

const through=Date.UTC(2026,9,8,7,56)/1000;
function report({ready=false,available=true,state='syncing',note='截至 15:56（CST），尾部同步中',total=10}={}) {
  return {day:'2026-10-08',time_zone:'Asia/Shanghai',generated_at:through+160,
    from_ts:Date.UTC(2026,9,7,16)/1000,to_ts:through,
    collection:{ready,metrics_available:available,coverage_complete:available,state,note,through_ts:through,target_ts:through+60},
    rows:[{group_id:1,company:'连续公司',members:[],metrics_ready:available,metrics_state:state,metrics_note:note,
      total,success:9,anomaly:0,failed:1,stability_anomaly:0,stability_failed:1,stability_pct:90,
      primary_models:[{name:'known-model',requests:total,share_pct:100}],fault:'unknown',fault_confidence:'none',
      reason:'已知错误责任待判',channel_scope:'unknown',scope_note:'证据不足',today_spend_usd:null,spend_state:'unavailable'}]};
}

test('certified prefix stays visible while target advances, on refresh and on reentry',async()=>{
  const view=healthView();
  view.replies.push(report({ready:true}));
  await view.load();
  const original=view.element('chRows').innerHTML;
  assert.match(original,/>10</);
  assert.match(original,/known-model/);
  assert.match(original,/90\.0%/);

  view.replies.push(report({ready:false}));
  await view.load();
  assert.equal(view.element('chRows').innerHTML,original,'one-minute target advance must not clear usable metrics');
  assert.match(view.element('chMeta').textContent,/截至 15:56.*尾部同步中/);

  view.ch.inited=true;
  view.window.customerHealthDeactivate();
  view.replies.push(report({ready:false}));
  view.window.customerHealthActivate();
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(view.element('chRows').innerHTML,original,'reentry must render the server-proven prefix again');
  assert.doesNotMatch(view.element('chRows').innerHTML,/回算中|指标未完成/);

  view.replies.push(report({ready:true,total:11,note:'截至 15:57（CST），尾部同步中（已追平定稿目标）'}));
  await view.load();
  assert.match(view.element('chRows').innerHTML,/>11</);
  assert.match(view.element('chMeta').textContent,/截至 15:57/);
});

for(const [state,label] of [['initial_backfill','首次回算中'],['coverage_gap','覆盖有缺口'],
  ['policy_backfill','统计口径更新中'],['source_unavailable','数据来源不可用']]) {
  test(`unavailable ${state} shows its real reason, never stale metrics or false zero`,()=>{
    const view=healthView();
    view.ch.expanded.add(1);
    view.render(report({available:false,state,note:`${label}，<不可用>`}));
    const html=view.element('chRows').innerHTML;
    assert.match(html,new RegExp(label));
    assert.match(html,/&lt;不可用&gt;/);
    assert.doesNotMatch(html,/known-model|90\.0%|<b>10<|<b>0<|lc-fault-tag/);
    assert.doesNotMatch(html,/<不可用>/);
  });
}

test('source failure with a usable local prefix is visibly different from normal synchronization',()=>{
  const view=healthView();
  view.render(report({state:'source_failed',note:'采集失败；截至 15:56（CST），仅展示已确认数据，不代表实时完整数据'}));
  assert.match(view.element('chRows').innerHTML,/>10</);
  assert.match(view.element('chMeta').textContent,/采集失败.*截至 15:56.*不代表实时完整数据/);
  assert.doesNotMatch(view.element('chMeta').textContent,/尾部同步中/);
});

test('zero is shown only for a certified visible interval and labelled as that interval',()=>{
  const view=healthView(), data=report({total:0});
  data.rows[0].success=0;
  data.rows[0].failed=0;
  data.rows[0].stability_pct=null;
  view.render(data);
  assert.match(view.element('chRows').innerHTML,/>0</);
  assert.match(view.element('chRows').innerHTML,/统计窗口内无记录/);
});
