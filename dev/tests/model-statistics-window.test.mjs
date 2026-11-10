import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function modelView() {
  const elements=new Map();
  const element=id=>{
    if(!elements.has(id)) {
      const classes=new Set();
      elements.set(id,{textContent:'',innerHTML:'',hidden:false,
        classList:{toggle(name,on){if(on)classes.add(name);else classes.delete(name)},contains(name){return classes.has(name)}}});
    }
    return elements.get(id);
  };
  const source=readFileSync(new URL('../../monitor/model_statistics.js',import.meta.url),'utf8');
  const end=source.lastIndexOf('})();');
  assert.ok(end>0);
  const context=vm.createContext({window:{},document:{getElementById:element}});
  vm.runInContext(readFileSync(new URL('../../monitor/view_refresh.js',import.meta.url),'utf8'),context);
  context.window.MonitorView.preserve=(_root,update)=>update();
  vm.runInContext(source.slice(0,end)+'\nglobalThis.modelTest={render,state};\n'+source.slice(end),context);
  const {render,state}=context.modelTest;
  state.expanded.add('test-model');
  state.expandedGroups.add(JSON.stringify(['test-model','test-group']));
  return {element,render(report){state.report=report;render(report)}};
}

const windowTo=Date.UTC(2026,8,30,7,20)/1000;
function report(lag=60) {
  const metrics={requests:300,routed_requests:300,unavailable_channel_requests:0,
    frt_observed:211,frt_p50_ms:1300,frt_p95_ms:2700,frt_p99_ms:3100,frt_max_ms:4200,frt_over_3s:7};
  return {
    from_ts:windowTo-86400,to_ts:windowTo,actual_through_ts:windowTo,target_ts:windowTo+lag,lag_seconds:lag,
    timing:{finalize_delay_seconds:120,alignment_seconds:60,collection_poll_seconds:60},
    source:{facts_complete:true,requests_complete:true,frt_complete:true,coverage_status:'complete'},
    models:[{...metrics,model:'test-model',groups:[{...metrics,group:'test-group',
      channels:[{...metrics,channel_id:42,channel_name:'test-channel'}],customers:[]}]}]
  };
}

for(const minutes of [1,3]) {
  test(`a complete window ${minutes} minutes behind keeps requests at every level and its actual cutoff without FRT`,()=>{
    const view=modelView();
    view.render(report(minutes*60));
    const banner=view.element('msCoverage');
    assert.match(banner.textContent,/^数据截至 .*15:20:00。封口等待 120 秒/);
    assert.match(banner.textContent,minutes===1?/正常等待：距封口目标 60 秒/:/采集积压 180 秒/);
    assert.match(banner.textContent,/统计区间 .*2026\/9\/29 15:20:00 至 2026\/9\/30 15:20:00/);
    assert.doesNotMatch(banner.textContent,/未完整|尚未全部确认/);
    assert.doesNotMatch(banner.textContent,/数据延迟\/覆盖不完整/);
    assert.equal(banner.classList.contains('incomplete'),minutes>1);
    const html=view.element('msModels').innerHTML;
    assert.doesNotMatch(html,/FRT|P50|P95|P99|超3秒|1\.3秒|2\.7秒|>211</);
    assert.match(html,/渠道请求统计/);
    assert.equal([...html.matchAll(/<td>300<\/td>/g)].length,5);
    assert.equal([...html.matchAll(/colspan="5"/g)].length,2);
    assert.match(html,/data-ms-channel-id="42"/);
  });
}

test('freshness and original overall coverage remain while FRT presentation is absent',()=>{
  const view=modelView(),data=report(60);
  data.source={facts_complete:false,requests_complete:true,frt_complete:false,ttft_complete:true,coverage_status:'incomplete'};
  view.render(data);
  const banner=view.element('msCoverage');
  assert.match(banner.textContent,/正常等待：距封口目标 60 秒/);
  assert.match(banner.textContent,/请求事实：完整/);
  assert.equal(banner.classList.contains('incomplete'),true);
  const html=view.element('msModels').innerHTML;
  assert.doesNotMatch(html,/1\.3秒|2\.7秒|3\.1秒|4\.2秒|7次|>211</);
  assert.equal([...html.matchAll(/<td>—<\/td>/g)].length,0,'removed FRT columns leave no empty cells');
});

test('legacy responses without freshness fields retain coverage proof without inventing an update time',()=>{
  for(const missing of [['target_ts','lag_seconds'],['target_ts'],['lag_seconds']]) {
    const view=modelView(),data=report();
    for(const field of missing)delete data[field];
    view.render(data);
    assert.doesNotMatch(view.element('msCoverage').textContent,/数据更新到|采集进度比目标落后/);
    assert.match(view.element('msCoverage').textContent,/数据覆盖已确认/);
    assert.match(view.element('msModels').innerHTML,/渠道请求统计/);
  }
});

test('missing independent FRT proof does not remove request details',()=>{
  const view=modelView(),data=report();
  delete data.source.frt_complete;
  view.render(data);
  assert.doesNotMatch(view.element('msCoverage').textContent,/FRT|TTFT/);
  assert.match(view.element('msModels').innerHTML,/test-channel/);
  assert.doesNotMatch(view.element('msModels').innerHTML,/1\.3秒|2\.7秒|3\.1秒|4\.2秒|7次|>211</);
});

test('collection failure warns even with zero lag while preserving historical request counts',()=>{
  const view=modelView(),data=report(0);
  data.collection_failed=true;
  view.render(data);
  assert.match(view.element('msCoverage').textContent,/最近一次采集失败/);
  assert.match(view.element('msCoverage').textContent,/数据覆盖已确认/);
  assert.equal(view.element('msCoverage').classList.contains('incomplete'),true);
  assert.match(view.element('msModels').innerHTML,/<td>300<\/td>/);
  data.collection_failed=false;
  view.render(data);
  assert.doesNotMatch(view.element('msCoverage').textContent,/采集失败/);
  assert.equal(view.element('msCoverage').classList.contains('incomplete'),false);
});

test('a caught-up closed window does not show a pending tail',()=>{
  const view=modelView();
  view.render(report(0));
  assert.doesNotMatch(view.element('msCoverage').textContent,/采集进度比目标落后/);
  assert.match(view.element('msModels').innerHTML,/渠道请求统计/);
});

test('model highlighting uses a strictly greater than 35% unavailable share',()=>{
  for(const [unavailable,highlighted] of [[0,false],[3499,false],[3500,false],[3501,true],[3600,true],[10000,true]]) {
    const view=modelView(),data=report();
    Object.assign(data.models[0],{requests:10000,routed_requests:10000-unavailable,unavailable_channel_requests:unavailable});
    view.render(data);
    const html=view.element('msModels').innerHTML;
    const modelRow=html.match(/<tr class="ms-model-row[^"]*"[^>]*>/)?.[0];
    assert.ok(modelRow);
    assert.equal(modelRow.includes('ms-model-row-high-unavailable'),highlighted,`unavailable=${unavailable}/10000`);
    assert.equal(modelRow.includes('占该模型总请求次数超过35%'),highlighted);
    assert.doesNotMatch(html,/ms-ttft-slow|ms-ttft-row-slow|超过40%/);
  }
});

test('the denominator is this model total, not routed requests or all models',()=>{
  const view=modelView(),data=report();
  data.models=[
    {model:'low',requests:100,routed_requests:70,unavailable_channel_requests:30,groups:[]},
    {model:'high',requests:100,routed_requests:64,unavailable_channel_requests:36,groups:[]},
    {model:'other',requests:10000,routed_requests:10000,unavailable_channel_requests:0,groups:[]}
  ];
  view.render(data);
  const rows=[...view.element('msModels').innerHTML.matchAll(/<tr class="ms-model-row([^"]*)"[^>]*>.*?<strong>(.*?)<\/strong>/g)];
  assert.equal(rows.length,3);
  assert.deepEqual(rows.filter(row=>row[1].includes('ms-model-row-high-unavailable')).map(row=>row[2]),['high']);
});

test('zero or missing request counts never highlight a model',()=>{
  for(const counts of [{requests:0,unavailable_channel_requests:0},{requests:0,unavailable_channel_requests:5},
    {unavailable_channel_requests:5},{requests:100}]) {
    const view=modelView(),data=report();
    data.models=[{model:'test-model',groups:[],...counts}];
    view.render(data);
    assert.doesNotMatch(view.element('msModels').innerHTML,/ms-model-row-high-unavailable/);
  }
});

test('model share highlighting does not depend on FRT coverage',()=>{
  for(const complete of [true,false]) {
    const view=modelView(),data=report();
    data.source.frt_complete=complete;
    Object.assign(data.models[0],{requests:100,routed_requests:64,unavailable_channel_requests:36});
    view.render(data);
    assert.match(view.element('msModels').innerHTML,/ms-model-row-high-unavailable/);
  }
});

test('slow legacy and canonical FRT fields are hidden while request rows remain',()=>{
  for(const legacy of [false,true]) {
    const view=modelView(),data=report();
    const model=data.models[0],group=model.groups[0],channel=group.channels[0];
    for(const item of [model,group,channel]) {
      Object.assign(item,{frt_p50_ms:5000,frt_p95_ms:5500,frt_p99_ms:6000,frt_max_ms:7000});
      if(legacy)for(const field of Object.keys(item).filter(key=>key.startsWith('frt_'))) {
        item[field.replace('frt_','ttft_')]=item[field];delete item[field];
      }
    }
    view.render(data);
    const html=view.element('msModels').innerHTML;
    assert.doesNotMatch(html,/ms-model-row-high-unavailable|ms-ttft-slow|ms-ttft-row-slow/);
    assert.doesNotMatch(html,/FRT|5\.0秒|5\.5秒|6\.0秒|7\.0秒|7次/);
    assert.match(html,/渠道请求统计/);
  }
});

test('page explanation and stylesheet contain only the model share highlight rule',()=>{
  const page=readFileSync(new URL('../../monitor/page.html',import.meta.url),'utf8');
  const css=readFileSync(new URL('../../monitor/model_statistics.css',import.meta.url),'utf8');
  assert.match(page,/仅当无可用渠道次数占该模型总请求次数超过35%时，模型行标红/);
  assert.doesNotMatch(page,/FRT P95 超过 3 秒或 FRT 超过 3 秒的请求会标红/);
  assert.match(css,/\.ms-model-row-high-unavailable td\{/);
  assert.doesNotMatch(css,/ms-ttft-slow|ms-ttft-row-slow/);
});

test('channel identity, customer details and request order survive presentation cleanup',()=>{
 const view=modelView(),data=report();const group=data.models[0].groups[0];
 group.channels=[{channel_id:-1,channel_kind:'unavailable',requests:16},{channel_id:0,channel_kind:'routed_unknown',requests:17},{channel_id:42,channel_name:'same-name',requests:267}];
 group.customers=[{customer_id:31,customer_name:'fixture-customer',requests:250},{customer_id:0,customer_name:'unknown',requests:50}];
 data.source.note='已路由请求：已覆盖；FRT 覆盖尚未确认，历史缺失样本不计入快速请求；拒绝来源：已核验';
 view.render(data);const html=view.element('msModels').innerHTML;
 assert.match(html,/data-ms-channel-key="unavailable:-1"/);assert.match(html,/data-ms-channel-key="routed_unknown:0"/);assert.match(html,/data-ms-channel-key="routed:42"/);
 assert.match(html,/<td>31<\/td><td>fixture-customer<\/td><td>250<\/td><td>83\.3%/);
 assert.match(html,/<td>无法识别<\/td><td>unknown<\/td><td>50<\/td><td>16\.7%/);
 assert.ok(html.indexOf('unavailable:-1')<html.indexOf('routed_unknown:0'));
 assert.doesNotMatch(view.element('msCoverage').textContent,/FRT/);assert.match(view.element('msCoverage').textContent,/已路由请求：已覆盖；拒绝来源：已核验/);
 const heads=[...html.matchAll(/<thead><tr>(.*?)<\/tr><\/thead>/g)].map(x=>[...x[1].matchAll(/<th>/g)].length);assert.deepEqual(heads,[5,5,2,4]);
});
