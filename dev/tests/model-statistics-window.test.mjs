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
    from_ts:windowTo-86400,to_ts:windowTo,target_ts:windowTo+lag,lag_seconds:lag,
    source:{facts_complete:true,requests_complete:true,frt_complete:true,coverage_status:'complete'},
    models:[{...metrics,model:'test-model',groups:[{...metrics,group:'test-group',
      channels:[{...metrics,channel_id:42,channel_name:'test-channel'}],customers:[]}]}]
  };
}

for(const minutes of [1,3]) {
  test(`a complete window ${minutes} minutes behind shows FRT at every level and its actual cutoff`,()=>{
    const view=modelView();
    view.render(report(minutes*60));
    const banner=view.element('msCoverage');
    assert.match(banner.textContent,new RegExp(`^数据更新到 .*15:20:00；采集进度比目标落后 ${minutes} 分钟。数据覆盖已确认`));
    assert.match(banner.textContent,/统计区间 .*2026\/9\/29 15:20:00 至 2026\/9\/30 15:20:00/);
    assert.doesNotMatch(banner.textContent,/未完整|尚未全部确认/);
    assert.equal(banner.classList.contains('incomplete'),false);
    const html=view.element('msModels').innerHTML;
    for(const metric of [/1\.3秒/g,/2\.7秒/g,/3\.1秒/g,/4\.2秒/g,/7次/g,/>211</g]) {
      assert.equal([...html.matchAll(metric)].length,3,'model, group, and channel retain complete-window metrics');
    }
  });
}

test('freshness never bypasses a real FRT coverage gap',()=>{
  const view=modelView(),data=report(60);
  data.source={facts_complete:false,requests_complete:true,frt_complete:false,ttft_complete:true,coverage_status:'incomplete'};
  view.render(data);
  const banner=view.element('msCoverage');
  assert.match(banner.textContent,/采集进度比目标落后 1 分钟/);
  assert.match(banner.textContent,/请求事实：完整；FRT（首个数据事件延迟）：未完整/);
  assert.equal(banner.classList.contains('incomplete'),true);
  const html=view.element('msModels').innerHTML;
  assert.doesNotMatch(html,/1\.3秒|2\.7秒|3\.1秒|4\.2秒|7次|>211</);
  assert.equal([...html.matchAll(/<td>—<\/td>/g)].length,18,'all six FRT cells at all three levels are unknown');
});

test('legacy responses without freshness fields retain coverage proof without inventing an update time',()=>{
  for(const missing of [['target_ts','lag_seconds'],['target_ts'],['lag_seconds']]) {
    const view=modelView(),data=report();
    for(const field of missing)delete data[field];
    view.render(data);
    assert.doesNotMatch(view.element('msCoverage').textContent,/数据更新到|采集进度比目标落后/);
    assert.match(view.element('msCoverage').textContent,/数据覆盖已确认/);
    assert.match(view.element('msModels').innerHTML,/2\.7秒/);
  }
});

test('freshness fields cannot substitute for missing independent FRT coverage proof',()=>{
  const view=modelView(),data=report();
  delete data.source.frt_complete;
  view.render(data);
  assert.match(view.element('msCoverage').textContent,/FRT（首个数据事件延迟）：未完整/);
  assert.doesNotMatch(view.element('msModels').innerHTML,/1\.3秒|2\.7秒|3\.1秒|4\.2秒|7次|>211</);
});

test('a caught-up closed window does not show a pending tail',()=>{
  const view=modelView();
  view.render(report(0));
  assert.doesNotMatch(view.element('msCoverage').textContent,/采集进度比目标落后/);
  assert.match(view.element('msModels').innerHTML,/2\.7秒/);
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

test('slow FRT remains visible without coloring model, group, channel or cells',()=>{
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
    for(const value of [/5\.0秒/g,/5\.5秒/g,/6\.0秒/g,/7\.0秒/g,/7次/g])assert.equal([...html.matchAll(value)].length,3);
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
