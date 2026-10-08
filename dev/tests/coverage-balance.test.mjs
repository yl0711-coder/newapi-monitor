import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function fixture(file, exports) {
  const elements=new Map();
  const element=id=>{
    if(!elements.has(id)){
      const classes=new Set();
      elements.set(id,{textContent:'',innerHTML:'',hidden:false,
        classList:{toggle(name,on){if(on)classes.add(name);else classes.delete(name)},contains(name){return classes.has(name)}}});
    }
    return elements.get(id);
  };
  const context=vm.createContext({document:{getElementById:element,addEventListener(){}},window:{}});
  const source=readFileSync(new URL(`../../monitor/${file}`,import.meta.url),'utf8');
  const end=source.lastIndexOf('})();');
  assert.ok(end>0);
  vm.runInContext(source.slice(0,end)+`\nglobalThis.fixture={${exports}};\n`+source.slice(end),context);
  return {api:context.fixture,element};
}

test('balance excludes owned credit only, preserving stopped suppliers, zeros and detail objects',()=>{
  const {api}=fixture('channel_management.js','upstreamBalanceSummary');
  const account=(balance,excluded=false)=>({upstream:{balance_usd:balance,exclude_from_balance_summary:excluded}});
  const own=account(1000000,true),external=account(123),stopped={...account(7),retired:true};
  const input=[own,external,stopped,account(0),account(null),account(NaN)];
  const result=api.upstreamBalanceSummary(input);
  assert.equal(result.total,130);
  assert.equal(result.known.length,3);
  assert.equal(result.excluded.length,1);
  assert.equal(own.upstream.balance_usd,1000000);
  assert.equal(api.upstreamBalanceSummary([own]).known.length,0);
  assert.equal(api.upstreamBalanceSummary([account(0)]).known.length,1);
});

test('model coverage UI distinguishes real-time tail, unknown history and complete proof',()=>{
  const {api,element}=fixture('model_statistics.js','render');
  for(const [status,complete,expected,warn] of [
    ['pending',false,/实时尾段待定稿/,false],
    ['incomplete',false,/历史覆盖尚未全部确认/,true],
    ['complete',true,/数据覆盖已确认/,false],
    [undefined,false,/历史覆盖尚未全部确认/,true]
  ]) {
    api.render({models:[],source:{coverage_status:status,facts_complete:complete,note:'测试',routed_coverage:{through_ts:1789980000}}});
    assert.match(element('msCoverage').textContent,expected);
    assert.match(element('msCoverage').textContent,/已路由水位/);
    assert.equal(element('msCoverage').classList.contains('incomplete'),warn);
  }
  api.render({models:null,source:null});
  assert.match(element('msCoverage').textContent,/历史覆盖尚未全部确认/);
});

test('model statistics renders rows without an undefined item and keeps missing over-3s unknown',()=>{
  const {api,element}=fixture('model_statistics.js','render,state,ttftStats,ttftCell');
  api.state.report={source:{ttft_complete:true}};
  assert.doesNotThrow(()=>api.render({
    source:{facts_complete:true,requests_complete:true,ttft_complete:true},
    models:[{model:'gpt-test',requests:2,routed_requests:2,unavailable_channel_requests:0,
      ttft_observed:2,ttft_p50_ms:1200,ttft_p95_ms:1800,ttft_p99_ms:2100,ttft_max_ms:2200,groups:[]}]
  }));
  const stats=api.ttftStats({ttft_observed:2,ttft_p50_ms:1200,ttft_p95_ms:1800,ttft_p99_ms:2100,ttft_max_ms:2200},true);
  assert.match(api.ttftCell(stats,'p99'),/2\.1秒/);
  assert.equal(api.ttftCell(stats,'over'),'—');
  const explicitZero=api.ttftStats({ttft_observed:2,ttft_over_3s:0},true);
  assert.match(api.ttftCell(explicitZero,'over'),/0次/);
  assert.match(element('msModels').innerHTML,/gpt-test/);
});
