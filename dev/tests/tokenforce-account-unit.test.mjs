import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8');
const page=readFileSync(new URL('../../monitor/page.html',import.meta.url),'utf8');
function extract(start,end){
  const from=source.indexOf(start),to=source.indexOf(end,from);
  assert.ok(from>=0&&to>from);
  return source.slice(from,to);
}
function fields(){
  const elements=new Map();
  return id=>{
    if(!elements.has(id))elements.set(id,{value:'',textContent:'',checked:false});
    return elements.get(id);
  };
}

test('TokenForce form fixes face-value units and removes editable FX configuration',()=>{
  const $=fields(),groups=new Map();
  $('cmUpstreamProvider').value='tokenforce';
  $('cmUpstreamUnitPerUSD').value='7.2';
  const document={
    querySelectorAll(selector){
      if(!groups.has(selector))groups.set(selector,[{hidden:false}]);
      return groups.get(selector);
    },
    querySelector:()=>({}),
  };
  const context=vm.createContext({$,document});
  vm.runInContext(extract('function syncUpstreamFields()','function upstreamState('),context);
  vm.runInContext('syncUpstreamFields()',context);
  assert.equal($('cmUpstreamUnitPerUSD').value,'1');
  assert.equal(groups.get('.cm-upstream-tokenforce')[0].hidden,false);
  assert.equal(groups.get('.cm-upstream-sub2api')[0].hidden,true);
  assert.match(page,/id="cmUpstreamUnitPerUSD"[^>]*value="1" readonly/);
  assert.match(page,/成本仅按倍率配置中的“该主域名的上游充值比例”换算/);
  assert.doesNotMatch(page,/每 1 USD 对应多少 CNY/);
});

test('TokenForce save ignores old FX field and clears credentials on failure',async()=>{
  for(const obsoleteUnit of ['', '7.2', '8']){
    const $=fields();
    $('cmUpstreamProvider').value='tokenforce';
    $('cmUpstreamBaseURL').value='https://maas.hainahn.com';
    $('cmUpstreamOrgID').value='123';
    $('cmUpstreamUnitPerUSD').value=obsoleteUnit;
    $('cmUpstreamTokenForceRefreshToken').value='fixture-refresh-token';
    $('cmUpstreamEnabled').checked=true;
    $('cmUpstreamUsageEnabled').checked=true;
    let sent;
    const context=vm.createContext({
      $,cm:{upstreamDomain:{domain:'hainahn.com'},report:{finance:{can_edit:true}}},
      resetUpstreamDiagnostic(){},showUpstreamMessage(){},
      fetch:async(url,options)=>{
        assert.equal(url,'/channels/upstream');
        sent=JSON.parse(options.body);
        throw Error('local simulated connection failure');
      },
    });
    vm.runInContext(extract('async function saveUpstream()','async function syncUpstreamNow()'),context);
    await vm.runInContext('saveUpstream()',context);
    assert.equal(sent.unit_per_usd,1);
    assert.equal(sent.user_id,123);
    assert.equal(sent.refresh_token,'fixture-refresh-token');
    assert.equal(sent.usage_sync_enabled,true);
    assert.equal($('cmUpstreamTokenForceRefreshToken').value,'');
    assert.equal($('cmUpstreamSave').disabled,false);
    assert.equal($('cmUpstreamUsageSync').disabled,false);
  }
});
