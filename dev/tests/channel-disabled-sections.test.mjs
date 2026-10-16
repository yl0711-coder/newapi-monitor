import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function fixture(){
  const elements=new Map();
  const element=id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',querySelector(){return null},removeAttribute(){}});return elements.get(id)};
  const context=vm.createContext({window:{},document:{addEventListener(){},getElementById:element}});
  vm.runInContext(readFileSync(new URL('../../monitor/channel_data_status.js',import.meta.url),'utf8'),context);
  const code=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8'),end=code.lastIndexOf('})();');
  vm.runInContext(code.slice(0,end)+'\nglobalThis.api={cm,render,filteredDomains};\n'+code.slice(end),context);
  return {api:context.api,element};
}
function domain(name,status=1,extra={}){
  const usage={requests:2,tokens:30,cost_usd:4};
  return {key:name,domain:name,configured:true,usage,manually_disabled:status===2,
    upstream:{configured:true,balance_usd:7},vendors:[{name:'test',channels:[{id:name,name,current:true,status,usage,groups:[],configured_groups:['test']}]}],...extra};
}
function setup(domains){
  const f=fixture();f.api.cm.report={domains,meta:{data_coverage:{complete:true}},finance:{can_edit:true},summary:{}};return f;
}
test('disabled accounts occupy their own lower section once; automatic and retiring accounts stay above',()=>{
  const {api,element}=setup([domain('a-disabled',2),domain('z-active'),domain('b-auto',3),domain('c-retiring',1,{retiring:true})]);
  const snapshot=JSON.stringify(api.cm.report);api.render();const html=element('cmBody').innerHTML;
  const divider=html.indexOf('aria-label="已禁用渠道"');assert(divider>0);
  for(const key of ['z-active','b-auto','c-retiring'])assert(html.indexOf(`data-cm-domain-toggle="${key}"`)<divider);
  assert(html.indexOf('data-cm-domain-toggle="a-disabled"')>divider);
  assert.equal((html.match(/data-cm-domain-toggle=/g)||[]).length,4);
  assert.equal(JSON.stringify(api.cm.report),snapshot);
  assert.match(html,/data-cm-upstream="a-disabled"/);assert.match(html,/data-cm-finance="a-disabled"/);
});
test('partition does not change summary amounts or expanded account state',()=>{
  const d=domain('account',2),{api,element}=setup([d,domain('active')]);
  api.cm.expandedDomains.add(d.key);api.render();const totals=element('cmSummary').innerHTML;
  assert.match(element('cmBody').innerHTML,/cm-domain-card open manually-disabled/);
  d.manually_disabled=false;api.render();assert.equal(element('cmSummary').innerHTML,totals);
  assert(!element('cmBody').innerHTML.includes('aria-label="已禁用渠道"'));
  assert.match(element('cmBody').innerHTML,/cm-domain-card open/);
});
test('filtering a mixed account to a disabled child does not reclassify the account',()=>{
  const d=domain('mixed');d.vendors[0].channels.push({...d.vendors[0].channels[0],id:'off',name:'off',status:2});
  const {api,element}=setup([d]);api.cm.filters.search='off';api.render();
  assert.match(element('cmBody').innerHTML,/aria-label="未禁用渠道"/);
  assert.doesNotMatch(element('cmBody').innerHTML,/aria-label="已禁用渠道"/);
});
test('disabled-only searches remain visible and empty searches retain empty state',()=>{
  const {api,element}=setup([domain('disabled',2),domain('active')]);api.cm.filters.search='disabled';api.render();
  assert.match(element('cmBody').innerHTML,/aria-label="已禁用渠道"/);
  assert.doesNotMatch(element('cmBody').innerHTML,/aria-label="未禁用渠道"|当前筛选没有匹配渠道/);
  api.cm.filters.search='no-such-account';api.render();assert.match(element('cmBody').innerHTML,/当前筛选没有匹配渠道/);
});
test('all-active and historical-only accounts are not described as disabled; names remain escaped',()=>{
  const d=domain('<history>',2,{manually_disabled:false});d.vendors[0].channels[0].current=false;
  const {api,element}=setup([d]);api.render();const html=element('cmBody').innerHTML;
  assert.doesNotMatch(html,/aria-label="已禁用渠道"|<history>/);assert.match(html,/&lt;history&gt;/);
});
