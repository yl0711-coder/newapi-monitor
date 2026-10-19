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
  vm.runInContext(code.slice(0,end)+'\nglobalThis.api={cm,render,filteredDomains,setDomainRetiring};\n'+code.slice(end),context);
  return {api:context.api,element,context};
}
function domain(name,status=1,extra={}){
  const usage={requests:2,tokens:30,cost_usd:4};
  return {key:name,domain:name,configured:true,usage,manually_disabled:status===2,
    upstream:{configured:true,balance_usd:7},vendors:[{name:'test',channels:[{id:name,name,current:true,status,usage,groups:[],configured_groups:['test']}]}],...extra};
}
function setup(domains){
  const f=fixture();f.api.cm.report={domains,meta:{data_coverage:{complete:true}},finance:{can_edit:true},summary:{}};return f;
}
test('only explicit retirement places accounts below, regardless of routing status',()=>{
  const {api,element}=setup([domain('a-disabled',2),domain('z-active'),domain('b-auto',3),domain('c-retiring',1,{retiring:true}),domain('d-retiring-disabled',2,{retiring:true}),domain('e-retiring-auto',3,{retiring:true})]);
  const snapshot=JSON.stringify(api.cm.report);api.render();const html=element('cmBody').innerHTML;
  const divider=html.indexOf('aria-label="已停用渠道"');assert(divider>0);
  for(const key of ['z-active','b-auto','a-disabled'])assert(html.indexOf(`data-cm-domain-toggle="${key}"`)<divider);
  for(const key of ['c-retiring','d-retiring-disabled','e-retiring-auto'])assert(html.indexOf(`data-cm-domain-toggle="${key}"`)>divider);
  assert.equal((html.match(/data-cm-domain-toggle=/g)||[]).length,6);
  assert.match(html,/不再充值续费/);
  assert.doesNotMatch(html,/已禁用渠道|未禁用渠道/);
  assert.equal(JSON.stringify(api.cm.report),snapshot);
  assert.match(html,/data-cm-upstream="a-disabled"/);assert.match(html,/data-cm-finance="a-disabled"/);
});
test('partition does not change summary amounts or expanded account state',()=>{
  const d=domain('account',2,{retiring:true}),{api,element}=setup([d,domain('active')]);
  api.cm.expandedDomains.add(d.key);api.render();const totals=element('cmSummary').innerHTML;
  assert.match(element('cmBody').innerHTML,/cm-domain-card open manually-disabled/);
  d.retiring=false;api.render();assert.equal(element('cmSummary').innerHTML,totals);
  assert(!element('cmBody').innerHTML.includes('aria-label="已停用渠道"'));
  assert.equal(d.manually_disabled,true);
  assert.match(element('cmBody').innerHTML,/cm-domain-card open/);
});
test('filtering a mixed account to a disabled child does not reclassify the account',()=>{
  const d=domain('mixed');d.vendors[0].channels.push({...d.vendors[0].channels[0],id:'off',name:'off',status:2});
  const {api,element}=setup([d]);api.cm.filters.search='off';api.render();
  assert.match(element('cmBody').innerHTML,/aria-label="未停用渠道"/);
  assert.doesNotMatch(element('cmBody').innerHTML,/aria-label="已停用渠道"/);
});
test('retired-only searches remain visible and empty searches retain empty state',()=>{
  const {api,element}=setup([domain('retired',1,{retiring:true}),domain('active')]);api.cm.filters.search='retired';api.render();
  assert.match(element('cmBody').innerHTML,/aria-label="已停用渠道"/);
  assert.doesNotMatch(element('cmBody').innerHTML,/aria-label="未停用渠道"|当前筛选没有匹配渠道/);
  api.cm.filters.search='no-such-account';api.render();assert.match(element('cmBody').innerHTML,/当前筛选没有匹配渠道/);
});
test('all-active and historical-only accounts are not described as disabled; names remain escaped',()=>{
  const d=domain('<history>',2,{manually_disabled:false});d.vendors[0].channels[0].current=false;
  const {api,element}=setup([d]);api.render();const html=element('cmBody').innerHTML;
  assert.doesNotMatch(html,/aria-label="已停用渠道"|<history>/);assert.match(html,/&lt;history&gt;/);
});
test('mark and cancel move the account without changing balances, totals or routing status',async()=>{
  const d=domain('account',2),{api,element,context}=setup([d,domain('other')]);
  api.render();const totals=element('cmSummary').innerHTML,requests=[];
  context.window.confirm=()=>true;
  context.fetch=async(url,options)=>{const body=JSON.parse(options.body);requests.push({url,body});return {ok:true,status:200,json:async()=>({retiring:body.retiring,updated_at:123})}};
  const button={disabled:false};
  await api.setDomainRetiring(d.key,button);
  assert.match(element('cmBody').innerHTML,/aria-label="已停用渠道"/);
  assert.equal(d.retiring,true);assert.equal(button.disabled,false);
  assert.equal(element('cmSummary').innerHTML,totals);
  await api.setDomainRetiring(d.key,button);
  assert.doesNotMatch(element('cmBody').innerHTML,/aria-label="已停用渠道"/);
  assert.equal(d.retiring,false);assert.equal(d.vendors[0].channels[0].status,2);
  assert.equal(element('cmSummary').innerHTML,totals);
  assert.deepEqual(requests.map(r=>r.body.expected_retiring),[false,true]);
  assert(requests.every(r=>r.url==='/channels/upstream/retirement'));
});
test('routing status filters and metric sorting never turn an unmarked account into retired',()=>{
  for(const status of ['','enabled','disabled'])for(const sort of ['name','cost','requests','tokens']){
    const {api,element}=setup([domain('a-retired',1,{retiring:true}),domain('z-disabled',2)]);
    api.cm.filters.status=status;api.cm.sort=sort;api.render();const html=element('cmBody').innerHTML;
    if(status!=='enabled')assert.match(html,/aria-label="未停用渠道"/);
    if(status==='disabled')assert.doesNotMatch(html,/aria-label="已停用渠道"/);
    if(!status)assert(html.indexOf('data-cm-domain-toggle="z-disabled"')<html.indexOf('data-cm-domain-toggle="a-retired"'));
  }
});
