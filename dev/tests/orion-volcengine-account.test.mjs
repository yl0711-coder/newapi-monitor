import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8');
const page=readFileSync(new URL('../../monitor/page.html',import.meta.url),'utf8');
function extract(start,end){const a=source.indexOf(start),b=source.indexOf(end,a);assert.ok(a>=0&&b>a);return source.slice(a,b);}

function formContext(provider){
  const elements=new Map();
  const $=id=>{if(!elements.has(id))elements.set(id,{value:'',textContent:''});return elements.get(id);};
  $('cmUpstreamProvider').value=provider;
  const groups=new Map();
  const document={querySelectorAll:selector=>{if(!groups.has(selector))groups.set(selector,[{hidden:false}]);return groups.get(selector);},querySelector:()=>({})};
  const context=vm.createContext({$,document});
  vm.runInContext(extract('function syncUpstreamFields()','function upstreamState('),context);
  vm.runInContext('syncUpstreamFields()',context);
  return {$,groups,context};
}

test('Orion and Volcengine forms show only their own credential fields',()=>{
  for(const provider of ['orion','volcengine']){
    const {$,groups}=formContext(provider);
    assert.equal(groups.get('.cm-upstream-'+provider)[0].hidden,false);
    for(const other of ['newapi','sub2api','aicodewith','tokenforce','openox','orion','volcengine'].filter(p=>p!==provider))
      assert.equal(groups.get('.cm-upstream-'+other)[0].hidden,true,provider+' hides '+other);
    assert.equal(groups.get('.cm-upstream-usage-option')?.[0]?.hidden??false,false);
    assert.match($('cmUpstreamUsageLabel').textContent,provider==='orion'?/Orion/:/火山/);
  }
  const {$}=formContext('volcengine');
  assert.match($('cmUpstreamUsageHelp').textContent,/不伪造小时明细/);
  assert.match(page,/<option value="orion">/);assert.match(page,/<option value="volcengine">/);
  for(const id of ['cmUpstreamOrionKey','cmUpstreamVolcAK','cmUpstreamVolcSK'])assert.match(page,new RegExp(`id="${id}" type="password"`));
});

async function save(provider,values){
  const fields=new Map();const $=id=>{if(!fields.has(id))fields.set(id,{value:'',checked:false});return fields.get(id);};
  $('cmUpstreamProvider').value=provider;$('cmUpstreamBaseURL').value='https://example.com';
  for(const [id,value] of Object.entries(values))$(id).value=value;
  let sent;const messages=[];
  const context=vm.createContext({$,cm:{upstreamDomain:{domain:'example.com'},report:{finance:{can_edit:true}}},resetUpstreamDiagnostic(){},showFinanceMessage(){},showUpstreamMessage(m){messages.push(m)},fetch:async(_url,opts)=>{sent=JSON.parse(opts.body);throw Error('fixture network failure');}});
  vm.runInContext(extract('async function saveUpstream()','async function syncUpstreamNow()'),context);
  await vm.runInContext('saveUpstream()',context);
  return {sent,$,messages};
}

test('Orion save sends the management key only and clears it on failure',async()=>{
  const {sent,$}=await save('orion',{cmUpstreamOrionKey:'smk_fixture'});
  assert.equal(sent.provider,'orion');assert.equal(sent.access_token,'smk_fixture');
  for(const field of ['user_id','refresh_token','password','session_id','access_key_id','secret_access_key'])assert.equal(sent[field],undefined);
  assert.equal($('cmUpstreamOrionKey').value,'');
});

test('Volcengine save sends AK/SK together and clears both on failure',async()=>{
  const {sent,$}=await save('volcengine',{cmUpstreamVolcAK:'AKfixture',cmUpstreamVolcSK:'SKfixture'});
  assert.equal(sent.provider,'volcengine');assert.equal(sent.access_key_id,'AKfixture');assert.equal(sent.secret_access_key,'SKfixture');
  for(const field of ['user_id','refresh_token','password','session_id','access_token'])assert.equal(sent[field],undefined);
  assert.equal($('cmUpstreamVolcAK').value,'');assert.equal($('cmUpstreamVolcSK').value,'');
  const half=await save('volcengine',{cmUpstreamVolcAK:'AKfixture'});
  assert.equal(half.sent,undefined);assert.match(half.messages.join(' '),/一起填写/);
});
