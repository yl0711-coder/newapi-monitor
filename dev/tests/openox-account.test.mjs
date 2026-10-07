import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8');
const page=readFileSync(new URL('../../monitor/page.html',import.meta.url),'utf8');
function extract(start,end){const a=source.indexOf(start),b=source.indexOf(end,a);assert.ok(a>=0&&b>a);return source.slice(a,b);}

test('OpenOx form selects only its own credentials and explains the financial boundary',()=>{
  const elements=new Map();
  const $=id=>{if(!elements.has(id))elements.set(id,{value:'',textContent:''});return elements.get(id);};
  $('cmUpstreamProvider').value='openox';
  const groups=new Map();
  const document={querySelectorAll:selector=>{if(!groups.has(selector))groups.set(selector,[{hidden:false}]);return groups.get(selector);},querySelector:()=>({})};
  const context=vm.createContext({$,document});
  vm.runInContext(extract('function syncUpstreamFields()','function upstreamState('),context);
  vm.runInContext('syncUpstreamFields()',context);
  assert.equal(groups.get('.cm-upstream-openox')[0].hidden,false);
  for(const provider of ['newapi','sub2api','aicodewith','tokenforce'])assert.equal(groups.get('.cm-upstream-'+provider)[0].hidden,true);
  assert.match($('cmUpstreamUsageLabel').textContent,/订阅抵扣/);
  assert.match($('cmUpstreamUsageHelp').textContent,/不代表钱包实际扣款/);
  assert.match(page,/id="cmUpstreamOpenOxToken" type="password"/);
  $('cmUpstreamProvider').value='newapi';vm.runInContext('syncUpstreamFields()',context);
  assert.equal(groups.get('.cm-upstream-openox')[0].hidden,true);
  assert.equal(groups.get('.cm-upstream-newapi')[0].hidden,false);
});

test('OpenOx detection is labelled and guide text is escaped',()=>{
  const box={};const context=vm.createContext({$:()=>box,esc:value=>String(value).replaceAll('<','&lt;')});
  vm.runInContext(extract('function renderUpstreamDiagnostic(','async function diagnoseUpstream()'),context);
  context.data={provider:'openox',confidence:'configured',instructions:['<fixture>']};
  vm.runInContext('renderUpstreamDiagnostic(data)',context);
  assert.match(box.innerHTML,/检测结果：OpenOx/);assert.match(box.innerHTML,/&lt;fixture>/);
});

test('OpenOx save sends management token only and clears the secret on network failure',async()=>{
  const fields=new Map();const $=id=>{if(!fields.has(id))fields.set(id,{value:'',checked:false});return fields.get(id);};
  $('cmUpstreamProvider').value='openox';$('cmUpstreamBaseURL').value='https://api.openox.net';$('cmUpstreamOpenOxToken').value='fixture-secret';
  let sent;const context=vm.createContext({$,cm:{upstreamDomain:{domain:'openox.net'},report:{finance:{can_edit:true}}},resetUpstreamDiagnostic(){},showFinanceMessage(){},showUpstreamMessage(){},fetch:async(_url,opts)=>{sent=JSON.parse(opts.body);throw Error('fixture network failure');}});
  vm.runInContext(extract('async function saveUpstream()','async function syncUpstreamNow()'),context);
  await vm.runInContext('saveUpstream()',context);
  assert.equal(sent.access_token,'fixture-secret');assert.equal(sent.provider,'openox');
  for(const field of ['user_id','refresh_token','password','session_id'])assert.equal(sent[field],undefined);
  assert.equal($('cmUpstreamOpenOxToken').value,'');assert.equal($('cmUpstreamSave').disabled,false);
});
