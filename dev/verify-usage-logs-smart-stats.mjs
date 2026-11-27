// Actual embedded JS in a lightweight DOM VM, backed by the explicit synthetic
// loopback preview. This is HTTP/interaction verification, not browser layout QA.
import vm from 'node:vm';
import assert from 'node:assert/strict';

const origin=process.argv[2];
assert.match(origin||'',/^http:\/\/127\.0\.0\.1:\d+$/,'requires explicit loopback synthetic preview origin');
const html=await fetch(origin+'/').then(r=>r.text());
assert.ok(html.includes('/usage-logs.js?v=9'),'current embedded asset version');
const source=await fetch(origin+'/usage-logs.js?v=9').then(r=>r.text());
const meta=await fetch(origin+'/usage-logs/meta').then(r=>r.json());
assert.equal(meta.success,true);

async function run(name,filters,{pageSize=100,wantStats,wantRows,wantCountButton=false}={}){
  const nodes=new Map(),calls=[];
  const el=id=>{
    if(!nodes.has(id))nodes.set(id,{value:'',hidden:false,disabled:false,innerHTML:'',textContent:'',style:{},dataset:{},
      classList:{add(){},remove(){},toggle(){}},setAttribute(){},addEventListener(){},querySelector(){return null},querySelectorAll(){return[]}});
    return nodes.get(id);
  };
  const context=vm.createContext({window:{matchMedia:()=>({matches:false})},document:{getElementById:el,createElement:()=>el('toast'),body:{appendChild(){}},addEventListener(){}},
    localStorage:{getItem:()=>null,setItem(){}},location:{hash:'',pathname:'/',search:''},history:{replaceState(){}},navigator:{},URLSearchParams,AbortController,setTimeout,clearTimeout,
    atob:s=>Buffer.from(s,'base64').toString('binary'),fetch:(url,options)=>{calls.push(String(url));return fetch(origin+url,options)}});
  vm.runInContext(source.replace('window.usageLogsActivate=activate;','globalThis.api={st,prepareSearch,loadList,loadCount};window.usageLogsActivate=activate;'),context);
  const v=context.api,now=Date.now();
  Object.assign(v.st,{meta:meta.data,pageSize,applied:{type:'0',startTime:now-3600000,endTime:now+60000,...filters}});
  v.prepareSearch();await v.loadList();
  for(let i=0;i<100&&(v.st.statLoading||v.st.countTask);i++)await new Promise(r=>setTimeout(r,10));
  assert.equal(v.st.loading,false,name);assert.equal(v.st.statLoading,false,name);
  assert.equal(v.st.items.length,wantRows,name);assert.equal(calls.filter(x=>x.includes('/stat?')).length,wantStats,name);
  assert.equal(calls.filter(x=>x.includes('/count?')).length,0,name);
  if(wantStats){
    assert.ok(/用量 <b>¥/.test(el('ulStats').innerHTML),name+' amount');
    assert.ok(/RPM <b>/.test(el('ulStats').innerHTML),name+' rates');
    assert.ok(/TPM <b>/.test(el('ulStats').innerHTML),name+' tokens');
  }else{
    assert.ok(/不包含消费/.test(el('ulStats').innerHTML),name+' skip reason');
    assert.ok(!/skeleton|用量 <b>¥0/.test(el('ulStats').innerHTML),name+' no invented zero');
  }
  if(v.st.items.some(x=>x.type===2)){
    assert.ok(/base · ¥4 \/ ¥20\/M/.test(el('ulTableWrap').innerHTML),name+' original billing summary');
    assert.ok(/\+1/.test(el('ulTableWrap').innerHTML),name+' extra billing segment');
  }
  if(wantCountButton){
    assert.equal(v.st.totalKnown,false);assert.ok(/计算总数/.test(el('ulPager').innerHTML));
    await v.loadCount(v.st.seq);assert.equal(v.st.total,6);assert.equal(calls.filter(x=>x.includes('/count?')).length,1);
  }
  console.log(JSON.stringify({name,rows:v.st.items.length,statRequests:wantStats,totalRequests:calls.length,explicitCount:wantCountButton,pass:true}));
}

await run('normal-all',{}, {wantStats:2,wantRows:36});
await run('error-type',{type:'5'},{wantStats:0,wantRows:2});
await run('all-error-request-id',{requestId:'demo-error-ultra'},{wantStats:0,wantRows:1});
await run('normal-customer-model-group',{type:'2',username:'demo-user',model:'claude-demo',group:'演示分组'},{wantStats:2,wantRows:32});
await run('issues-manual-count',{type:'issues'},{pageSize:2,wantStats:2,wantRows:2,wantCountButton:true});
await run('error-keyword',{message:'Invalid value'},{wantStats:0,wantRows:1});
await run('empty-customer',{username:'absent-demo-user'},{wantStats:0,wantRows:0});
console.log('PASS: 7 synthetic scenarios; current embedded JS + live Go HTTP handlers. Not browser layout acceptance.');
