import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
const js=readFileSync(new URL('../../monitor/logchain.js',import.meta.url),'utf8');
const html=readFileSync(new URL('../../monitor/page.html',import.meta.url),'utf8');
const flush=()=>new Promise(setImmediate);
test('governance evidence prefill does not query source on entry or focus',async()=>{
 const h=fixture({preset:'group_governance',request_id:'gg-request',channel_id:'3',model:'m',group:'g',date:'2026-11-10',from_time:'12:00',to_time:'12:00'});
 await h.activate();assert.equal(h.logs().length,0);assert.equal(h.lc.requiresManualQuery,true);assert.equal(h.lc.filters.request_id,'gg-request');
 await h.activate();assert.equal(h.logs().length,0);
});
class Element{
  value='';hidden=false;disabled=false;checked=false;innerHTML='';textContent='';handlers=new Map();dataset={};
  classList={toggle(){},add(){},remove(){}};
  addEventListener(name,fn){this.handlers.set(name,fn)}
  setAttribute(){} querySelector(){return null} querySelectorAll(){return []} focus(){} close(){} showModal(){}
  async emit(name,event={}){this.handlers.get(name)?.({preventDefault(){},...event});await flush()}
}
function fixture(navigation={}){
  const elements=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element()]));
  const events=new Map(),timers=new Map(),requests=[];let timer=0,status='pending_delivery';
  const on=(name,fn)=>events.set(name,[...(events.get(name)||[]),fn]);
  const row={id:10,created_at:1792721400,type:5,request_id:'fixture-request',user_id:7,username:'fixture',channel_id:1,model_name:'fixture-model',content:'fixture'};
  const document={hidden:false,getElementById:id=>elements.get(id)||null,querySelector:()=>null,querySelectorAll:()=>[],addEventListener:on};
  const context=vm.createContext({window:{monitorNavigationContext:()=>navigation,addEventListener:on},document,URLSearchParams,AbortController,
    setTimeout(fn,delay){const id=++timer;timers.set(id,{fn,delay});return id},clearTimeout:id=>timers.delete(id),
    fetch:async(url,options={})=>{
      requests.push({url,options});let data={};
      if(url.startsWith('/logchain/requests')){
        const more=url.includes('before_id=');data={rows:[{...row,...(more?{id:9,created_at:1792721340}: {})}],has_more:!more,next_before_ts:row.created_at,next_before_id:10,
          generated_at:1792721600+requests.length,query_through_ts:1792721400,cloudwatch_enabled:true,coverage_complete:true};
      }else if(url==='/logchain/filters')data={groups:[],channels:[],domains:[]};
      else if(url==='/logchain/investigations')data={investigation_id:'fixture-task',poll_after_ms:800};
      else if(url.endsWith('/cancel'))data={status:'cancelled',source_status:[]};
      else data={status,source_status:[],summary:{conclusion:'保留的证据'}};
      return {ok:true,status:200,text:async()=>JSON.stringify(data),json:async()=>data};
    }});
  const end=js.lastIndexOf('})();');
  vm.runInContext(js.slice(0,end)+'\nglobalThis.testing={lc,load,loadCloudWatchEvidence,cancelCloudWatchInvestigation,STANDALONE_INVESTIGATION};\n'+js.slice(end),context);
  return {context,document,requests,...context.testing,el:id=>elements.get(id),
    logs:()=>requests.filter(r=>r.url.startsWith('/logchain/requests')),
    async activate(){context.window.logChainActivate();await flush()},
    deactivate(){context.window.logChainDeactivate()},
    async emit(name){for(const fn of events.get(name)||[])fn();await flush()},
    async tick(delay){for(const [id,t] of [...timers])if(t.delay===delay){timers.delete(id);t.fn()}await flush()},
    setTaskStatus:s=>{status=s}
  };
}

test('Token prefill blocks focus, visibility, timers, reactivation and filter changes until explicit query',async()=>{
  const v=fixture({preset:'token_local_review',user_id:'7',date:'2026-10-22'});
  await v.activate();await v.emit('focus');v.document.hidden=true;await v.emit('visibilitychange');v.document.hidden=false;await v.emit('visibilitychange');
  await v.tick(30000);v.deactivate();await v.activate();
  v.el('lcStream').value='true';await v.el('lcStream').emit('change');
  assert.equal(v.logs().length,0);
  await v.el('lcApply').emit('click');assert.equal(v.logs().length,1);
  assert.match(v.logs()[0].url,/from=2026-10-22&to=2026-10-22/);
  v.deactivate();await v.activate();assert.equal(v.logs().length,2,'submitted prefill can refresh on return');
  assert.match(v.logs()[1].url,/from=2026-10-22&to=2026-10-22/);
});

test('sidebar return with an empty hash context cannot release Token manual-query boundary',async()=>{
 const navigation={preset:'token_local_review',user_id:'7',date:'2026-10-22'};
 const v=fixture(navigation);await v.activate();v.deactivate();
 for(const key of Object.keys(navigation))delete navigation[key];
 await v.activate();await v.emit('focus');await v.emit('visibilitychange');await v.tick(30000);
 assert.equal(v.logs().length,0);assert.equal(v.lc.date,'2026-10-22');
 await v.el('lcApply').emit('click');assert.equal(v.logs().length,1);
});

test('pending_delivery survives multiple refresh periods, completion and subsequent same-query refresh',async()=>{
  const v=fixture();await v.activate();v.lc.expanded.add('10');await v.loadCloudWatchEvidence('10');await v.tick(800);
  assert.equal(v.lc.cwState.get('10').result.status,'pending_delivery');
  for(let i=0;i<4;i++){await v.tick(30000);await v.emit('focus')}
  assert.equal(v.logs().length,1);assert.equal(v.lc.cwState.get('10').investigationId,'fixture-task');
  await v.tick(60000);assert.equal(v.lc.cwState.get('10').loading,true);
  v.setTaskStatus('complete');await v.tick(60000);
  assert.equal(v.lc.cwState.get('10').loading,false);assert.equal(v.lc.cwState.get('10').result.status,'complete');
  v.lc.expanded.clear();await v.tick(30000);
  assert.equal(v.logs().length,2);assert.equal(v.lc.cwState.get('10').result.status,'complete');
});

test('active row task stays cancellable when a filter change is attempted, then accepts the filter',async()=>{
  const v=fixture();await v.activate();await v.loadCloudWatchEvidence('10');await v.tick(800);
  v.el('lcStream').value='true';await v.el('lcStream').emit('change');
  assert.equal(v.logs().length,1);assert.equal(v.lc.filters.stream,'');
  assert.match(v.el('lcError').innerHTML||v.el('lcError').textContent,/完成或取消/);
  await v.cancelCloudWatchInvestigation('10');assert.equal(v.lc.cwState.get('10').result.status,'cancelled');
  v.el('lcStream').value='true';await v.el('lcStream').emit('change');assert.equal(v.logs().length,2);
  assert.match(v.logs().at(-1).url,/stream=true/);assert.equal(v.lc.cwState.has('10'),false);
});

test('paging and expanded details suspend automatic replacement without changing date or cursors',async()=>{
  const v=fixture();await v.activate();await v.load(true);
  assert.deepEqual(Array.from(v.lc.rows,r=>r.id),[10,9]);
  const before=v.lc.date;await v.tick(30000);await v.emit('focus');assert.equal(v.logs().length,2);
  v.deactivate();await v.activate();assert.equal(v.logs().length,2);assert.equal(v.lc.date,before);
  await v.el('lcApply').emit('click');assert.equal(v.logs().length,3);assert.equal(v.lc.rows.length,1);
  v.lc.expanded.add('10');await v.tick(30000);assert.equal(v.logs().length,3);
  v.lc.expanded.clear();await v.tick(30000);assert.equal(v.logs().length,4);
});

test('leaving a page preserves independent task identity and evidence while clearing sensitive inputs',async()=>{
  const v=fixture();await v.activate();
  const saved={seq:3,loading:true,investigationId:'standalone',result:{status:'pending_delivery'}};
  v.lc.cwState.set(v.STANDALONE_INVESTIGATION,saved);v.el('lcInvestigationRequestID').value='sensitive';
  v.deactivate();assert.equal(v.el('lcInvestigationRequestID').value,'');assert.equal(v.lc.cwState.get(v.STANDALONE_INVESTIGATION),saved);
  await v.activate();assert.equal(v.lc.cwState.get(v.STANDALONE_INVESTIGATION),saved);assert.equal(v.logs().length,1);
});
