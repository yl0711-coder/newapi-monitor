import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

const script=readFileSync(new URL('../../monitor/ecs_log_sources.js',import.meta.url),'utf8');
function ui(fetch, timers={setTimeout,clearTimeout}) {
  let html='', buttons=[], selects=[];
  const root={textContent:'',get innerHTML(){return html;},set innerHTML(value){
    html=value;
    buttons=[...value.matchAll(/<button[^>]*data-page="([^"]+)"([^>]*)>/g)].map(m=>({dataset:{page:m[1]},disabled:m[2].includes('disabled'),addEventListener(_event,fn){this.click=fn;}}));
    selects=[...value.matchAll(/<select data-select="([^"]+)"/g)].map(m=>({dataset:{select:m[1]},disabled:false,value:'',addEventListener(_event,fn){this.change=fn;}}));
  },querySelectorAll(selector){
    if(selector.includes(','))return [...buttons,...selects];
    return selector.startsWith('button')?buttons:selector.startsWith('select')?selects:[];
  },insertAdjacentHTML(_position,value){html=value+html;}};
  const context=vm.createContext({window:{},document:{getElementById:()=>root},fetch,location:{},Date,AbortController,URLSearchParams,...timers});
  vm.runInContext(script,context);
  return {api:context.window.MonitorECSLogSources,root,context,button:kind=>buttons.find(b=>b.dataset.page===kind),select:kind=>selects.find(s=>s.dataset.select===kind)};
}
const service='arn:service/cluster/worker';
const source=(overrides={})=>({service_arn:service,task_arn:'arn:task/cluster/a',node:'ecs-source',container:'nginx',lane:'access',status:'reporting',runtime_id:'runtime',last_heartbeat:123,last_report:123,lease_until:200,...overrides});
const payload=(overrides={})=>({enabled:true,sources:[source()],total:1,page:1,has_more:false,health:{available:true,services:[{service_arn:service,status:'reporting',discovery_status:'discovered',active_tasks:1,active_sources:1,task_count:1,starting_sources:0,unhealthy_sources:0,stopped_pending:0,delivery_gaps:0}]},...overrides});
const ok=data=>({ok:true,json:async()=>data});

test('collector closure is distinct from business coverage and abnormal exits remain visible',async()=>{
  const data=payload({sources:[source({status:'delivery_gap_unresolved',archive_status:'collector_chain_closed',archive_closed_at:123})]});
  data.health.archive={status:'scanned',accepted_batches:5,collector_closures:1};
  const {api,root}=ui(async()=>ok(data));await api.load();
  assert.match(root.innerHTML,/退出交付缺口待核实/);
  assert.match(root.innerHTML,/非业务完整性证明/);
  assert.match(root.innerHTML,/采集链结束 1 条/);
  assert.match(root.innerHTML,/不计作完整覆盖/);
});

test('archive failures remain visible with backlog and partial scans are not complete',async()=>{
  for(const [status,label] of [['scan_failed','归档读取失败'],['scan_progressing','归档分段检查中']]) {
    const data=payload();
    data.health.archive={status,accepted_batches:10,pending_batches:20,conflict_batches:0,rejected_batches:0,last_scan:100,last_progress:110,last_failure:105};
    const {api,root}=ui(async()=>ok(data));
    await api.load();
    assert.ok(root.innerHTML.includes(label));
    assert.match(root.innerHTML,/最近推进/);assert.match(root.innerHTML,/最近失败/);
    assert.match(root.innerHTML,/不计作完整覆盖/);
  }
});

test('ECS log UI groups task lanes without confusing liveness with coverage',async()=>{
  const {api,root}=ui(async()=>ok(payload({sources:[source(),source({lane:'evidence',status:'heartbeating_no_log_batches'})],total:2})));
  await api.load();
  assert.equal((root.innerHTML.match(/<details /g)||[]).length,1);
  assert.match(root.innerHTML,/代理在线，尚无日志批次/);
  assert.match(root.innerHTML,/心跳在线不代表区间数据完整/);
  assert.match(root.innerHTML,/任务阶段/);
  assert.doesNotMatch(root.innerHTML,/data-action=|<details[^>]* open/);
});

test('malformed or unavailable registry never becomes an empty healthy table',async()=>{
  for(const data of [null,payload({sources:[null]}),payload({health:{available:true,services:[null]}}),payload({health:{available:false,services:[]}})]) {
    const {api,root}=ui(async()=>ok(data));await api.load();
    assert.match(root.textContent,/不能据此判断采集正常或任务已退出/);
  }
});

test('source strings and server errors cannot inject HTML',async()=>{
  let count=0;
  const {api,root}=ui(async()=>++count===1?ok(payload({sources:[source({container:'<img src=x>',runtime_id:'"><script>x</script>'})]})):{ok:false,json:async()=>({error:'<bad gateway>'})});
  await api.load();assert.doesNotMatch(root.innerHTML,/<img|<script/);
  await api.load();assert.match(root.innerHTML,/上次读取结果/);assert.doesNotMatch(root.innerHTML,/<bad gateway>/);
});

test('failed pagination retains previous page; timer cannot overtake a pending navigation',async()=>{
  let calls=0, finish;
  const {api,root,button}=ui(async url=>{
    if(++calls===1)return ok(payload({has_more:true,total:102}));
    assert.match(url,/page=2/);
    return new Promise(resolve=>{finish=resolve;});
  });
  await api.load();
  const navigation=button('next').click();
  await api.load();assert.equal(calls,2);
  finish({ok:false,json:async()=>({error:'retry later'})});await navigation;
  assert.match(root.innerHTML,/第 1 页/);assert.equal(button('next').disabled,false);
});

test('filter switches to stopped records and resets page',async()=>{
  const urls=[];
  const {api,select,root}=ui(async url=>{urls.push(url);return ok(payload());});
  await api.load();const filter=select('phase');filter.value='stopped';await filter.change();
  assert.match(urls[1],/phase=stopped&page=1/);assert.match(root.innerHTML,/value="stopped" selected/);
});

test('stale responses cannot overwrite newer status',async()=>{
  let first, count=0;
  const {api,root}=ui(async()=>++count===1?new Promise(resolve=>{first=resolve;}):ok(payload({sources:[source({container:'new'})]})));
  const pending=api.load();await api.load();first(ok(payload({sources:[source({container:'old'})]})));await pending;
  assert.match(root.innerHTML,/new \/ access/);assert.doesNotMatch(root.innerHTML,/old \/ access/);
});

test('deadline failure is visible and permits subsequent refresh',async()=>{
  let expire, calls=0;
  const {api,root}=ui((_url,{signal})=>{
    if(++calls>1)return ok(payload());
    return new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>reject(Error('timeout'))));
  },{setTimeout(fn,ms){assert.equal(ms,10000);expire=fn;return 1;},clearTimeout(){}});
  const pending=api.load();expire();await pending;assert.match(root.textContent,/timeout/);
  await api.load();assert.match(root.innerHTML,/AWS 发现正常/);
});

test('disabled runtime and login expiration are explicit',async()=>{
  const disabled=ui(async()=>ok(payload({enabled:false})));await disabled.api.load();
  assert.match(disabled.root.textContent,/动态日志接入未启用/);
  const expired=ui(async()=>({status:401}));await expired.api.load();assert.equal(expired.context.location.href,'/login');
});

test('verified retained files do not claim complete business coverage',async()=>{
  const {api,root}=ui(async()=>ok(payload({sources:[source({archive_status:'collector_chain_closed', final_boundary_status:'retained_files_verified'})]})));
  await api.load();
  assert.match(root.innerHTML,/保留日志文件与最终采集偏移已核对/);
  assert.match(root.innerHTML,/非业务完整性证明/);
  assert.doesNotMatch(root.innerHTML,/业务数据完整|全站数据完整/);
});
