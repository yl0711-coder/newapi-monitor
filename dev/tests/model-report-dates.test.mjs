import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const read=p=>readFileSync(new URL('../../monitor/'+p,import.meta.url),'utf8');
const now=Date.parse('2026-10-16T08:30:00Z');
function harness(initial={}){
 const elements=new Map(),requests=[],navigation=[],location={href:''},timers=[],windowEvents={},documentEvents={};
 let context={...initial};
 const el=id=>{
  if(!elements.has(id)){
   const classes=new Set(),events={};
   elements.set(id,{id,value:'',textContent:'',innerHTML:'',hidden:false,disabled:false,dataset:{},attrs:{},events,children:[],
    classList:{toggle(k,v){v?classes.add(k):classes.delete(k)},add(k){classes.add(k)},remove(k){classes.delete(k)},contains(k){return classes.has(k)}},
    setAttribute(k,v){this.attrs[k]=v},addEventListener(k,f){(events[k]??=[]).push(f)},focus(){},
    querySelectorAll(s){return this.children.filter(b=>[...s.matchAll(/data-md-action(?:="([^"]*)")?/g)].some(m=>!m[1]||b.dataset.mdAction===m[1]))},
    querySelector(s){return this.querySelectorAll(s)[0]},
    emit(k,extra={}){for(const f of events[k]||[])f({preventDefault(){},target:this,...extra})}
   });
  }return elements.get(id);
 };
 const buttons=(prefix,attribute,values)=>values.map(value=>{const b=el(prefix+value);b.dataset[attribute]=value;b.getAttribute=k=>k==='data-ms-window'?value:b.attrs[k];return b});
 const tabs=buttons('tab-','msView',['requests','frt']);tabs[0].id='msRequestsTab';tabs[1].id='msFRTTab';elements.set(tabs[0].id,tabs[0]);elements.set(tabs[1].id,tabs[1]);
 const windows=buttons('ms-window-','msWindow',['24h','3d','7d']),frtWindows=buttons('frt-window-','frtWindow',['24h','3d','7d']);
 for(const p of ['ms','frt'])el(p+'Dates').children=buttons(p+'-action-','mdAction',['today','yesterday','previous','next','single','reset','start','end']);
 class Clock extends Date{constructor(...a){super(...(a.length?a:[now]))}static now(){return now}}
 const windowObject={addEventListener(k,f){(windowEvents[k]??=[]).push(f)}};
 const documentObject={hidden:false,addEventListener(k,f){(documentEvents[k]??=[]).push(f)},getElementById:el,querySelectorAll:s=>s==='[data-ms-view]'?tabs:s==='[data-ms-window]'?windows:s==='[data-frt-window]'?frtWindows:[]};
 const sandbox={window:windowObject,document:documentObject,Date:Clock,URLSearchParams,AbortController,location,
  setTimeout(fn){timers.push(fn);return timers.length},clearTimeout(){},
  fetch:(url,options)=>new Promise((resolve,reject)=>requests.push({url,options,resolve,reject}))};
 const c=vm.createContext(sandbox);
 vm.runInContext(read('model_report_dates.js'),c);
 for(const [file,key,exports]of [['model_statistics.js','old','state,load,render'],['model_frt.js','frt','state,load,render']]){
  const src=read(file),at=src.lastIndexOf('})();');vm.runInContext(src.slice(0,at)+'\nglobalThis.'+key+'={'+exports+'};\n'+src.slice(at),c);
 }
 c.window.monitorNavigationContext=()=>context;
 c.window.monitorNavigate=(tab,params)=>{navigation.push({tab,params});context={...params};c.window.modelStatisticsPageActivate()};
 const activate=()=>c.window.modelStatisticsPageActivate();
 const click=id=>el(id).emit('click');
 const emitWindow=event=>{for(const fn of windowEvents[event]||[])fn()};
 const emitDocument=event=>{for(const fn of documentEvents[event]||[])fn()};
 const runTimer=()=>{const fn=timers.shift();if(fn)fn()};
 return {...c,el,requests,navigation,activate,click,context:()=>context,emitWindow,emitDocument,runTimer,timers};
}
const sample={mean_ms:500,observed:2,p50_ms:500,p95_ms:500,p99_ms:500,max_ms:500,coverage_complete:true};
function report(req,{empty=false,complete=true,noWindow=false}={}){
 const q=new URL(req.url,'http://local').searchParams,from=q.get('from')||'2026-10-15T08:20:00.000Z',to=q.get('to')||'2026-10-16T08:20:00.000Z';
 const range={mode:q.get('range')||'rolling',requested_from:from,requested_to:to,available_through:to,retained_from:'2026-10-06T16:00:00Z',max_days:7,has_window:!noWindow,note:noWindow?'所选日期尚无可用区间；不能确认零请求。':'今日尚未结束，仅展示实际可用截止前的数据。'};
 if(req.url.startsWith('/model-statistics/report'))return {window:q.get('window')||range.mode,range,from_ts:Date.parse(from)/1000,to_ts:noWindow?Date.parse(from)/1000:Date.parse(to)/1000,generated_at:now/1000,source:{facts_complete:complete,requests_complete:complete},models:empty?[]:[{model:'model',requests:100,routed_requests:64,unavailable_channel_requests:36,groups:[]}]};
 return {window:q.get('window')||range.mode,range,from,to,generated_at:new Date(now).toISOString(),coverage:{complete,note:complete?'覆盖已核验':'覆盖不完整'},channels:empty?[]:[{channel_id:7,name:'fixture',frt:sample,groups:[{group:'g',frt:sample,models:[{model:'model',frt:sample}]}]}]};
}
const flush=()=>new Promise(setImmediate);
async function finish(h,options={},status=200){const r=h.requests.at(-1),data=status===200?report(r,options):{error:'本地读取失败'};r.resolve({ok:status===200,status,json:async()=>data,text:async()=>JSON.stringify(data)});await flush();return r}
function query(h){return new URL(h.requests.at(-1).url,'http://local').searchParams}

test('Beijing day arithmetic, inclusive end, leap year and strict invalid dates',()=>{
 const h=harness(),d=h.window.ModelReportDates;
 assert.equal(d.today(),'2026-10-16');
 assert.equal(d.calendar('2026-10-15','2026-10-15').from,'2026-10-14T16:00:00.000Z');
 assert.equal(d.calendar('2026-10-15','2026-10-15').to,'2026-10-15T16:00:00.000Z');
 assert.equal(d.shift('2026-01-01',-1),'2025-12-31');assert.equal(d.shift('2024-02-28',1),'2024-02-29');
 assert.equal(Date.parse(d.calendar('2025-12-31','2026-01-01').to)-Date.parse(d.calendar('2025-12-31','2026-01-01').from),172800000);
 for(const day of ['2026-02-29','2026-13-01','2026-10-1','invalid'])assert.throws(()=>d.parseDay(day));
 for(const [a,b]of [['2026-10-16','2026-10-15'],['2026-10-01','2026-10-16'],['2026-10-17','2026-10-17']])assert.throws(()=>d.calendar(a,b));
});

test('both pages query today, yesterday, named day and previous/next without losing the date',async()=>{
 for(const view of ['requests','frt']){
  const h=harness(view==='frt'?{view}:{}),p=view==='frt'?'frt':'ms';h.activate();await finish(h);
  h.click(p+'-action-today');assert.equal(query(h).get('from'),'2026-10-15T16:00:00.000Z');assert.equal(query(h).get('window'),null);await finish(h);
  h.click(p+'-action-yesterday');assert.equal(query(h).get('from'),'2026-10-14T16:00:00.000Z');await finish(h);
  h.el(p+'DateStart').value='2026-10-12';h.click(p+'-action-single');assert.equal(query(h).get('to'),'2026-10-12T16:00:00.000Z');await finish(h);
  h.click(p+'-action-next');assert.equal(h.el(p+'DateStart').value,'2026-10-13');await finish(h);
  h.click(p+'-action-previous');assert.equal(h.el(p+'DateStart').value,'2026-10-12');await finish(h);
  h.el(p+'DateEnd').value='2026-10-14';h.el(p+'Dates').emit('submit');assert.equal(query(h).get('to'),'2026-10-14T16:00:00.000Z');await finish(h);
  const saved=h.requests.at(-1).url;h.click(p==='ms'?'msRefresh':'frtRefresh');assert.equal(h.requests.at(-1).url,saved);await finish(h);
 }
});

test('dates and exact filters survive subpage switch, URL reload and independent resets',async()=>{
 const h=harness();h.activate();await finish(h);h.click('ms-action-yesterday');await finish(h);
 h.click('msFRTTab');await finish(h);h.el('frtChannel').value='7';h.el('frtGroup').value='g & x';h.el('frtModel').value='m/a';h.el('frtFilters').emit('submit');await finish(h);
 h.click('frt-action-yesterday');assert.equal(query(h).get('group'),'g & x');await finish(h);
 const saved=h.requests.at(-1).url;
 h.click('msRequestsTab');assert.equal(query(h).get('from'),'2026-10-14T16:00:00.000Z');await finish(h);
 h.click('msFRTTab');assert.equal(h.requests.at(-1).url,saved);await finish(h);
 const restored=harness(h.context());restored.activate();assert.equal(restored.requests[0].url,saved);await finish(restored);
 h.click('frt-action-reset');assert.equal(query(h).get('window'),'24h');assert.equal(query(h).get('group'),'g & x');await finish(h);
 h.click('frt-action-yesterday');await finish(h);h.click('frtReset');assert.equal(h.el('frtGroup').value,'');assert.equal(h.el('frtChannel').value,'');assert.equal(h.el('frtModel').value,'');assert.equal(query(h).get('group'),null);assert.equal(query(h).get('from'),'2026-10-14T16:00:00.000Z');await finish(h);
});

test('comparison carries both exact UTC endpoints instead of reconstructing a rolling window',async()=>{
 const h=harness();h.activate();await finish(h);h.click('ms-action-yesterday');await finish(h);
 const original=h.old.state.report;h.click('msFRTTab');await finish(h);h.click('frtCompare');
 const q=query(h);assert.equal(q.get('from'),new Date(original.from_ts*1000).toISOString());assert.equal(q.get('to'),new Date(original.to_ts*1000).toISOString());assert.equal(q.get('range'),'exact');assert.equal(q.get('window'),null);
 await finish(h,{complete:false});assert.match(h.el('frtCoverage').textContent,/固定/);
 h.click('frtRefresh');assert.equal(query(h).get('from'),q.get('from'));await finish(h);
});

test('FRT reuses the model page timer and focus refresh without loading the hidden request view',async()=>{
 const h=harness({view:'frt'});h.activate();await finish(h);
 const first=h.requests.length;
 h.runTimer();
 assert.equal(h.requests.length,first+1);
 assert.match(h.requests.at(-1).url,/\/model-statistics\/frt\?/);
 await finish(h);
 const second=h.requests.length;
 h.emitWindow('focus');
 assert.equal(h.requests.length,second+1);
 assert.match(h.requests.at(-1).url,/\/model-statistics\/frt\?/);
 await finish(h);
});

test('rapid date changes isolate late success, error and unauthorized responses on both pages',async()=>{
 for(const view of ['requests','frt']){
  const h=harness(view==='frt'?{view}:{}),p=view==='frt'?'frt':'ms';h.activate();await finish(h);
  h.click(p+'-action-yesterday');const old=h.requests.at(-1);h.click(p+'-action-today');assert.ok(old.options.signal.aborted);
  await finish(h);const result=h[p==='ms'?'old':'frt'].state.report;
  old.resolve({ok:false,status:401,json:async()=>({}),text:async()=>''});await flush();assert.equal(h.location.href,'');assert.equal(h[p==='ms'?'old':'frt'].state.report,result);
  h.click(p+'-action-yesterday');const stale=h.requests.at(-1);h.click(p+'-action-today');await finish(h);stale.reject(Error('late error'));await flush();
  assert.equal(h.el(p==='ms'?'msError':'frtError').hidden,true);assert.equal(h.el(p+'DateStart').value,'2026-10-16');
  h.click(p==='ms'?'msRefresh':'frtRefresh');await finish(h,{},503);assert.equal(h.el(p==='ms'?'msError':'frtError').hidden,false);
  assert.equal(h.el(p+'DateStart').disabled,false);h.click(p==='ms'?'msRefresh':'frtRefresh');await finish(h);
  h.click(p==='ms'?'msRefresh':'frtRefresh');const leave=h.requests.at(-1);h.click(p==='ms'?'msFRTTab':'msRequestsTab');assert.ok(leave.options.signal.aborted);leave.reject(Error('cancelled late'));await flush();await finish(h);
 }
});

test('unavailable selected day does not render zero as proven traffic; invalid input issues no query',async()=>{
 const h=harness();h.activate();await finish(h);h.click('ms-action-today');await finish(h,{empty:true,complete:false,noWindow:true});
 assert.match(h.el('msSummary').innerHTML,/不能据此确认零请求/);assert.equal(h.window.modelStatisticsComparison(),null);
 const n=h.requests.length;h.el('msDateStart').value='2026-02-30';h.click('ms-action-single');assert.equal(h.requests.length,n);assert.match(h.el('msDateNote').textContent,/不存在/);
 h.el('msDateStart').value='2020-01-01';h.el('msDateEnd').value='2020-01-01';h.el('msDates').emit('submit');assert.equal(h.requests.length,n);assert.match(h.el('msDateNote').textContent,/留存/);
 h.click('msFRTTab');await finish(h,{empty:true,complete:false,noWindow:true});assert.match(h.el('frtTree').innerHTML,/不能据此判断/);
});
