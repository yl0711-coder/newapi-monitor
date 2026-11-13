import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const read=file=>readFileSync(new URL('../../monitor/'+file,import.meta.url),'utf8');
function harness(file='model_frt.js'){
 const elements=new Map(),requests=[],location={href:''};
 const element=id=>{
  if(!elements.has(id)){const classes=new Set();elements.set(id,{id,hidden:false,innerHTML:'',textContent:'',dataset:{},attrs:{},classList:{toggle(c,b){if(b)classes.add(c);else classes.delete(c)},remove(c){classes.delete(c)},contains(c){return classes.has(c)}},setAttribute(k,v){this.attrs[k]=v},addEventListener(){}})}
  return elements.get(id);
 };
 const context=vm.createContext({window:{},document:{getElementById:element,querySelectorAll:()=>[]},location,URLSearchParams,AbortController,fetch:(url,options)=>new Promise((resolve,reject)=>requests.push({url,options,resolve,reject}))});
 const source=read(file),at=source.lastIndexOf('})();');
 vm.runInContext(read('view_refresh.js'),context);context.window.MonitorView.preserve=(_root,update)=>update();
 const exports=file==='model_frt.js'?'state,load,render,metrics,deactivate':'state,load';
 vm.runInContext(source.slice(0,at)+'\nglobalThis.testing={'+exports+'};\n'+source.slice(at),context);
 context.testing.state.active=true;
 return {...context.testing,window:context.window,element,requests,location};
}
const metrics={mean_ms:760,mean_coverage_complete:true,mean_unavailable_reason:'',observed:100,p50_ms:400,p95_ms:4000,p99_ms:4100,max_ms:4200,over_3s:10,over_3s_pct:10,coverage_complete:true,unavailable_reason:''};
function report(name='model') {return {window:'24h',from:'2026-10-13T08:20:00Z',to:'2026-10-14T08:20:00Z',target:'2026-10-14T08:23:00Z',lag_seconds:180,generated_at:'2026-10-14T08:24:00Z',coverage:{complete:true,status:'complete',note:'覆盖已核验'},channels:[{channel_id:7,name:'channel',frt:metrics,groups:[{group:'actual-group',frt:metrics,models:[{model:name,frt:metrics}]}]}]}}
const response=(data=report(),status=200)=>({ok:status===200,status,json:async()=>data,text:async()=>JSON.stringify(data)});

test('FRT hierarchy has only channel/group expanders and model leaves, escaped labels',()=>{
 const h=harness();h.render({...report('<img src=x onerror=alert(1)>'),actual_through_ts:Date.parse('2026-10-14T08:20:00Z')/1000,timing:{finalize_delay_seconds:120,alignment_seconds:60,collection_poll_seconds:60}});
 const html=h.element('frtTree').innerHTML;
 assert.equal((html.match(/<details /g)||[]).length,2);assert.equal((html.match(/class="frt-model"/g)||[]).length,1);
 assert.match(html,/&lt;img/);assert.match(html,/平均 FRT/);assert.match(html,/0.76 秒/);assert.match(html,/FRT 样本/);assert.match(html,/最大 FRT/);assert.doesNotMatch(html,/10%|&gt; 3 秒|> 3 秒/);assert.doesNotMatch(html,/<img|on(click|mouseover)=|渠道命中|已进入|无可用渠道|排队|并发|high-unavailable/);
 assert.ok(html.indexOf('channel')<html.indexOf('actual-group'));
 assert.match(h.element('frtCoverage').textContent,/16:20:00.*北京时间，左闭右开/);
 assert.match(h.element('frtCoverage').textContent,/采集积压 180 秒/);
});
for(const reason of ['no_valid_samples','historical_unsupported','invalid_histogram'])test(reason+' never formats missing latency as zero',()=>{
 const h=harness(),html=h.metrics({observed:reason==='no_valid_samples'?0:null,p50_ms:null,p95_ms:null,p99_ms:null,max_ms:null,coverage_complete:false,unavailable_reason:reason});
 assert.doesNotMatch(html,/0 秒/);assert.match(html,/—/);assert.match(html,/未完整/);
 assert.match(html,reason==='no_valid_samples'?/没有有效 FRT 样本/:reason==='historical_unsupported'?/历史口径不支持/:/直方图不一致/);
});
test('empty incomplete facts do not claim no traffic or healthy latency',()=>{
 const h=harness(),r=report();r.channels=[];r.coverage.complete=false;r.coverage.note='本地来源未启用';h.render(r);
 assert.match(h.element('frtTree').innerHTML,/不能据此判断/);assert.match(h.element('frtCoverage').textContent,/来源未启用/);assert.ok(h.element('frtCoverage').classList.contains('gap'));
});
test('explicit unknown FRT watermark cannot fall back to the requested end',()=>{
 const h=harness(),r=report();r.actual_through_ts=0;h.render(r);
 assert.match(h.element('frtCoverage').textContent,/数据截至 未知/);
});
test('refresh cancels old load and stale success/finally cannot overwrite latest result',async()=>{
 const h=harness();const first=h.load();const second=h.load();assert.ok(h.requests[0].options.signal.aborted);
 h.requests[1].resolve(response(report('new')));await second;h.requests[0].resolve(response(report('old')));await first;
 assert.match(h.element('frtTree').innerHTML,/new/);assert.doesNotMatch(h.element('frtTree').innerHTML,/old/);assert.equal(h.element('frtTree').attrs['aria-busy'],'false');
});
test('stale response body, stale errors and stale 401 cannot escape generation guard',async()=>{
 const h=harness();let bodyResolve;const first=h.load();h.requests[0].resolve({ok:true,status:200,json:()=>new Promise(r=>bodyResolve=r)});await new Promise(setImmediate);assert.equal(typeof bodyResolve,'function');
 const second=h.load();bodyResolve(report('stale-body'));await first;assert.equal(h.element('frtTree').attrs['aria-busy'],'true');
 h.requests[1].resolve(response(report('new')));await second;
 const third=h.load();h.deactivate();h.requests[2].resolve(response({},401));await third;assert.equal(h.location.href,'');
 h.state.active=true;const fourth=h.load();h.deactivate();h.requests[3].reject(Error('stale-error'));await fourth;assert.equal(h.element('frtError').hidden,true);
});
test('switch away aborts request and returning can load again',async()=>{
 const h=harness();const first=h.load();h.deactivate();assert.ok(h.requests[0].options.signal.aborted);h.requests[0].resolve(response());await first;assert.equal(h.element('frtTree').innerHTML,'');
 h.state.active=true;const second=h.load();h.requests[1].resolve(response());await second;assert.match(h.element('frtTree').innerHTML,/model/);
});
test('errors, malformed responses and current unauthorized state are explicit',async()=>{
 const h=harness();
 for(const r of [response({error:'local unavailable'},503),response({}),{ok:true,status:200,json:async()=>{throw Error('invalid JSON')}}]){
  const p=h.load();h.requests.at(-1).resolve(r);await p;assert.equal(h.element('frtError').hidden,false);assert.equal(h.element('frtTree').attrs['aria-busy'],'false');assert.equal(h.state.report,null);
 }
 const p=h.load();h.requests.at(-1).resolve(response({},401));await p;assert.equal(h.location.href,'/login');
});
test('pinned comparison sends original UTC range and stays read-only',async()=>{
 const h=harness();h.state.to='2026-10-14T08:20:00.000Z';h.state.window='3d';const p=h.load();const r=h.requests[0];assert.match(r.url,/window=3d&to=2026/);assert.equal(new URL(r.url,'http://local').searchParams.get('to'),h.state.to);assert.equal(r.options.cache,'no-store');assert.equal(r.options.method,undefined);r.resolve(response());await p;
 assert.match(h.element('frtCoverage').textContent,/已固定为原统计窗口/);
});
test('old model page also ignores late unauthorized responses after leaving its subpage',async()=>{
 const h=harness('model_statistics.js');h.state.inited=true;const p=h.load('24h');h.window.modelStatisticsDeactivate();h.requests[0].resolve(response({},401));await p;assert.equal(h.location.href,'');
});
test('new navigation exists in both menus with shell title and independent Token local report',()=>{
 const html=read('page.html');
 for(const nav of html.matchAll(/<nav class="tabs[^]*?<\/nav>/g))assert.match(nav[0],/data-tab="anomalies"/);
 assert.match(read('stability.js'),/anomalies:\{title:'异常监控'/);
 const token=html.split('id="tab-anomalies"')[1].split('<!-- /tab-anomalies -->')[0];
 assert.match(token,/Token 异常/);assert.match(token,/id="taList"/);assert.match(token,/id="taFactsHealth"/);assert.match(token,/id="taCreateExport"/);assert.match(token,/tiered_expr/);assert.doesNotMatch(token,/data-tab="alerts"|已检测 \d|0 个异常/);
 assert.match(html,/aria-controls="msRequestsView"/);assert.match(html,/aria-controls="msFRTView"/);
 assert.match(read('model_frt.css'),/max-width:360px/);assert.match(read('model_frt.css'),/focus-visible/);
});
test('exact filters are URL encoded and included in local GET scope',async()=>{
 const h=harness();h.state.filters={channel_id:'7',group:'g & /',model:'m/a+b'};const p=h.load();const req=h.requests[0],q=new URL(req.url,'http://local').searchParams;assert.equal(q.get('channel_id'),'7');assert.equal(q.get('group'),'g & /');assert.equal(q.get('model'),'m/a+b');req.resolve(response());await p;assert.match(h.element('frtCoverage').textContent,/各层统计均为此筛选范围/);
});

test('historical missing sum keeps histogram and explains unavailable mean',()=>{
 const h=harness(),html=h.metrics({...metrics,mean_ms:null,mean_coverage_complete:false,mean_unavailable_reason:'historical_sum_unavailable'});
 assert.match(html,/不可计算/);assert.match(html,/历史样本缺少真实耗时总和/);assert.match(html,/4 秒/);assert.match(html,/已核验/);assert.doesNotMatch(html,/0 秒|> 3 秒|比例/);
});
test('all levels preserve server mean ordering including missing and ties',()=>{
 const h=harness(),r=report();const make=(name,mean)=>({model:name,frt:{...metrics,mean_ms:mean}});
 r.channels[0].groups[0].models=[make('slow',4000),make('tie-a',500),make('tie-b',500),make('unknown',null)];
 h.render(r);const html=h.element('frtTree').innerHTML;const indices=['slow','tie-a','tie-b','unknown'].map(n=>html.indexOf(n));
 assert.ok(indices.every((n,i)=>i===0||n>indices[i-1]));
 const section=read('page.html').split('id="msFRTView"')[1].split('</section>')[0];assert.match(section,/真实耗时总和 ÷ 有效样本数/);assert.doesNotMatch(section,/3 秒|排队/);
});
