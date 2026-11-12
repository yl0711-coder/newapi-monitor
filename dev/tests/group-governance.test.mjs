import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const read=file=>readFileSync(new URL('../../monitor/'+file,import.meta.url),'utf8');
function harness(){
 const elements=new Map(),requests=[],timers=new Map(),events=new Map(),navigations=[];let next=0;
 const element=id=>{if(!elements.has(id))elements.set(id,{id,value:'',checked:false,hidden:false,innerHTML:'',textContent:'',attrs:{},style:{},events:new Map(),setAttribute(k,v){this.attrs[k]=v},addEventListener(k,f){this.events.set(k,f)}});return elements.get(id)};
 const document={hidden:false,getElementById:element,addEventListener:(k,f)=>events.set(k,f)};
 const window={addEventListener:(k,f)=>events.set(k,f),MonitorView:{preserve:(el,fn)=>fn()},monitorNavigate:(...args)=>navigations.push(args)};
 const context=vm.createContext({window,document,TextEncoder,URLSearchParams,AbortController,Intl,Date,location:{href:''},setTimeout(f,ms){const id=++next;timers.set(id,{f,ms});return id},clearTimeout:id=>timers.delete(id),fetch:(url,options)=>new Promise((resolve,reject)=>requests.push({url,options,resolve,reject}))});
 const source=read('group_governance.js'),end=source.lastIndexOf('})();');vm.runInContext(source.slice(0,end)+'\nglobalThis.testing={state,load,render,renderHeader,filtered,groups,channelHTML,actionHTML,groupHTML,channelDetails,channelEvidence,priority,openTroubleshooting,key};\n'+source.slice(end),context);
 return {...context.testing,window,document,element,requests,timers,events,navigations};
}
const model={key:'model-key',group:'g',model:'m',channel_id:1,channel_name:'c1',channel_status:1,status:'unchanged',from_ts:1700000000,to_ts:1700000300,window_minutes:5,requests:50,rpm:10,peak_rpm:12,frt:{mean_ms:1000,observed:50,p50_ms:900,p95_ms:1600,over_3s:0,over_3s_pct:0,coverage_complete:true},over_5s:0,over_10s:0,over_5s_pct:0,over_10s_pct:0,comparison:{count:5,p50_ms:1000,p80_ms:1100,p90_ms:1500,position:2,above_median_pct:0},baseline:{mean_ms:900,rpm:8,minutes:30},rules:[{id:'frt_attention',eligible:true,hit:false,hits:0,recoveries:0,actual:1000,threshold:1300,reason:'未达到触发线'}],requests_complete:true,errors_complete:true,attributed_errors:0,pending_errors:0,other_errors:0,fault_rate_pct:0,error_reasons:[],actions:[],alternatives:[],checks:[],missing:[],trend:[]};
const channel={...model,model:'',key:'channel-key',models:['m','m2'],frt_source_complete:true,evidence:[{...model,mean_ms:1000,observed:50}],decision:{label:'不用调整',reason:'速度和错误情况正常，维持现状',frt:'按当前模型构成对齐参照 1.00 秒，相比慢 0.0%；未达到持续判慢条件',errors:'没有已确认渠道错误；待判 0 次',traffic:'比此前正常水平增加 25.0%（每分钟 8.00 次）',gaps:[]}};
function report(m={}){return {enabled:true,state:{last_success_at:1700000300,current_group_count:2},groups:[{group:'g',current:true,channels:[{id:1,name:'c1',status:1}],enabled_channels:1},{group:'idle',current:true,channels:[],enabled_channels:0}],realtime:{rule_version:'fixture',status:'ready',generated_at:1700000340,metrics_through_ts:1700000300,config_snapshot_at:1700000300,finalize_delay_seconds:120,adjustment_count:0,pause_count:0,increase_count:0,requests_complete:true,channels:[{...channel,...m}],actions:[]}}}
const response=data=>({ok:true,status:200,json:async()=>data});

test('group and channel disclosures retain complete evidence and escape source values',()=>{
 const h=harness();h.state.report=report({models:['<bad>'],channel_name:'<unsafe>'});h.state.open.add(h.key('group','g'));h.state.open.add(h.key('channel','g',1));h.state.open.add(h.key('basis',channel.key));h.renderHeader();h.render();
 const html=h.element('ggRows').innerHTML;
 for(const section of ['调整事项与影响范围','首字耗时','同模型比较与连续评估','渠道汇总与正常基准','请求量','具体错误与排障样本','分钟趋势','数据状态与来源'])assert.ok(html.includes(section),section);
 assert.ok(html.includes('&lt;bad&gt;'));assert.ok(!html.includes('<unsafe>'));assert.equal(h.element('ggTotal').textContent,'2');assert.ok(html.includes('idle'));
});
test('unknown metrics and coverage cannot be formatted as zero or normal',()=>{
 const h=harness();h.state.report=report();const html=h.channelDetails({...channel,status:'pending',frt:{},fault_rate_pct:null,errors_complete:false,decision:{label:'暂无法判断',reason:'错误归责尚未完成',frt:'无有效首字耗时样本，暂不能比较速度',gaps:['日志原文尚未补取']}});
 assert.ok(html.includes('0 次 · 不可计算'));assert.ok(html.includes('日志原文尚未补取'));assert.ok(html.includes('错误归责尚未完成'));assert.ok(html.includes('首字耗时</small><b>不可计算'));assert.ok(!html.includes('首字耗时</small><b>0'));
});
test('status and search filter use channel/model evidence while top counts stay deduplicated',()=>{
 const h=harness();h.state.report=report({status:'pause',actions:['pause:1']});h.state.report.realtime.actions=[{id:'pause:1',kind:'pause',channel_id:1,scope:['g / m']}];h.state.report.realtime.adjustment_count=1;h.state.report.realtime.pause_count=1;
 h.element('ggSearch').value='c1';h.element('ggStatus').value='pause';assert.equal(h.filtered().length,1);h.renderHeader();assert.equal(h.element('ggAdjust').textContent,'1');assert.equal(h.element('ggTotal').textContent,'2');
 h.element('ggSearch').value='missing';assert.equal(h.filtered().length,0);
});
test('refresh keeps prior result and open levels, filter and user data while awaiting a response',async()=>{
 const h=harness();h.state.active=true;h.state.report=report();h.state.open.add(h.key('group','g'));h.element('ggSearch').value='g';h.render();const before=h.element('ggRows').innerHTML;
 const pending=h.load();assert.equal(h.element('ggRows').innerHTML,before);h.requests[0].resolve(response(report({requests:60})));await pending;
 assert.ok(h.state.open.has(h.key('group','g')));assert.equal(h.element('ggSearch').value,'g');assert.equal(h.state.report.realtime.channels[0].requests,60);
});
test('late responses, errors and deactivation cannot replace newer results',async()=>{
 const h=harness();h.state.active=true;const a=h.load(),b=h.load();assert.ok(h.requests[0].options.signal.aborted);
 h.requests[1].resolve(response(report({requests:70})));await b;h.requests[0].resolve(response(report({requests:1})));await a;assert.equal(h.state.report.realtime.channels[0].requests,70);
 const c=h.load();h.window.groupGovernanceDeactivate();h.requests[2].resolve(response(report({requests:2})));await c;assert.equal(h.state.report.realtime.channels[0].requests,70);
 h.state.active=true;const d=h.load();h.requests[3].reject(Error('local unavailable'));await d;assert.equal(h.state.report.realtime.channels[0].requests,70);assert.match(h.element('ggAlert').textContent,/保留上次结果/);
});
test('visible refresh is 30 seconds and focus/resume refreshes local results only',async()=>{
 const h=harness();h.window.groupGovernanceActivate();assert.equal(h.requests[0].url,'/group-governance/report');assert.equal([...h.timers.values()][0].ms,30000);
 h.requests[0].resolve(response(report()));await Promise.resolve();await Promise.resolve();
 h.document.hidden=true;h.events.get('visibilitychange')();assert.equal(h.timers.size,0);
 h.document.hidden=false;h.events.get('visibilitychange')();assert.equal(h.requests.length,2);h.events.get('focus')();assert.equal(h.requests.length,3);
 h.window.groupGovernanceDeactivate();h.events.get('focus')();assert.equal(h.requests.length,3);assert.ok(h.requests.every(r=>r.url.startsWith('/group-governance/')));
});
test('error sample links only prefill the existing troubleshooting manual-query preset',()=>{
 const h=harness();h.openTroubleshooting({request_id:'req',created_at:1700000000,channel_id:3,group:'g',model:'m'});
 assert.equal(h.requests.length,0);assert.equal(h.navigations[0][0],'logchain');const c=h.navigations[0][1];assert.equal(c.preset,'group_governance');assert.equal(c.request_id,'req');assert.equal(c.from_time,c.to_time);assert.equal(c.date,'2023-11-15');
});
test('page and styles keep governance selectors inside this module',()=>{
 const html=read('page.html'),css=read('group_governance.css');for(const id of ['ggTotal','ggAdjust','ggPause','ggIncrease','ggRows'])assert.ok(html.includes('id="'+id+'"'));
 for(const selector of css.split('{').slice(0,-1).map(x=>x.slice(x.lastIndexOf('}')+1)).filter(x=>x.includes('.gg-')))for(const s of selector.split(','))assert.ok(s.trim().startsWith('.group-governance-page'),s);
});

test('configuration-only mode cannot present uncollected user data as an empty census',()=>{
 const h=harness();h.state.report=report();h.state.report.config_only=true;h.state.open.add(h.key('group','g'));h.state.open.add(h.key('audit','g'));h.render();const html=h.element('ggRows').innerHTML;assert.ok(html.includes('用户 未采集'));assert.ok(html.includes('disabled title="当前来源模式未采集关联用户"'));
});

test('configuration pending, known zero and failed refresh are three distinct summaries',async()=>{
 const h=harness();h.state.report=report();Object.assign(h.state.report.realtime,{advice_ready:false,config_ready:false});h.renderHeader();assert.equal(h.element('ggAdjust').textContent,'暂不可判断');assert.match(h.element('ggAlert').textContent,/渠道配置尚未更新/);
 Object.assign(h.state.report.realtime,{advice_ready:true,config_ready:true});h.renderHeader();assert.equal(h.element('ggAdjust').textContent,'0');assert.match(h.element('ggAlert').textContent,/暂无需要执行的调整事项/);
 h.state.active=true;const pending=h.load();h.requests[0].reject(Error('temporarily unavailable'));await pending;assert.equal(h.element('ggAdjust').textContent,'暂不可判断');assert.match(h.element('ggAlert').textContent,/刷新失败，保留上次结果/);h.render();assert.match(h.element('ggAlert').textContent,/刷新失败/);
 const recovery=h.load();h.requests[1].resolve(response(report()));await recovery;assert.equal(h.element('ggAdjust').textContent,'0');assert.doesNotMatch(h.element('ggAlert').textContent,/刷新失败/);
});

test('opening a channel presents one set of totals and a single backend recommendation',()=>{
 const h=harness();h.state.report=report({decision:{...channel.decision,label:'不用调整',reason:'请求量增加70%，首字耗时和错误情况均未恶化。'}});h.state.open.add(h.key('group','g'));h.state.open.add(h.key('channel','g',1));h.render();const html=h.element('ggRows').innerHTML;
 assert.equal((html.match(/class="gg-core"/g)||[]).length,3);assert.equal((html.match(/请求量增加70%/g)||[]).length,1);assert.ok(!html.includes('gg-model-row'));assert.ok(!html.includes('gg-detail-box'));assert.equal(h.requests.length,0);
});

test('merged action shows exact triggering channels and backend numbers without busiest-channel inference',()=>{
 const h=harness();h.state.report=report();const action={id:'increase:g:m',kind:'increase',subtypes:['capacity','backup'],group:'g',model:'m',scope:['g / m'],reason:'补容量并增加独立备用',triggers:[{channel_id:9,channel_name:'真实触发',model:'m',from_ts:model.from_ts,to_ts:model.to_ts,rule:{id:'frt_attention',unit:'ms',actual:3440,comparison:2000,threshold:2600,hits:2,active:true,description:'当前 3.44 秒，参照为 2.00 秒，触发线为 2.60 秒。最近 3 个新分钟中已评估 3 个，2 次满足条件，持续条件已成立。'}}]};
 const html=h.actionHTML(action);for(const s of ['#9','真实触发','3.44 秒','2.60 秒','最近 3 个新分钟中已评估 3 个，2 次满足条件','补充容量','增加独立备用'])assert.ok(html.includes(s),s);assert.ok(!html.includes('3440'));
 h.state.open.add(h.key('channel','g',1));const htmlChannel=h.channelHTML({group:'g',actions:[action]},{id:1,name:'最忙但正常',status:1,metric:channel});assert.ok(!htmlChannel.includes('真实触发'));
});

test('configuration-triggered backup renders its backend explanation without inventing minute hits',()=>{
 const h=harness();const text='该渠道已停用，近期可靠独立备用不足。';const html=h.actionHTML({kind:'increase',triggers:[{channel_id:3,model:'m',rule:{id:'primary_exited',active:true,hits:0,description:text}}]});
 assert.ok(html.includes(text));assert.ok(!html.includes('0次满足'));assert.ok(!html.includes('最近3个新分钟'));
});

test('three indicators and one advice keep thresholds and model comparisons collapsed',()=>{
 const h=harness();h.state.report=report();const html=h.channelDetails({...channel,decision:{label:'不用调整',reason:'正常增长无需调整',frt:'当前1秒，比较线1.3秒。',errors:'未发现渠道错误。',traffic:'流量增加但性能正常。',gaps:['正常基准不足']}});
 assert.ok(html.indexOf('首字耗时</small>')<html.indexOf('已确认渠道／上游错误</small>'));assert.ok(html.indexOf('已确认渠道／上游错误</small>')<html.indexOf('请求量</small>'));
 assert.match(html,/查看详细依据/);assert.ok(!html.includes('分钟趋势'));assert.ok(!html.includes('同模型比较与连续评估'));assert.match(html,/正常基准不足/);assert.match(html,/包含失败重试/);
 h.state.open.add(h.key('basis',channel.key));const expanded=h.channelDetails(channel);assert.match(expanded,/单次请求耗时/);assert.match(expanded,/不是渠道平均耗时的分位线/);assert.match(expanded,/分钟趋势/);
});

test('priority puts actionable and active groups first with stable ties and retains idle groups',()=>{
 const h=harness();h.state.report=report();h.state.report.groups=[{group:'idle',channels:[]},{group:'normal-a',channels:[]},{group:'normal-b',channels:[]},{group:'issue',channels:[]}];h.state.report.realtime.channels=[{...channel,group:'normal-a'},{...channel,group:'normal-b'},{...channel,group:'issue',status:'attention'}];
 assert.deepEqual(Array.from(h.groups(),g=>g.group),['issue','normal-a','normal-b','idle']);h.state.report.groups.reverse();assert.deepEqual(Array.from(h.groups(),g=>g.group),['issue','normal-a','normal-b','idle']);
});

test('no requests names its window and incomplete collection is not an idle result',()=>{
 const h=harness();h.state.report=report({status:'no_requests',requests:0,window_minutes:15});const html=h.groupHTML(h.groups().find(g=>g.group==='g'));assert.match(html,/最近15分钟无请求/);
 h.state.report=report({status:'pending',requests:0,requests_complete:false});const pending=h.groupHTML(h.groups().find(g=>g.group==='g'));assert.ok(!pending.includes('分钟无请求'));assert.match(pending,/暂无法判断/);
});

test('latest pause, increase, uncertain, unchanged and idle order is applied at both levels',()=>{
 const h=harness();h.state.report=report();h.state.report.groups=['idle','normal','observe','increase','pause'].map(group=>({group,channels:[]}));
 h.state.report.realtime.channels=[{...channel,group:'idle',requests:0,status:'no_requests'},{...channel,group:'normal'},{...channel,group:'observe',status:'pending'},{...channel,group:'increase',status:'increase'},{...channel,group:'pause',status:'pause'}];
 assert.deepEqual(Array.from(h.groups(),g=>g.group),['pause','increase','observe','normal','idle']);
 const items=h.state.report.realtime.channels.map((m,i)=>({...m,group:'g',channel_id:i+1,channel_name:'ordered-'+(i+1),key:'c'+i}));h.state.report.realtime.channels=items;h.state.report.groups=[{group:'g',channels:[]}];h.state.open.add(h.key('group','g'));
 let html=h.groupHTML(h.groups()[0]);assert.deepEqual([...html.matchAll(/ordered-(\d)/g)].map(x=>x[1]),['5','4','3','2','1']);
 items[4].status='unchanged';items[3].status='unchanged';html=h.groupHTML(h.groups()[0]);assert.deepEqual([...html.matchAll(/ordered-(\d)/g)].map(x=>x[1]),['3','2','4','5','1']);
 h.state.report.realtime.channels.reverse();assert.equal(h.groupHTML(h.groups()[0]),html,'same-priority channel order changed');
});

test('fresh response moves recovered groups down while retaining filters and open channel identity',async()=>{
 const h=harness();h.state.active=true;h.state.report=report();h.state.report.groups=[{group:'a',channels:[]},{group:'b',channels:[]}];h.state.report.realtime.channels=[{...channel,group:'a',status:'pause'},{...channel,group:'b',status:'increase'}];
 h.state.open.add(h.key('group','a'));h.state.open.add(h.key('channel','a',1));h.state.open.add(h.key('basis',channel.key));h.element('ggSearch').value='c1';
 assert.deepEqual(Array.from(h.groups(),g=>g.group),['a','b']);const fresh=structuredClone(h.state.report);fresh.realtime.channels[0].status='unchanged';
 const pending=h.load();h.requests[0].resolve(response(fresh));await pending;assert.deepEqual(Array.from(h.groups(),g=>g.group),['b','a']);assert.equal(h.element('ggSearch').value,'c1');assert.ok(h.state.open.has(h.key('channel','a',1)));assert.ok(h.state.open.has(h.key('basis',channel.key)));
});

test('one addition covering two models is counted once and shown only on actual triggering channels',()=>{
 const h=harness();h.state.report=report({actions:['add:g'],status:'increase'});Object.assign(h.state.report.realtime,{adjustment_count:1,increase_count:1,actions:[{id:'add:g',kind:'increase',group:'g',models:['m','m2'],scope:['g / m','g / m2'],reason:'已确认上游限流'}]});h.state.open.add(h.key('group','g'));h.state.open.add(h.key('channel','g',1));h.state.open.add(h.key('basis',channel.key));h.render(true);
 const html=h.element('ggRows').innerHTML;assert.equal(h.element('ggAdjust').textContent,'1');assert.equal(h.element('ggIncrease').textContent,'1');assert.equal((html.match(/1 项关联调整/g)||[]).length,2);assert.equal((html.match(/class="gg-action increase"/g)||[]).length,1);assert.equal((html.match(/class="gg-channel-row"/g)||[]).length,1);
});

test('no first-byte samples with confirmed errors retains fault counts and the exact sample model',()=>{
 const h=harness();h.state.report=report();const m={...channel,frt:{mean_ms:null,mean_unavailable_reason:'no_valid_samples'},attributed_errors:17,fault_rate_pct:100,pending_errors:0,decision:{...channel.decision,label:'继续观察',reason:'已确认渠道或上游错误，先处理具体故障',frt:'无有效首字耗时样本，暂不能比较速度',errors:'主要原因：上游 HTTP/2 流错误（上游侧）；待判 0 次'},error_reasons:[{reason:'上游 HTTP/2 流错误',count:17,responsibility:'上游侧',samples:[{request_id:'exact-id',model:'actual-model'}]}]};
 const html=h.channelDetails(m);assert.match(html,/17 次 · 100%/);assert.match(html,/无有效首字耗时样本/);assert.match(html,/上游 HTTP\/2 流错误/);assert.doesNotMatch(html,/采集落后/);h.state.open.add(h.key('basis',m.key));const details=h.channelDetails(m);assert.match(details,/actual-model/);assert.match(details,/采集覆盖完整/);
});

test('channel shows its own 15/30 minute window, both sample counts and backend expansion reason',()=>{
 const h=harness();h.state.report=report();h.renderHeader();assert.match(h.element('ggCoverage').textContent,/15.*低样本渠道.*30/);
 for(const minutes of [15,30]){
  const m={...channel,window_minutes:minutes,requests:17,frt:{...channel.frt,observed:9},window_reason:minutes===30?'近 15 分钟样本不足（请求尝试 3 次、有效首字样本 1 个，门槛各 20），已采用近 30 分钟数据':''};
  h.state.open.add(h.key('channel','g',1));const html=h.channelHTML({group:'g',actions:[]},{id:1,name:'c1',status:1,metric:m});
  assert.match(html,new RegExp('最近'+minutes+'分钟 17 次尝试'));assert.match(html,/9 个有效首字样本/);
  if(minutes===30)assert.ok(html.includes(m.window_reason));assert.equal(h.requests.length,0);
 }
});
