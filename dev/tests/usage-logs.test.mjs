import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../../monitor/usage_logs.js',import.meta.url),'utf8');
const meta={quota_per_unit:500000,quota_display_type:'CNY',usd_exchange_rate:7,groups:['default']};
const row={id:1,type:2,created_at:1794500000,user_id:31,username:'fixture',token_name:'token',model_name:'model',group:'default',quota:500000,prompt_tokens:100,completion_tokens:20,use_time:2,is_stream:true,other:'{}'};
const ok=data=>new Response(JSON.stringify({success:true,data}),{headers:{'content-type':'application/json'}});
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return{promise,resolve}};
function fixture(handler=()=>ok({})){ 
  const nodes=new Map(),calls=[],timers=new Map();let timerID=0;
  const el=id=>{
    if(!nodes.has(id)){const classes=new Set();nodes.set(id,{id,value:'',hidden:false,disabled:false,innerHTML:'',textContent:'',dataset:{},style:{},
      classList:{add:x=>classes.add(x),remove:x=>classes.delete(x),toggle(x,on){if(on)classes.add(x);else classes.delete(x)},contains:x=>classes.has(x)},
      setAttribute(){},addEventListener(){},querySelector:()=>el(id+':child'),querySelectorAll:()=>[],remove(){},click(){}})}
    return nodes.get(id);
  };
  const context=vm.createContext({window:{matchMedia:()=>({matches:false})},document:{getElementById:el,createElement:()=>el('anchor'),body:{appendChild(){}},addEventListener(){}},
    localStorage:{getItem:()=>null,setItem(){}},location:{pathname:'/',search:'',hash:''},history:{replaceState(){}},navigator:{},
    URLSearchParams,AbortController,
    setTimeout:(fn,ms)=>{const id=++timerID;timers.set(id,{fn,ms});return id},clearTimeout:id=>timers.delete(id),atob:s=>Buffer.from(s,'base64').toString('binary'),
    fetch:(url,options={})=>{calls.push({url,options});return handler(url,options)}});
  vm.runInContext(source.replace('window.usageLogsActivate=activate;',`globalThis.api={st,activate,deactivate,search,prepareSearch,loadMeta,loadList,loadCount,loadStat,cancelQueries,openUser,openDetail,openModal,closeModal,cellCost,queryParams,fmtQuota,detailSegments,loadShortcuts,renderShortcuts,onShortcutBarClick,onShortcutModalClick,saveShortcut,openShortcutManage,scApply,rejectionReason,cellRejection,renderTable,readHash,onPagerClick};window.usageLogsActivate=activate;`),context);
  const api=context.api;api.st.meta={...meta};api.st.applied={startTime:1794400000000,endTime:1794500000000};api.st.pageSize=100;
  return{...api,el,calls,timers,context};
}

test('explicit rejection is visible without changing consumption type or cost; text is escaped',()=>{
  const v=fixture(),it={...row,other:JSON.stringify({admin_info:{reject_reason:'claude_stop_reason=refusal <img src=x onerror=alert(1)>'}})};
  assert.match(v.rejectionReason(it),/^claude_stop_reason=refusal/);
  const cell=v.cellRejection(it,4);assert.match(cell,/模型拒答/);assert.match(cell,/data-detail="4"/);assert.doesNotMatch(cell,/<img/);assert.match(cell,/&lt;img/);
  v.st.items=[it];v.renderTable();assert.match(v.el('ulTableWrap').innerHTML,/拒绝原因/);assert.match(v.el('ulTableWrap').innerHTML,/ul-row-amber/);assert.match(v.el('ulTableWrap').innerHTML,/>消耗</);
  assert.equal(it.type,2);assert.match(v.cellCost(it),/¥7/);
  v.openDetail(it);assert.match(v.el('ulModalBody').innerHTML,/拒绝原因/);assert.doesNotMatch(v.el('ulModalBody').innerHTML,/<img/);
  v.st.cols.rejection=false;v.renderTable();assert.doesNotMatch(v.el('ulTableWrap').innerHTML,/<th class="ul-c-rejection"/);
});

test('rejection presence accepts legacy strings but not malformed JSON, non-string or explicit nested null',()=>{
  const v=fixture();
  for(const other of ['invalid','null','[]','{}','{"reject_reason":false}','{"reject_reason":12}','{"reject_reason":{}}','{"reject_reason":" \t\r\n"}','{"reject_reason":"legacy","admin_info":{"reject_reason":null}}','{"reject_reason":"legacy","admin_info":{"reject_reason":false}}']){
    assert.equal(v.rejectionReason({...row,other}),'',other);
  }
  for(const other of ['{"reject_reason":"legacy"}','{"reject_reason":"legacy","admin_info":null}','{"reject_reason":"legacy","admin_info":"old-format"}'])assert.equal(v.rejectionReason({...row,other}),'legacy');
  assert.equal(v.rejectionReason({...row,other:'{"reject_reason":"legacy","admin_info":{"reject_reason":"nested"}}'}),'nested');
  assert.doesNotMatch(v.cellRejection({...row,other:'{"admin_info":{"reject_reason":"claude_stop_reason=end_turn"}}'},0),/模型拒答/);
});

test('diagnostics survive hash and list parameters but cannot alter existing quota or rate scope',async()=>{
  const v=fixture(url=>ok(url.includes('skip_quota=1')?{rpm:1,tpm:2}:{quota:500000}));
  v.context.location.hash='#tab=usage-logs&type=issues&message='+encodeURIComponent('Invalid %_!');
  const draft=v.readHash();assert.equal(draft.type,'issues');assert.equal(draft.message,'Invalid %_!');
  Object.assign(v.st.applied,draft,{startTime:1794400000000,endTime:1794403600000});
  const list=v.queryParams(false);assert.equal(list.get('diagnostic_scope'),'issues');assert.equal(list.get('message'),'Invalid %_!');assert.equal(list.has('type'),false);
  await v.loadStat();assert.equal(v.calls.length,2);
  for(const call of v.calls){const q=new URL(call.url,'http://localhost').searchParams;assert.equal(q.has('diagnostic_scope'),false);assert.equal(q.has('message'),false)}
  v.st.applied.type='rejected';assert.equal(v.queryParams(false).get('diagnostic_scope'),'rejected');
  v.st.applied.type='5';assert.equal(v.queryParams(false).get('type'),'5');assert.equal(v.queryParams(false).has('diagnostic_scope'),false);
});

test('currency settings failure cannot show money using fallback rates; retry recovers',async()=>{
  let fail=true;
  const v=fixture(()=>fail?new Response('{"success":false,"message":"offline"}'):ok(meta));
  v.st.meta=null;
  assert.equal(await v.loadMeta(),false);assert.equal(v.st.meta,null);
  assert.match(v.el('ulTableWrap').innerHTML,/货币设置/);assert.equal(v.calls.length,1);
  fail=false;assert.equal(await v.loadMeta(),true);assert.equal(v.fmtQuota(500000),'¥7');
});

test('superseded lists are cancelled and late responses cannot replace current rows',async()=>{
  const first=deferred();let n=0;
  const v=fixture(()=>++n===1?first.promise:ok({items:[{...row,id:2}],total:1,page_size:100}));
  const p=v.loadList();await v.loadList();assert.equal(v.calls[0].options.signal.aborted,true);
  first.resolve(ok({items:[row],total:1,page_size:100}));await p;
  assert.equal(v.st.items[0].id,2);assert.equal(v.el('ulSearch').disabled,false);
});

test('stat refresh clears old money and leaving a tab cancels queries without updating hidden UI',async()=>{
  const waiting=deferred(),v=fixture(()=>waiting.promise);v.st.inited=true;v.st.stat={quota:500000};
  const p=v.loadStat();assert.equal(v.st.stat,null);assert.match(v.el('ulStats').innerHTML,/skeleton/);
  const before=v.el('ulStats').innerHTML;v.deactivate();assert.equal(v.calls[0].options.signal.aborted,true);
  waiting.resolve(ok({quota:999999}));await p;assert.equal(v.el('ulStats').innerHTML,before);
  assert.doesNotThrow(()=>fixture().deactivate());
});

test('only identical in-flight statistics are coalesced; completed totals refresh on a new search',async()=>{
  const waiting=deferred();let count=0;
  const v=fixture(url=>url.includes('skip_quota=1')?ok({rpm:3,tpm:40}):++count===1?waiting.promise:ok({quota:200}));
  v.st.applied.endTime=v.st.applied.startTime+3600000;
  const first=v.loadStat();
  Object.assign(v.st.applied,{type:'2',requestId:'r1',upstreamRequestId:'up-r1'});
  await v.loadStat();assert.equal(v.calls.length,2);assert.equal(v.calls[0].options.signal.aborted,false);
  const url=new URL(v.calls[0].url,'http://localhost');
  for(const key of['type','request_id','upstream_request_id'])assert.equal(url.searchParams.has(key),false);
  waiting.resolve(ok({quota:100}));await first;assert.equal(v.st.stat.quota,100);
  await v.loadStat();assert.equal(v.calls.length,4);assert.equal(v.st.stat.quota,200);
});

test('a changed statistics scope cancels the old sum and cannot inherit its result',async()=>{
  const waiting=deferred();let count=0;
  const v=fixture(url=>url.includes('skip_quota=1')?ok({rpm:3,tpm:40}):++count===1?waiting.promise:ok({quota:200}));
  v.st.applied.endTime=v.st.applied.startTime+3600000;
  const first=v.loadStat();v.st.applied.username='other-user';await v.loadStat();
  assert.equal(v.calls[0].options.signal.aborted,true);assert.equal(v.st.stat.quota,200);
  waiting.resolve(ok({quota:999999}));await first;assert.equal(v.st.stat.quota,200);
});

test('default time selection is frozen on activation and stays fixed during paging',async()=>{
  const v=fixture(url=>ok(url.includes('/list?')?{items:[],total:0,page_size:100}:{quota:0}));
  v.st.inited=true;await v.activate();
  assert.ok(v.st.applied.startTime>0);assert.ok(v.st.applied.endTime>v.st.applied.startTime);
  const end=v.queryParams(true).get('end_timestamp');v.st.page=2;
  assert.equal(v.queryParams(true).get('end_timestamp'),end);
});

test('late user info cannot overwrite a log detail dialog or a newer user',async()=>{
  const waiting=deferred(),v=fixture(()=>waiting.promise);
  const p=v.openUser(31);v.openDetail({...row});const before=v.el('ulModalBody').innerHTML;
  assert.equal(v.calls[0].options.signal.aborted,true);
  waiting.resolve(ok({id:31,username:'late-user'}));await p;
  assert.equal(v.el('ulModalBody').innerHTML,before);assert.doesNotMatch(before,/late-user/);
});

test('filters remain bound to the submitted search, with type/IDs retained',()=>{
  const v=fixture();Object.assign(v.st.applied,{type:'5',requestId:'本站',upstreamRequestId:'上游',username:'some%'});
  const q=v.queryParams(false);assert.equal(q.get('type'),'5');assert.equal(q.get('request_id'),'本站');assert.equal(q.get('upstream_request_id'),'上游');assert.equal(q.get('username'),'some%');
});

test('sequential page uses only its own cursor; jumps without an anchor keep normal paging',async()=>{
  const v=fixture(()=>ok({items:[{...row}],total:2005,page_size:100,next_cursor:{created_at:1794500000,id:1}}));
  await v.loadList();v.st.page=2;
  assert.equal(v.queryParams(true).get('before_id'),'1');
  assert.equal(v.queryParams(true).get('before_created_at'),'1794500000');
  v.st.page=12;assert.equal(v.queryParams(true).get('before_id'),null);
  v.st.pageCursors.clear();v.st.page=2;assert.equal(v.queryParams(true).get('before_id'),null);
});

test('legacy/unknown/refund/subscription logs render safely; untrusted strings remain escaped',()=>{
  const v=fixture();
  for(const type of [0,1,2,3,4,5,6,7,99])for(const other of ['', 'null', 'invalid', '{"admin_info":null}', '{"billing_source":"subscription","subscription_consumed":0}']){
    assert.doesNotThrow(()=>v.openDetail({...row,type,other,content:'<script>bad()</script>'}));
    assert.doesNotMatch(v.el('ulModalBody').innerHTML,/<script>/);
  }
  assert.match(v.cellCost({...row,other:'{"billing_source":"subscription","subscription_consumed":0}'}),/¥0/);
  v.openDetail({...row,other:'{"future_field":9007199254740993}'});
  assert.match(v.el('ulModalBody').innerHTML,/原始日志数据/);assert.match(v.el('ulModalBody').innerHTML,/9007199254740993/);
});

const tick=()=>new Promise(resolve=>setImmediate(resolve));

function smartStatsFixture(items,extra={}){
  const v=fixture(url=>url.includes('/list?')?ok({items,total:items.length,has_more:false,page_size:100,...extra}):url.includes('/count?')?ok({total:300}):ok(url.includes('skip_quota=1')?{rpm:3,tpm:120}:{quota:500000}));
  v.st.applied.endTime=v.st.applied.startTime+3600000;
  return v;
}
const clickCount=v=>v.onPagerClick({target:{closest:s=>s==='[data-count-calc]'?{disabled:false}:null}});

test('non-consume types never request statistics, including an explicit retry; no fabricated zero',async()=>{
  for(const type of ['1','3','4','5','6','7']){
    const v=smartStatsFixture([{...row,type:Number(type)}]);v.st.applied.type=type;
    v.prepareSearch();await v.loadList();await v.loadStat(true);await tick();
    assert.equal(v.calls.filter(c=>c.url.includes('/stat?')).length,0,type);
    assert.equal(v.calls.length,1,'no extra presence probe or aggregation');
    assert.match(v.el('ulStats').innerHTML,/所选日志类型不包含消费/);
    assert.doesNotMatch(v.el('ulStats').innerHTML,/skeleton|¥0|RPM <b>0/);
    v.cancelQueries();assert.match(v.el('ulStats').innerHTML,/不包含消费/);
  }
});
test('ordinary all/consume customer-model-group searches retain automatic quota and real-time rates',async()=>{
  for(const type of ['0','2']){
    const v=smartStatsFixture([row]);Object.assign(v.st.applied,{type,username:'fixture',group:'default',model:'model',channel:'7'});
    v.prepareSearch();await v.loadList();await tick();
    const calls=v.calls.filter(c=>c.url.includes('/stat?'));assert.equal(calls.length,2,type);
    for(const c of calls){const q=new URL(c.url,'http://localhost').searchParams;for(const k of ['username','group','model_name','channel'])assert.ok(q.has(k),k)}
    assert.match(v.el('ulStats').innerHTML,/¥7/);assert.match(v.el('ulStats').innerHTML,/RPM <b>3/);assert.match(v.el('ulStats').innerHTML,/TPM <b>120/);
    assert.match(v.el('ulStats').innerHTML,/最近 60 秒/);
  }
});
test('complete first page without consumption skips stats; zero rows is not a zero monetary result',async()=>{
  for(const items of [[],[{...row,type:5}],[{...row,type:5},{...row,id:2,type:6}]]){
    const v=smartStatsFixture(items);v.prepareSearch();await v.loadList();await tick();
    assert.equal(v.calls.length,1);assert.equal(v.st.stat,null);assert.equal(v.st.consumeEvidence.status,'none');
    assert.match(v.el('ulStats').innerHTML,/完整查询结果不包含消费/);assert.doesNotMatch(v.el('ulStats').innerHTML,/¥0|skeleton/);
  }
});
test('absence on a partial first page or final later page cannot suppress valid automatic statistics',async()=>{
  const v=smartStatsFixture([{...row,type:5}],{total:null,total_pending:true,has_more:true,page_size:100});
  v.prepareSearch();await v.loadList();await tick();assert.equal(v.st.consumeEvidence.status,'unknown');
  assert.equal(v.calls.filter(c=>c.url.includes('/stat?')).length,2);assert.match(v.el('ulStats').innerHTML,/¥7/);
  v.st.page=2;await v.loadList();await tick();assert.equal(v.st.consumeEvidence.status,'unknown');assert.match(v.el('ulStats').innerHTML,/¥7/);
  const w=smartStatsFixture([{...row,type:5}],{total:101,has_more:false});w.st.page=2;
  w.prepareSearch();await w.loadList();await tick();assert.equal(w.st.consumeEvidence.status,'unknown');assert.equal(w.calls.filter(c=>c.url.includes('/stat?')).length,2);
});
test('consumption evidence survives paging but expires on a new search, customer or refresh',async()=>{
  let consumes=true;
  const v=fixture(url=>url.includes('/list?')?ok({items:[{...row,type:consumes?2:5}],total:1,page_size:100}):ok(url.includes('skip_quota=1')?{rpm:3,tpm:120}:{quota:500000}));
  v.st.applied.endTime=v.st.applied.startTime+3600000;
  v.prepareSearch();await v.loadList();await tick();assert.equal(v.st.consumeEvidence.status,'present');
  consumes=false;v.st.page=2;await v.loadList();assert.equal(v.st.consumeEvidence.status,'present');assert.match(v.el('ulStats').innerHTML,/¥7/);
  v.st.page=1;v.prepareSearch();await v.loadList();await tick();assert.equal(v.st.consumeEvidence.status,'none');assert.match(v.el('ulStats').innerHTML,/不包含消费/);
  consumes=true;v.prepareSearch();await v.loadList();await tick();assert.equal(v.st.consumeEvidence.status,'present');assert.match(v.el('ulStats').innerHTML,/¥7/);
});
test('switching to errors aborts old quota/rates and late success cannot resurrect their cards',async()=>{
  const wait=deferred(),v=fixture(()=>wait.promise);v.st.applied.endTime=v.st.applied.startTime+3600000;
  const task=v.loadStat();v.st.applied.type='5';v.prepareSearch();
  assert.ok(v.calls.every(c=>c.options.signal.aborted));assert.equal(v.st.statLoading,false);
  const note=v.el('ulStats').innerHTML;assert.match(note,/所选日志类型不包含消费/);
  wait.resolve(ok({quota:999999,rpm:999,tpm:999}));await task;
  assert.equal(v.el('ulStats').innerHTML,note);assert.equal(v.st.stat,null);
});
test('mixed issues/rejection modes can consume and keep automatic statistics when consume rows exist',async()=>{
  for(const type of ['issues','rejected']){
    const v=smartStatsFixture([row],{total:null,total_pending:true,has_more:true});v.st.applied.type=type;
    v.prepareSearch();await v.loadList();await tick();
    assert.equal(v.calls.filter(c=>c.url.includes('/stat?')).length,2);assert.equal(v.calls.filter(c=>c.url.includes('/count?')).length,0);
    assert.match(v.el('ulStats').innerHTML,/¥7/);assert.match(v.el('ulPager').innerHTML,/总计：未计算/);
  }
});
test('diagnostic exact count is explicit, coalesced, does not block paging and is reused afterwards',async()=>{
  const waiting=deferred();let counts=0;
  const v=fixture(url=>url.includes('/count?')?(++counts,waiting.promise):url.includes('/list?')?ok({items:[row],total:null,total_pending:true,has_more:true,page_size:100,next_cursor:{created_at:row.created_at,id:row.id}}):ok({}));
  v.st.applied.message='refusal';await v.loadList();
  assert.equal(counts,0);assert.match(v.el('ulPager').innerHTML,/计算总数/);assert.equal(v.st.hasMore,true);
  clickCount(v);clickCount(v);assert.equal(counts,1);assert.match(v.el('ulPager').innerHTML,/data-count-calc disabled/);
  v.st.page=2;await v.loadList();assert.equal(counts,1);assert.equal(v.queryParams(true).has('before_id'),true);
  waiting.resolve(ok({total:300}));await tick();assert.equal(v.st.totalKnown,true);assert.equal(v.st.total,300);
  v.st.page=3;await v.loadList();await tick();assert.equal(counts,1);assert.equal(v.st.total,300);
});
test('diagnostic count failure restores a clickable retry without hiding rows or inventing zero',async()=>{
  let fail=true;
  const v=fixture(url=>url.includes('/count?')?(fail?new Response('{"success":false,"message":"timeout"}'):ok({total:30})):ok({items:[row],total:null,total_pending:true,has_more:true,page_size:100}));
  v.st.applied.type='issues';await v.loadList();clickCount(v);await tick();
  assert.equal(v.st.totalKnown,false);assert.equal(v.st.items.length,1);assert.equal(v.st.hasMore,true);
  assert.match(v.el('ulPager').innerHTML,/暂不可用/);assert.doesNotMatch(v.el('ulPager').innerHTML,/data-count-calc disabled/);
  fail=false;clickCount(v);await tick();assert.equal(v.st.totalKnown,true);assert.equal(v.st.total,30);
});
test('old diagnostic count cannot overwrite a changed search; completed small results need no count',async()=>{
  const waiting=deferred();let countCalls=0;
  const v=fixture(url=>url.includes('/count?')?(++countCalls,waiting.promise):url.includes('message=new')?ok({items:[{...row,id:2}],total:1,page_size:100}):ok({items:[row],total:null,total_pending:true,has_more:true,page_size:100}));
  v.st.applied.message='old';await v.loadList();clickCount(v);
  const old=v.calls.find(c=>c.url.includes('/count?'));v.st.applied.message='new';v.prepareSearch();await v.loadList();
  assert.equal(old.options.signal.aborted,true);assert.equal(v.st.total,1);
  waiting.resolve(ok({total:9999}));await tick();assert.equal(v.st.total,1);assert.equal(countCalls,1);assert.equal(v.st.items[0].id,2);
});

const batchCursor=id=>({id,created_at:row.created_at});
function batchReply(items,{pending=false,more=false,cursor=null,scanned=1000,total=null}={}){
  return{items,bounded_search:true,scan_pending:pending,scan_cursor:pending?cursor:null,has_more:more,next_cursor:more?cursor:null,scanned,total,total_pending:total===null};
}
function boundedFixture(handler){
  const v=fixture((url,options)=>url.includes('/list?')?handler(new URL(url,'http://localhost').searchParams,options):ok(url.includes('/count?')?{total:1234}:url.includes('skip_quota=1')?{rpm:1,tpm:20}:{quota:500000}));
  v.st.meta.diagnostic_batch_enabled=true;v.st.applied.message='refusal';v.st.applied.endTime=v.st.applied.startTime+3600000;
  return v;
}
test('bounded empty batches stop only at safety limit, retain progress, and cannot claim no matches in the full range',async()=>{
  let n=0;const v=boundedFixture(()=>ok(batchReply([],{pending:true,cursor:batchCursor(100000-1000*++n)})));
  v.prepareSearch();await v.loadList();await tick();assert.equal(n,64);assert.equal(v.st.scanPending,true);assert.equal(v.st.scanScanned,64000);
  assert.match(v.el('ulTableWrap').innerHTML,/尚未查完/);assert.match(v.el('ulPager').innerHTML,/恢复查询/);
  assert.equal(v.calls.filter(x=>x.url.includes('/stat?')).length,0);assert.equal(v.calls.filter(x=>x.url.includes('/count?')).length,0);
  assert.equal(v.st.totalKnown,false);assert.match(v.el('ulStats').innerHTML,/检索尚未完成/);
  assert.equal(new URL(v.calls[1].url,'http://localhost').searchParams.get('before_id'),'99000');
});
test('empty intermediate batches auto-continue to true exhaustion without inventing a single-snapshot total',async()=>{
  let n=0;const v=boundedFixture(()=>ok(++n<=3?batchReply([],{pending:true,cursor:batchCursor(100-10*n)}):batchReply([],{scanned:50})));
  v.prepareSearch();await v.loadList();await tick();
  assert.equal(n,4);assert.equal(new URL(v.calls[3].url,'http://localhost').searchParams.get('before_id'),'70');
  assert.equal(v.st.scanPending,false);assert.equal(v.st.scanScanned,3050);assert.equal(v.st.totalKnown,false);
  assert.match(v.el('ulTableWrap').innerHTML,/已查完所选范围/);assert.equal(v.calls.filter(c=>c.url.includes('/stat?')).length,0);
});
test('bounded page filling combines ordered results and requests only remaining rows; statistics start once',async()=>{
  let n=0;const ids=[30,19,9];const v=boundedFixture(q=>{
    const i=n++;assert.equal(q.get('page_size'),String(100-i));
    return ok(batchReply([{...row,id:ids[i]}],i<2?{pending:true,cursor:batchCursor(i===0?20:10)}:{scanned:50}));
  });
  v.prepareSearch();await v.loadList();await tick();assert.equal(n,3);assert.deepEqual(Array.from(v.st.items,x=>x.id),ids);
  assert.equal(v.st.pageSize,100);assert.equal(v.st.scanPending,false);assert.equal(v.calls.filter(c=>c.url.includes('/stat?')).length,2);
});
test('matched page cursor, not candidate tail, preserves matches not yet displayed',async()=>{
  let n=0;const v=boundedFixture(q=>{n++;return ok(n===1?batchReply([{...row,id:30},{...row,id:29}],{more:true,cursor:batchCursor(29),scanned:2}):batchReply([{...row,id:28}],{scanned:30}))});
  v.st.pageSize=2;await v.loadList();assert.equal(n,1);assert.equal(v.st.pageCursors.get(2).id,29);
  v.st.page=2;await v.loadList();assert.equal(new URL(v.calls[1].url,'http://localhost').searchParams.get('before_id'),'29');
  assert.equal(v.st.items[0].id,28);
});
test('a full page without another match disables next, instead of offering a false empty page',async()=>{
  let n=0;const v=boundedFixture(q=>{n++;if(n===1)return ok(batchReply([{...row,id:30},{...row,id:29}],{pending:true,cursor:batchCursor(10)}));assert.equal(q.get('page_size'),'1');return ok(batchReply([],{scanned:10}))});
  v.st.pageSize=2;await v.loadList();assert.equal(n,2);assert.equal(v.st.pageCursors.has(2),false);assert.equal(v.st.hasMore,false);assert.equal(v.st.totalKnown,false);
  assert.match(v.el('ulPager').innerHTML,/data-page="2" disabled/);assert.doesNotMatch(v.el('ulPager').innerHTML,/查找下一页/);
});
test('proof-only lookup keeps the discovered next row reachable and never appends it to a full page',async()=>{
  let n=0;const v=boundedFixture(q=>{n++;if(n===1)return ok(batchReply([{...row,id:30},{...row,id:29}],{pending:true,cursor:batchCursor(10)}));assert.equal(q.get('before_id'),'10');return ok(batchReply([{...row,id:9}],{scanned:3}))});
  v.st.pageSize=2;await v.loadList();assert.equal(n,2);assert.equal(v.st.hasMore,true);assert.equal(v.st.scanPending,false);
  assert.deepEqual(Array.from(v.st.items,x=>x.id),[30,29]);assert.equal(v.st.pageCursors.get(2).id,10);
  v.st.page=2;await v.loadList();assert.deepEqual(Array.from(v.st.items,x=>x.id),[9]);assert.equal(v.st.hasMore,false);
});
test('proof failure leaves a full but unfinished page, blocks next and resumes without duplicate rows',async()=>{
  let n=0,fail=true;const v=boundedFixture(()=>++n===1?ok(batchReply([{...row,id:30},{...row,id:29}],{pending:true,cursor:batchCursor(10)})):fail?new Response('{"success":false,"message":"timeout"}'):ok(batchReply([{...row,id:9}],{scanned:3})));
  v.st.pageSize=2;await v.loadList();assert.equal(v.st.scanPending,true);assert.equal(v.st.pageCursors.has(2),false);assert.match(v.el('ulTableWrap').innerHTML,/本页未完成/);assert.match(v.el('ulPager').innerHTML,/data-page="2" disabled/);
  fail=false;await v.loadList(true);assert.equal(v.st.hasMore,true);assert.deepEqual(Array.from(v.st.items,x=>x.id),[30,29]);assert.equal(v.st.pageCursors.get(2).id,10);
});
test('failed later batch retains rows/checkpoint and can retry without skipping the failed interval',async()=>{
  let n=0,fail=true;const v=boundedFixture(q=>{
    n++;if(n===1)return ok(batchReply([{...row,id:30}],{pending:true,cursor:batchCursor(20)}));
    assert.equal(q.get('before_id'),'20');if(fail)return new Response('{"success":false,"message":"source failed"}');
    return ok(batchReply([{...row,id:19}],{scanned:100}));
  });
  v.prepareSearch();await v.loadList();await tick();assert.equal(v.st.items.length,1);assert.equal(v.st.scanCursor.id,20);assert.match(v.el('ulPager').innerHTML,/source failed/);
  fail=false;await v.loadList(true);await tick();assert.deepEqual(Array.from(v.st.items,x=>x.id),[30,19]);assert.equal(v.st.scanPending,false);
});
test('cancellation retains only completed batch state; late source response cannot advance it or issue more batches',async()=>{
  const waiting=deferred();let n=0;const v=boundedFixture(()=>++n===1?ok(batchReply([{...row,id:30}],{pending:true,cursor:batchCursor(20)})):waiting.promise);
  v.prepareSearch();const p=v.loadList();await tick();assert.equal(n,2);v.cancelQueries();
  assert.equal(v.st.items[0].id,30);assert.equal(v.st.scanCursor.id,20);assert.match(v.el('ulPager').innerHTML,/已取消/);
  waiting.resolve(ok(batchReply([{...row,id:19}],{pending:true,cursor:batchCursor(10)})));await p;
  assert.equal(n,2);assert.equal(v.st.scanCursor.id,20);assert.equal(v.st.items.length,1);
});
test('unsafe/non-progressing cursors fail closed and cannot cause an auto-request loop',async()=>{
  let n=0;const v=boundedFixture(()=>ok(batchReply([],{pending:true,cursor:batchCursor(20)})));
  v.prepareSearch();await v.loadList();assert.equal(v.calls.filter(c=>c.url.includes('/list?')).length,2);
  assert.equal(v.st.scanCursor.id,20);assert.match(v.el('ulPager').innerHTML,/续查位置无效/);
});
test('new bounded search cancels an old pending batch without mixing its results into the new filter',async()=>{
  const old=deferred();let n=0;const v=boundedFixture(q=>q.get('message')==='old'?old.promise:ok(batchReply([{...row,id:2}],{scanned:1,total:1})));
  v.st.applied.message='old';v.prepareSearch();const p=v.loadList();v.st.applied.message='new';v.prepareSearch();await v.loadList();await tick();
  old.resolve(ok(batchReply([{...row,id:30}],{pending:true,cursor:batchCursor(20)})));await p;
  assert.equal(v.st.items[0].id,2);assert.equal(v.st.scanPending,false);assert.equal(v.st.scanScanned,1);
});
test('a failed new page cannot inherit the old unfinished page checkpoint',async()=>{
  let n=0;const v=boundedFixture(()=>++n===1?ok(batchReply([{...row,id:30}],{pending:true,cursor:batchCursor(20)})):new Response('{"success":false,"message":"offline"}'));
  v.st.page=2;v.st.pageCursors.set(2,batchCursor(40));await v.loadList();assert.equal(v.st.scanPending,true);
  v.st.page=1;await v.loadList();assert.equal(v.st.scanPending,false);assert.equal(v.st.scanCursor,null);
  assert.doesNotMatch(v.el('ulPager').innerHTML,/恢复查询/);assert.equal(v.st.items.length,0);
});
test('one wall deadline covers automatic batches; timeout preserves only the last committed checkpoint',async()=>{
  const waiting=deferred();let n=0;const v=boundedFixture(()=>++n===1?ok(batchReply([],{pending:true,cursor:batchCursor(20)})):waiting.promise);
  v.prepareSearch();const p=v.loadList();await tick();
  const timer=[...v.timers.values()].find(x=>x.ms===30000);assert.ok(timer);timer.fn();
  waiting.resolve(ok(batchReply([],{pending:true,cursor:batchCursor(10)})));await p;
  assert.equal(n,2);assert.equal(v.st.scanCursor.id,20);assert.equal(v.st.scanPending,true);assert.match(v.el('ulPager').innerHTML,/等待超时/);
});

test('rates become visible before a slow quota and the responses do not erase each other',async()=>{
 const sum=deferred(),v=fixture(url=>url.includes('quota_only=1')?sum.promise:ok({quota:null,quota_skipped:true,rpm:6,tpm:120}));
 v.st.applied.endTime=v.st.applied.startTime+3600000;
 const pending=v.loadStat();await tick();
 assert.match(v.el('ulStats').innerHTML,/RPM <b>6/);assert.match(v.el('ulStats').innerHTML,/TPM <b>120/);
 assert.match(v.el('ulStats').innerHTML,/sky skeleton/);assert.equal(v.st.statLoading,true);
 assert.equal(v.calls.length,2);assert.ok(v.calls[0].url.includes('skip_quota=1'));assert.ok(v.calls[1].url.includes('quota_only=1'));
 sum.resolve(ok({quota:500000,rates_skipped:true,rpm:null,tpm:null}));await pending;
 assert.match(v.el('ulStats').innerHTML,/¥7/);assert.match(v.el('ulStats').innerHTML,/RPM <b>6/);assert.doesNotMatch(v.el('ulStats').innerHTML,/skeleton/);
});
test('quota may finish first without inventing zero RPM or overwriting the amount later',async()=>{
 const rate=deferred(),v=fixture(url=>url.includes('skip_quota=1')?rate.promise:ok({quota:500000,rates_skipped:true}));
 v.st.applied.endTime=v.st.applied.startTime+3600000;
 const pending=v.loadStat();await tick();assert.match(v.el('ulStats').innerHTML,/¥7/);
 assert.match(v.el('ulStats').innerHTML,/rose skeleton/);assert.doesNotMatch(v.el('ulStats').innerHTML,/RPM <b>0/);
 rate.resolve(ok({quota:null,quota_skipped:true,rpm:8,tpm:90}));await pending;
 assert.match(v.el('ulStats').innerHTML,/¥7/);assert.match(v.el('ulStats').innerHTML,/RPM <b>8/);
});
test('partial errors keep successful results and allow an explicit quota retry',async()=>{
 let fail=true;
 const v=fixture(url=>url.includes('skip_quota=1')?ok({rpm:9,tpm:80}):fail?new Response('{"success":false,"message":"sum timeout"}'):ok({quota:500000}));
 v.st.applied.endTime=v.st.applied.startTime+3600000;
 await v.loadStat();assert.match(v.el('ulStats').innerHTML,/RPM <b>9/);assert.match(v.el('ulStats').innerHTML,/用量计算失败 · 重试/);
 assert.doesNotMatch(v.el('ulStats').innerHTML,/¥0/);
 fail=false;await v.loadStat(true);assert.match(v.el('ulStats').innerHTML,/¥7/);assert.doesNotMatch(v.el('ulStats').innerHTML,/失败/);
 const w=fixture(url=>url.includes('skip_quota=1')?new Response('{"success":false,"message":"rates timeout"}'):ok({quota:500000}));
 w.st.applied.endTime=w.st.applied.startTime+3600000;
 await w.loadStat();assert.match(w.el('ulStats').innerHTML,/¥7/);assert.match(w.el('ulStats').innerHTML,/RPM 暂不可用/);
});
test('cancel after a partial result stops both requests and leaves no endless skeleton',async()=>{
 const sum=deferred(),v=fixture(url=>url.includes('skip_quota=1')?ok({rpm:3,tpm:10}):sum.promise);
 v.st.applied.endTime=v.st.applied.startTime+3600000;
 const p=v.loadStat();await tick();v.cancelQueries();
 assert.equal(v.calls.find(c=>c.url.includes('quota_only=1')).options.signal.aborted,true);assert.doesNotMatch(v.el('ulStats').innerHTML,/skeleton/);
 assert.match(v.el('ulStats').innerHTML,/RPM <b>3/);
 const frozen=v.el('ulStats').innerHTML;sum.resolve(ok({quota:999999}));await p;
 assert.equal(v.el('ulStats').innerHTML,frozen);assert.equal(v.st.statLoading,false);
});
test('both late parts from an old scope are ignored after changing filters',async()=>{
 const oldRates=deferred(),oldQuota=deferred();
 const v=fixture(url=>url.includes('username=new')?ok(url.includes('quota_only=1')?{quota:1000000}:{rpm:12,tpm:24}):url.includes('quota_only=1')?oldQuota.promise:oldRates.promise);
 v.st.applied.endTime=v.st.applied.startTime+3600000;
 const old=v.loadStat();v.st.applied.username='new';await v.loadStat();
 const current=v.el('ulStats').innerHTML;assert.match(current,/RPM <b>12/);assert.match(current,/¥14/);
 oldQuota.resolve(ok({quota:9999999}));oldRates.resolve(ok({rpm:9999,tpm:9999}));await old;
 assert.equal(v.el('ulStats').innerHTML,current);
 assert.ok(v.calls.slice(0,2).every(c=>c.options.signal.aborted));
});
test('a slow exact count blocks neither records nor statistics',async()=>{
  const counting=deferred();
  const v=fixture(url=>url.includes('/count?')?counting.promise:ok(url.includes('/stat?')?{quota:77}:{items:[row],total:null,total_pending:true,has_more:true,page_size:100}));
  v.st.applied.endTime=v.st.applied.startTime+3600000;
  v.st.statRequested=true;await v.loadList();
  assert.equal(v.st.items[0].id,1);assert.equal(v.st.totalKnown,false);assert.equal(v.el('ulSearch').disabled,false);
  assert.match(v.el('ulPager').innerHTML,/计算中/);assert.ok(v.calls.some(c=>c.url.includes('/stat?')));
  await tick();assert.equal(v.st.totalKnown,false);assert.equal(v.st.stat.quota,77);
  assert.match(v.calls[0].url,/defer_total=1/);
  counting.resolve(ok({total:2005}));await tick();
  assert.equal(v.st.total,2005);assert.equal(v.st.totalKnown,true);assert.equal(v.st.stat.quota,77);
  assert.equal(v.calls.filter(c=>c.url.includes('/count?')).length,1);
});
test('completed first-page results need no count request and no duplicate count for exact IDs',async()=>{
  const v=fixture(()=>ok({items:[row],total:1,page_size:100}));await v.loadList();
  assert.equal(v.st.totalKnown,true);assert.equal(v.st.total,1);assert.equal(v.calls.length,1);
});
test('identical in-flight statistics are not recomputed when they finish during deferred counting',async()=>{
  const statistic=deferred(),counting=deferred();let statCalls=0;
  const v=fixture(url=>url.includes('skip_quota=1')?ok({rpm:1,tpm:10}):url.includes('/stat?')?(++statCalls,statistic.promise):url.includes('/count?')?counting.promise:ok({items:[row],total_pending:true,total:null,has_more:true,page_size:100}));
  v.st.applied.endTime=v.st.applied.startTime+3600000;
  const first=v.loadStat();v.st.applied.type='2';v.prepareSearch();await v.loadList();
  assert.equal(v.st.statRequested,false);
  statistic.resolve(ok({quota:77}));await first;
  counting.resolve(ok({total:200}));await tick();assert.equal(statCalls,1);assert.equal(v.st.stat.quota,77);
  v.prepareSearch();assert.equal(v.st.statRequested,true); // completed results are never a persistent cache
});
test('paging shares an in-flight count and then reuses the completed total until the search is refreshed',async()=>{
  const counting=deferred();let countCalls=0;
  const v=fixture(url=>url.includes('/count?')?(++countCalls===1?counting.promise:ok({total:3000})):ok({items:[row],total:null,total_pending:true,has_more:true,page_size:100}));
  await v.loadList();v.st.page=2;await v.loadList();assert.equal(countCalls,1);
  counting.resolve(ok({total:2005}));await tick();assert.equal(v.st.total,2005);assert.equal(v.st.totalKnown,true);
  v.st.page=3;await v.loadList();await tick();assert.equal(countCalls,1,'same search must not re-run COUNT per page');assert.equal(v.st.total,2005);assert.equal(v.st.totalKnown,true);
  assert.match(v.el('ulPager').innerHTML,/按本次搜索时计算/);
  // search(), reset and page-size changes clear the reused total.
  v.st.totalKey=null;v.st.totalCached=null;v.st.page=4;await v.loadList();await tick();assert.equal(countCalls,2);assert.equal(v.st.total,3000);
  v.st.applied.username='changed';v.st.page=2;await v.loadList();await tick();assert.equal(countCalls,3,'changed filters need their own total');
  assert.match(source,/st\.applied=\{\.\.\.st\.draft\};st\.page=1;st\.pageCursors\.clear\(\);st\.totalKey=null;st\.totalCached=null;/);
  assert.match(source,/st\.page=1;st\.pageCursors\.clear\(\);st\.totalKey=null;st\.totalCached=null;writeHash\(\);loadList\(\)/);
});
test('rows newer than the reused total keep the next page reachable instead of bouncing back',async()=>{
  const v=fixture(url=>url.includes('/count?')?ok({total:100}):ok({items:[row],total:null,total_pending:true,has_more:true,page_size:100}));
  v.st.page=2;await v.loadList();await tick();
  assert.equal(v.st.totalKnown,true);assert.equal(v.st.page,2,'a non-empty page beyond a stale total must not redirect');
  assert.doesNotMatch(v.el('ulPager').innerHTML,/data-page="3" disabled/);
});
test('wide ranges defer the quota SUM to an explicit click; rates and narrow ranges stay automatic',async()=>{
  const v=fixture(url=>url.includes('/stat?')?(url.includes('skip_quota=1')?ok({quota:null,quota_skipped:true,rpm:3,tpm:40}):ok({quota:500000,quota_skipped:false,rpm:3,tpm:40})):ok({}));
  v.st.applied={startTime:1794400000000,endTime:1794400000000+27*3600*1000};
  await v.loadStat();
  assert.match(v.calls.at(-1).url,/skip_quota=1/);assert.match(v.el('ulStats').innerHTML,/data-quota-calc/);assert.match(v.el('ulStats').innerHTML,/RPM <b>3/);
  await v.loadStat(true);
  assert.doesNotMatch(v.calls.at(-1).url,/skip_quota/);assert.doesNotMatch(v.el('ulStats').innerHTML,/data-quota-calc/);assert.match(v.el('ulStats').innerHTML,/¥7/);
  v.st.applied={startTime:1794400000000,endTime:1794400000000+25*3600*1000};
  await v.loadStat();assert.doesNotMatch(v.calls.at(-1).url,/skip_quota/);
});
test('cancelling a clicked wide-range SUM restores the rates and the calculate button',async()=>{
  const hold=deferred();
  const v=fixture((url,o)=>url.includes('skip_quota=1')?ok({quota:null,quota_skipped:true,rpm:1,tpm:2}):new Promise((_,reject)=>{o.signal.addEventListener('abort',()=>reject(new DOMException('Cancelled','AbortError')),{once:true})}));
  v.st.applied={startTime:1794400000000,endTime:1794400000000+72*3600*1000};
  await v.loadStat();const sum=v.loadStat(true);v.cancelQueries();await sum.catch(()=>{});
  assert.match(v.el('ulStats').innerHTML,/data-quota-calc/);assert.equal(v.st.statLoading,false);hold.resolve();
});
test('changed filters cancel their old count and cannot inherit its late total',async()=>{
  const counting=deferred();let countCalls=0;
  const v=fixture(url=>url.includes('/count?')?(++countCalls===1?counting.promise:ok({total:200})):ok({items:[row],total:null,total_pending:true,has_more:true,page_size:100}));
  await v.loadList();const old=v.calls.find(c=>c.url.includes('/count?'));
  v.st.applied.username='other';await v.loadList();await tick();
  assert.equal(old.options.signal.aborted,true);assert.equal(v.st.total,200);
  counting.resolve(ok({total:999999}));await tick();assert.equal(v.st.total,200);
});
test('count failure keeps records and next-page access without inventing a zero total',async()=>{
  const v=fixture(url=>url.includes('/count?')?new Response('{"success":false,"message":"timeout"}'):ok({items:[row],total:null,total_pending:true,has_more:true,page_size:100}));
  await v.loadList();await tick();assert.equal(v.st.items.length,1);assert.equal(v.st.totalKnown,false);
  assert.match(v.el('ulPager').innerHTML,/暂不可用/);assert.doesNotMatch(v.el('ulPager').innerHTML,/data-page="2" disabled/);
});
test('cancel releases loading UI, aborts HTTP and ignores late list/count responses',async()=>{
  const waiting=deferred(),v=fixture(()=>waiting.promise);
  const list=v.loadList();v.cancelQueries();assert.equal(v.calls[0].options.signal.aborted,true);
  assert.equal(v.el('ulSearch').disabled,false);assert.equal(v.el('ulCancel').hidden,true);
  waiting.resolve(ok({items:[row],total:100}));await list;assert.equal(v.st.items.length,0);
  const counting=deferred(),w=fixture(url=>url.includes('/count?')?counting.promise:ok({items:[row],total:null,total_pending:true,has_more:true,page_size:100}));
  await w.loadList();w.cancelQueries();assert.equal(w.st.items.length,1);assert.equal(w.st.totalKnown,false);
  counting.resolve(ok({total:900}));await tick();assert.equal(w.st.totalKnown,false);assert.match(w.el('ulPager').innerHTML,/未完成/);
});
test('browser timeout aborts the request and restores controls instead of spinning forever',async()=>{
  const v=fixture((url,{signal})=>new Promise((resolve,reject)=>signal.addEventListener('abort',()=>{const e=new Error('aborted');e.name='AbortError';reject(e)},{once:true})));
  const request=v.loadList();const timer=[...v.timers.values()].find(t=>t.ms===30000);assert.ok(timer);timer.fn();await request;
  assert.equal(v.calls[0].options.signal.aborted,true);assert.equal(v.el('ulSearch').disabled,false);
  assert.match(v.el('ulTableWrap').innerHTML,/查询等待超时/);
});
test('repeated Enter shares the unchanged running search but changed filters still supersede it',async()=>{
  const waiting=deferred();let lists=0;
  const v=fixture(url=>url.includes('/list?')?(++lists===1?waiting.promise:ok({items:[{...row,id:2}],total:1,page_size:100})):ok({quota:1}));
  v.st.draft={...v.st.applied};await v.search();
  for(let i=0;i<20;i++)await v.search();
  assert.equal(lists,1);assert.equal(v.calls.find(c=>c.url.includes('/list?')).options.signal.aborted,false);
  v.el('ulUser').value='other';await v.search();await tick();assert.equal(lists,2);assert.equal(v.st.items[0].id,2);
  waiting.resolve(ok({items:[row],total:1,page_size:100}));await tick();assert.equal(v.st.items[0].id,2);
  await v.search();await tick();assert.equal(lists,3); // no completed-result cache
});

test('server queries put MySQL time hints after SELECT; page remains isolated from Usage',()=>{
  const go=readFileSync(new URL('../../monitor/newapi_usage_logs.go',import.meta.url),'utf8');
  assert.match(go,/usageLogsSelect\s*= "SELECT \/\*\+ MAX_EXECUTION_TIME\(18000\) \*\/ "/);
  const quotaGo=readFileSync(new URL('../../monitor/usage_logs_quota.go',import.meta.url),'utf8');
  // The two exact time-index fallback SELECTs share the same server budget.
  const queries=(go+'\n'+quotaGo).match(/(?:QueryContext|QueryRowContext)\(ctx, usageLogsSelect\+/g)||[];assert.equal(queries.length,10);
  const planGo=readFileSync(new URL('../../monitor/usage_logs_aggregate_probe.go',import.meta.url),'utf8');
  assert.match(planGo,/q := usageLogsSelect \+ "CASE WHEN/);
  assert.match(planGo,/usageLogsAggregateProbeSQL\(f, w, expression\)/);
  assert.doesNotMatch(planGo,/UNION ALL/,'retired UNION is test-only');
  assert.equal((quotaGo.match(/usageLogsProbeAggregate\(ctx, db,/g)||[]).length,2,'COUNT and SUM share the exact-or-plan protocol');
  assert.equal((go.match(/m\.queryUsageLogsCount\(ctx, db,/g)||[]).length,2,'both COUNT callers share the guarded SQL helper');
  assert.match(quotaGo,/query := usageLogsSelect \+ `CASE/);
  assert.match(go,/COUNT\(\*\) OVER\(\) AS page_matches FROM \(SELECT id AS page_id, created_at AS page_created_at/);
  assert.match(go,/COUNT\(\*\) OVER\(\) AS matched_total/);
  const html=readFileSync(new URL('../../monitor/page.html',import.meta.url),'utf8');assert.match(html,/else if\(window\.usageLogsDeactivate\)window\.usageLogsDeactivate\(\)/);
  const server=readFileSync(new URL('../../monitor/server.go',import.meta.url),'utf8');assert.match(server,/view := r.Group\("\/", m.requireRole\(roleAdmin\)\)/);
  // Replica-backed GET routes stay at five; shortcut routes only touch the local store.
  assert.equal((server.match(/view.GET\("\/usage-logs\/(?!shortcuts)/g)||[]).length,5);
  assert.equal((server.match(/view.(GET|POST)\("\/usage-logs\/shortcuts/g)||[]).length,5);
  const sc=readFileSync(new URL('../../monitor/usage_logs_shortcuts.go',import.meta.url),'utf8');
  assert.doesNotMatch(sc,/prodDB|usageLogsSource|QueryContext|ExecContext/);assert.match(sc,/m\.storeDB/);
  assert.doesNotMatch(server,/view.GET\("\/usage-logs\/export/);
  assert.doesNotMatch(source,/ulExport|runExport|csvCell/);
  // The only Exec is the KILL QUERY of this request's own replica thread.
  const execs=go.match(/ExecContext\([^\n]*/g)||[];assert.equal(execs.length,1);assert.match(execs[0],/^ExecContext\(ctx, fmt\.Sprintf\("KILL QUERY %d", threadID\)\)/);
  assert.doesNotMatch(go,/\b(?:INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|AutoMigrate)\b/i);
});
test('re-activating the tab never reuses a total counted before leaving it',()=>{
  assert.match(source,/async function activate\(\)\{[\s\S]*?st\.pageCursors\.clear\(\);st\.totalKey=null;st\.totalCached=null;/);
});
test('the eye toggle keeps stat messages instead of drawing endless placeholders',()=>{
  assert.match(source,/st\.masked=!st\.masked;renderEye\(\);if\(st\.stat\)renderStats\(\);renderTable\(\)/);
});
test('wide-range failures keep the rates and stay calculable; cancel offers the calculation',async()=>{
  let failRates=false;
  const v=fixture(url=>url.includes('skip_quota=1')?(failRates?new Response('{"success":false,"message":"busy"}'):ok({quota:null,quota_skipped:true,rpm:4,tpm:9})):new Response('{"success":false,"message":"timeout"}'));
  v.st.applied={startTime:1794400000000,endTime:1794400000000+72*3600*1000};
  await v.loadStat();await v.loadStat(true);
  assert.match(v.el('ulStats').innerHTML,/RPM <b>4/);assert.match(v.el('ulStats').innerHTML,/data-quota-calc/);assert.match(v.el('ulStats').innerHTML,/用量计算失败/);
  failRates=true;await v.loadStat();
  assert.match(v.el('ulStats').innerHTML,/RPM 暂不可用/);assert.match(v.el('ulStats').innerHTML,/data-quota-calc/);
  v.st.rates=null;v.st.stat=null;v.cancelQueries();assert.match(v.el('ulStats').innerHTML,/data-quota-calc/);
});
test('searching again with unchanged filters keeps a clicked wide-range SUM running',async()=>{
  const v=fixture((url,o)=>url.includes('skip_quota=1')?ok({quota:null,quota_skipped:true,rpm:1,tpm:2}):new Promise(()=>{}));
  v.st.applied={startTime:1794400000000,endTime:1794400000000+72*3600*1000};
  await v.loadStat();v.loadStat(true);const sum=v.calls.at(-1);
  v.prepareSearch();
  assert.equal(sum.options.signal.aborted,false);assert.equal(v.st.statRequested,false);
});

const scItems=[{id:1,username:'yiche-api',label:'易车',sort:1},{id:2,username:'gaotu',label:'gaotu',sort:2}];
const scTarget=(sel,data={})=>({closest:q=>q.split(',').some(x=>x.trim()===sel)?{dataset:data,hasAttribute:a=>`[${a}]`===sel,disabled:false,textContent:'',classList:{add(){},remove(){}},closest:()=>null}:null});
function scFixture(extra){
  const v=fixture((url,o)=>{
    if(String(url).startsWith('/usage-logs/shortcuts'))return extra?extra(url,o):ok({items:scItems,max:20});
    return ok({items:[],total:0,page_size:100});
  });
  v.st.inited=true;v.st.draft={startTime:1794400000000,endTime:1794500000000,type:'0',model:'',token:'',channel:'',group:'',username:'',requestId:'',upstreamRequestId:''};
  return v;
}

test('shortcut chips load once per admin, mark the active user and respect masking',async()=>{
  const v=scFixture();
  await v.loadShortcuts();
  const h=v.el('ulShortcuts').innerHTML;
  assert.match(h,/data-sc="1"[^>]*>易车</);assert.match(h,/data-sc="2"[^>]*>gaotu</);assert.match(h,/data-sc-add/);assert.match(h,/data-sc-manage/);
  assert.doesNotMatch(h,/ul-sc-chip active/);
  v.st.draft.username='gaotu';v.renderShortcuts();
  assert.match(v.el('ulShortcuts').innerHTML,/class="ul-sc-chip active" data-sc="2" aria-pressed="true"/);
  v.st.masked=true;v.renderShortcuts();
  const m=v.el('ulShortcuts').innerHTML;
  assert.match(m,/>易车</);assert.match(m,/data-sc="2"[^>]*>••••</);assert.doesNotMatch(m,/yiche-api|用户名：gaotu/);
});

test('clicking a shortcut searches that user; clicking the active one clears the user filter',async()=>{
  const v=scFixture();await v.loadShortcuts();
  v.onShortcutBarClick({target:scTarget('[data-sc]',{sc:'1'})});
  assert.equal(v.el('ulUser').value,'yiche-api');assert.equal(v.st.draft.username,'yiche-api');
  await new Promise(r=>setImmediate(r));
  const list=v.calls.map(c=>new URL(c.url,'http://localhost')).find(u=>u.pathname==='/usage-logs/list');
  assert.ok(list,'search issued a list query');assert.equal(list.searchParams.get('username'),'yiche-api');
  v.onShortcutBarClick({target:scTarget('[data-sc]',{sc:'1'})});
  assert.equal(v.el('ulUser').value,'');assert.equal(v.st.draft.username,'');
});

test('shortcut load failure shows a retry instead of chips',async()=>{
  const v=scFixture(()=>new Response('{"success":false,"message":"boom"}'));
  await v.loadShortcuts();
  assert.match(v.el('ulShortcuts').innerHTML,/加载失败/);assert.match(v.el('ulShortcuts').innerHTML,/data-sc-retry/);
});

test('saving a shortcut posts JSON and surfaces server validation errors',async()=>{
  let fail=true;const posts=[];
  const v=scFixture((url,o)=>{if(o.method==='POST'){posts.push({url,o});return fail?new Response(JSON.stringify({success:false,message:'该用户已在快捷标签中'})):ok({items:scItems,max:20})}return ok({items:[],max:20})});
  v.openModal('t','','');v.el('ulScUser').value=' yiche-api ';v.el('ulScLabel').value='易车';v.el('ulScErr').hidden=true;
  await v.saveShortcut(0,false);
  assert.equal(posts.length,1);assert.equal(posts[0].url,'/usage-logs/shortcuts');assert.equal(posts[0].o.headers['Content-Type'],'application/json');
  assert.deepEqual(JSON.parse(posts[0].o.body),{username:'yiche-api',label:'易车'});
  assert.equal(v.el('ulScErr').hidden,false);assert.match(v.el('ulScErr').textContent,/已在快捷标签中/);assert.equal(v.el('ulModal').hidden,false);
  fail=false;v.el('ulModalFoot:child').disabled=false;
  await v.saveShortcut(0,false);
  assert.equal(v.el('ulModal').hidden,true);assert.equal(v.st.shortcuts.length,2);
  v.openModal('t','','');v.el('ulModalFoot:child').disabled=false;v.el('ulScUser').value='gao%';v.el('ulScErr').hidden=true;
  await v.saveShortcut(0,false);assert.equal(posts.length,2,'wildcard rejected client-side');assert.match(v.el('ulScErr').textContent,/通配符/);
});

test('deleting a shortcut needs a second confirming click',async()=>{
  const posts=[];
  const v=scFixture((url,o)=>{if(o.method==='POST'){posts.push(url);return ok({items:[scItems[1]],max:20})}return ok({items:scItems,max:20})});
  await v.loadShortcuts();v.openShortcutManage();
  const btn={dataset:{},disabled:false,textContent:'删除',classList:{add(){},remove(){}},hasAttribute:a=>a==='data-sc-del',closest:q=>q==='.ul-sc-row'?{dataset:{id:'1'}}:null};
  const ev={target:{closest:()=>btn}};
  v.onShortcutModalClick(ev);assert.equal(posts.length,0);assert.equal(btn.textContent,'确认删除');
  v.onShortcutModalClick(ev);await new Promise(r=>setImmediate(r));
  assert.deepEqual(posts,['/usage-logs/shortcuts/1/delete']);assert.equal(v.st.shortcuts.length,1);
});
