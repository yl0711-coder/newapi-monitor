import test from 'node:test';
import assert from 'node:assert/strict';
import {readdirSync,readFileSync} from 'node:fs';
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
  vm.runInContext(source.replace('window.usageLogsActivate=activate;',`globalThis.api={st,activate,deactivate,search,prepareSearch,loadMeta,loadList,loadCount,loadStat,cancelQueries,openUser,openDetail,openModal,closeModal,cellCost,queryParams,fmtQuota,detailSegments,loadShortcuts,renderShortcuts,onShortcutBarClick,onShortcutModalClick,saveShortcut,openShortcutManage,scApply};window.usageLogsActivate=activate;`),context);
  const api=context.api;api.st.meta={...meta};api.st.applied={startTime:1794400000000,endTime:1794500000000};api.st.pageSize=100;
  return{...api,el,calls,timers};
}

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
  const v=fixture(()=>++count===1?waiting.promise:ok({quota:200}));
  const first=v.loadStat();
  Object.assign(v.st.applied,{type:'5',requestId:'r1',upstreamRequestId:'up-r1'});
  await v.loadStat();assert.equal(v.calls.length,1);assert.equal(v.calls[0].options.signal.aborted,false);
  const url=new URL(v.calls[0].url,'http://localhost');
  for(const key of['type','request_id','upstream_request_id'])assert.equal(url.searchParams.has(key),false);
  waiting.resolve(ok({quota:100}));await first;assert.equal(v.st.stat.quota,100);
  await v.loadStat();assert.equal(v.calls.length,2);assert.equal(v.st.stat.quota,200);
});

test('a changed statistics scope cancels the old sum and cannot inherit its result',async()=>{
  const waiting=deferred();let count=0;
  const v=fixture(()=>++count===1?waiting.promise:ok({quota:200}));
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
test('a slow exact count does not block visible records; statistics start after the count',async()=>{
  const counting=deferred();
  const v=fixture(url=>url.includes('/count?')?counting.promise:ok(url.includes('/stat?')?{quota:77}:{items:[row],total:null,total_pending:true,has_more:true,page_size:100}));
  v.st.statRequested=true;await v.loadList();
  assert.equal(v.st.items[0].id,1);assert.equal(v.st.totalKnown,false);assert.equal(v.el('ulSearch').disabled,false);
  assert.match(v.el('ulPager').innerHTML,/计算中/);assert.ok(!v.calls.some(c=>c.url.includes('/stat?')));
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
  const v=fixture(url=>url.includes('/stat?')?(++statCalls,statistic.promise):url.includes('/count?')?counting.promise:ok({items:[row],total_pending:true,total:null,has_more:true,page_size:100}));
  const first=v.loadStat();v.st.applied.type='5';v.prepareSearch();await v.loadList();
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
  const adapter=new URL('../../monitor/newapi_usage_logs.go',import.meta.url);
  const pkg=new URL('../../internal/usagelogs/',import.meta.url);
  const go=[adapter,...readdirSync(pkg)
    .filter(name=>name.endsWith('.go')&&!name.endsWith('_test.go'))
    .map(name=>new URL(name,pkg))].map(file=>readFileSync(file,'utf8')).join('\n');
  assert.match(go,/usageLogsSelect\s*= "SELECT \/\*\+ MAX_EXECUTION_TIME\(18000\) \*\/ "/);
  // The package-wide scan also covers the indexed time-window query previously in usage_logs_plan.go.
  const queries=go.match(/(?:QueryContext|QueryRowContext)\(ctx, usageLogsSelect\+/g)||[];assert.equal(queries.length,10);
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
  assert.match(v.el('ulStats').innerHTML,/统计加载失败/);assert.match(v.el('ulStats').innerHTML,/data-quota-calc/);
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
