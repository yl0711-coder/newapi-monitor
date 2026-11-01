import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const html=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
function fixture({token=false,month='09'}={}){
  const nodes=new Map(),requests=[];
  const $=id=>{if(!nodes.has(id))nodes.set(id,{style:{},textContent:'',innerHTML:''});return nodes.get(id);};
  $('detail').innerHTML='retained August detail';
  const no=()=>{};
  const ctx=vm.createContext({$,AbortController,document:{querySelectorAll:()=>[]},
    rangeDates:()=>({from:`2026-${month}-01`,to:`2026-${month}-30`}),
    api:(url,options)=>new Promise(resolve=>requests.push({url,options,resolve})),
    syncOvBar:no,restoreLastLoadedRange:no,renderStat:no,renderMatrix:no,renderTrend:no,renderMemberBar:no,
    renderBreakdownUnavailable:no,loadBreakdown:no,fillMemberSelect:no,loadLogs:no,resizeLogDetails:no,loadMonthlyList:no,
    renderDim:no,getCh:()=>null,renderModelPie:no,emptyChart:no,renderTokens:no,drawDailyChart:no,
    fmtCost:n=>String(n),fmtNum:n=>String(n),fmtCompact:n=>String(n),esc:n=>String(n),
  });
  vm.runInContext(`let overviewSeq=0,overviewAbort=null,detailSeq=0,detailAbort=null,breakdownSeq=0,breakdownAbort=null;
    let logAbort=null,logSeq=0,logLoading=false;
    let range='custom',customFrom='2026-${month}-01',customTo='2026-${month}-30',lastLoadedRange={},ov={users:[]},mxPage=1;
    let detailUid=43,detailTok=${token?'{tid:9,name:"token"}':'null'},detailRangeKey='2026-08-01|2026-08-30',dModelPieCh=null;
    let view='logs',logsLoaded=true,logDirty=false,monthlyAbort=null,monthlyDownloadAbort=null,monthlySequence=0,monthlyListCursor='';`,ctx);
  for(const name of ['cancelPortalDependentLoads','portalDetailRangeKey','hideStalePortalDetail','loadDetail','loadTokenDetail','load','showView','backToList']){
    vm.runInContext(html.match(new RegExp(`^(?:async )?function ${name}\\([^]*?^}`,'m'))[0],ctx);
  }
  const tick=()=>new Promise(resolve=>setImmediate(resolve));
  return {ctx,$,requests,tick};
}

for(const token of [false,true])test(`date switch while viewing logs reloads ${token?'token':'member'} detail on return`,async()=>{
  const {ctx,$,requests,tick}=fixture({token});
  const loading=vm.runInContext('load()',ctx);
  requests[0].resolve({ok:true,data:{from:'2026-09-01',to:'2026-09-30',users:[]}});
  await loading;
  assert.equal(requests.length,1,'hidden member detail is not queried eagerly');
  vm.runInContext('showView("overview")',ctx);
  assert.equal($('detail').style.display,'none','old-period money is not exposed under the new date header');
  assert.match(requests[1].url,/from=2026-09-01&to=2026-09-30/);
  assert.equal(requests[1].url.includes('token_id=9'),token);
  requests[1].resolve({ok:true,data:{stats:{summary:{cost_usd:9}},token:{name:'token'}}});
  await tick();
  assert.equal($('detail').style.display,'block');
  assert.equal(vm.runInContext('detailRangeKey',ctx),'2026-09-01|2026-09-30');
});

test('unchanged date returns to retained detail without an extra request',()=>{
  const {ctx,$,requests}=fixture({month:'08'});
  vm.runInContext('showView("overview")',ctx);
  assert.equal($('detail').style.display,'block');
  assert.equal(requests.length,0);
});

test('failed refresh leaves the valid overview visible and retry remains possible',async()=>{
  const {ctx,$,requests,tick}=fixture();
  vm.runInContext('showView("overview")',ctx);
  requests[0].resolve({ok:false,error:'temporary failure'});await tick();
  assert.equal($('detail').style.display,'none');
  assert.equal($('listView').style.display,'');
  assert.equal($('ovErr').textContent,'temporary failure');
  vm.runInContext('showView("logs");showView("overview")',ctx);
  assert.equal(requests.length,2);
  requests[1].resolve({ok:true,data:{stats:{summary:{}}}});await tick();
  assert.equal($('detail').style.display,'block');
  assert.equal($('ovErr').textContent,'');
});

test('a pending detail response cannot reopen a panel after navigating to monthly bills',async()=>{
  const {ctx,$,requests,tick}=fixture();
  vm.runInContext('showView("overview");showView("monthly")',ctx);
  assert.equal(requests[0].options.signal.aborted,true);
  requests[0].resolve({ok:true,data:{stats:{summary:{}}}});await tick();
  assert.equal($('detail').style.display,'none');
  assert.equal($('monthlyView').style.display,'block');
  assert.equal(vm.runInContext('detailRangeKey',ctx),'2026-08-01|2026-08-30');
});

test('a date refresh started inside overview also hides old-period amounts',async()=>{
  const {ctx,$,requests}=fixture();
  vm.runInContext('view="overview"',ctx);
  const loading=vm.runInContext('load()',ctx);
  requests[0].resolve({ok:true,data:{from:'2026-09-01',to:'2026-09-30',users:[]}});await loading;
  assert.equal($('detail').style.display,'none');
  assert.equal(requests.length,2);
});
