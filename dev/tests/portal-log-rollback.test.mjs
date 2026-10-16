import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
function install(ctx,name,async=false){
  const source=page.match(new RegExp(`^${async?'async ':''}function ${name}\\([^]*?^}`,'m'));
  assert.ok(source,`${name} missing`);
  vm.runInContext(source[0],ctx);
}
function harness(){
  const elements=new Map(),rendered=[],downloads=[],failures=[];
  const ctx=vm.createContext({
    AbortController,URLSearchParams,esc:String,
    logStack:[0,42],logTotal:99,logSeq:0,logAbort:null,logLoading:false,
    logRowsCache:[{type:6,cost_usd:0.0002}],logHasMore:true,logNextCursor:43,
    exporting:false,expHeaders:false,expCtrl:null,expTimer:null,pendingExportTicket:'stale-ticket',billExportClient:null,
    $(id){if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',style:{},disabled:false});return elements.get(id);},
    logQuery:()=> 'from=2026-10-14&to=2026-10-14&type=7',
    hideLogRawTip(){},renderLogRows(rows){rendered.push(rows);},renderLogPager(){},
    setInterval:()=>1,clearInterval(){},setExpBar(){},expStatus(){},closeExp(){},askExportConfirm(){},updateBillResumeButton(){},
    beginNativeExport(ticket,confirm){downloads.push({ticket,confirm});},
    expFail(message){failures.push(message);},
  });
  install(ctx,'logContractError');install(ctx,'loadLogs',true);install(ctx,'startExport',true);
  return {ctx,elements,rendered,downloads,failures};
}

test('new page rejects actual legacy response semantics instead of showing false refund zero/login results',async()=>{
  const h=harness();
  // v1.18.26 ignores type=7 and sets refund cost_usd to zero; it has no contract marker.
  const old={ok:true,rows:[{type:6,cost_usd:0},{type:2,cost_usd:0.0002}],has_more:true,next_cursor:'44'};
  const before=JSON.stringify(old);
  h.ctx.api=async()=>old;
  await vm.runInContext('loadLogs(false)',h.ctx);
  assert.equal(h.rendered.length,0);
  assert.equal(h.ctx.logRowsCache.length,0);
  assert.equal(h.ctx.logHasMore,false);assert.equal(h.ctx.logNextCursor,0);
  assert.equal(h.ctx.logStack.length,1);assert.equal(h.ctx.logTotal,null);
  assert.match(h.elements.get('logTb').innerHTML,/服务版本不一致，请刷新页面/);
  assert.doesNotMatch(h.elements.get('logTb').innerHTML,/\$0\.000000|暂无日志/);
  assert.equal(h.elements.get('logPager').innerHTML,'');assert.equal(h.ctx.logLoading,false);
  assert.equal(JSON.stringify(old),before);
});

test('empty/unknown legacy protocol cannot masquerade as a supported empty login result',async()=>{
  for(const marker of [undefined,null,'1',0,2]){
    const h=harness();h.ctx.api=async()=>({ok:true,log_contract:marker,rows:[]});
    await vm.runInContext('loadLogs(true)',h.ctx);
    assert.equal(h.rendered.length,0);
    assert.match(h.elements.get('logTb').innerHTML,/请刷新页面/);
  }
});

test('supported protocol preserves true-zero, null facts, refunds, login and empty result rows',async()=>{
  const rows=[
    {type:2,cost_usd:0,source_facts:{quota_present:true}},
    {type:6,cost_usd:0.0002,source_facts:{quota_present:true}},
    {type:2,cost_usd:0,source_facts:{quota_present:false}},
    {type:7,cost_usd:0},
  ];
  for(const data of [rows,[]]){
    const h=harness(),before=JSON.stringify(data);
    h.ctx.api=async()=>({ok:true,log_contract:1,rows:data,has_more:false,next_cursor:'0'});
    await vm.runInContext('loadLogs(true)',h.ctx);
    assert.equal(h.rendered.length,1);assert.equal(h.rendered[0],data);
    assert.equal(JSON.stringify(data),before);
    assert.doesNotMatch(h.elements.get('logTb').innerHTML,/服务版本不一致/);
  }
});

test('stale incompatible reply does not erase a newer successful request',async()=>{
  const h=harness();let finishOld;
  h.ctx.api=()=>new Promise(resolve=>{finishOld=resolve;});
  const old=vm.runInContext('loadLogs(true)',h.ctx);
  const rows=[{type:7}];h.ctx.api=async()=>({ok:true,log_contract:1,rows});
  await vm.runInContext('loadLogs(true)',h.ctx);
  finishOld({ok:true,rows:[{type:6,cost_usd:0}]});await old;
  assert.equal(h.rendered.length,1);assert.equal(h.rendered[0],rows);
  assert.doesNotMatch(h.elements.get('logTb').innerHTML,/服务版本不一致/);
});

test('old export prepare cannot start a download or preserve a stale confirmation ticket',async()=>{
  for(const need_confirm of [false,true]){
    const h=harness();
    h.ctx.fetch=async()=>({ok:true,json:async()=>({ok:true,ticket:'legacy-ticket',need_confirm})});
    await vm.runInContext('startExport()',h.ctx);
    assert.equal(h.downloads.length,0);assert.equal(h.ctx.pendingExportTicket,'');
    assert.match(h.failures[0],/服务版本不一致，请刷新页面/);
    assert.equal(h.ctx.exporting,false);assert.equal(h.elements.get('expBtn').disabled,false);
  }
});

test('supported export prepare still supports direct and capped-confirmation downloads',async()=>{
  for(const need_confirm of [false,true]){
    const h=harness();
    h.ctx.fetch=async()=>({ok:true,json:async()=>({ok:true,log_contract:1,ticket:'signed-ticket',need_confirm,cap:50000,total:60000})});
    await vm.runInContext('startExport()',h.ctx);
    assert.equal(h.failures.length,0);
    if(need_confirm){assert.equal(h.downloads.length,0);assert.equal(h.ctx.pendingExportTicket,'signed-ticket');}
    else assert.deepEqual(h.downloads,[{ticket:'signed-ticket',confirm:false}]);
  }
});

test('native download uses a versioned path absent from the old server, including delayed confirmation',()=>{
  const h=harness(),links=[];
  h.ctx.document={body:{appendChild(){}},createElement(){const a={click(){links.push(this.href);},remove(){}};return a;}};
  h.ctx.setTimeout=()=>1;
  install(h.ctx,'beginNativeExport');
  vm.runInContext('beginNativeExport("signed+ticket",false)',h.ctx);
  vm.runInContext('beginNativeExport("signed+ticket",true)',h.ctx);
  assert.deepEqual(links,['/api/logs/export/v1?ticket=signed%2Bticket','/api/logs/export/v1?ticket=signed%2Bticket&confirm=1']);
  // No automatic fallback to the legacy route: a rollback between prepare and download returns 404, not a different CSV.
  assert.doesNotMatch(page.match(/^function beginNativeExport\([^]*?^}/m)[0],/href='\/api\/logs\/export\?'/);
});

test('bill download requires bill protocol and never truncates or falls back',async()=>{
  for(const mode of ['ok','legacy','cap','missing-ticket','rejected']){
    const h=harness(),links=[];
    h.ctx.document={body:{appendChild(){}},createElement(){return {click(){links.push(this.href);},remove(){}};}};
    h.ctx.fetch=async url=>{
      assert.match(url,/format=bill-v1/);
      return {ok:mode!=='rejected',json:async()=>({ok:mode!=='rejected',error:'缩小时间范围',log_contract:1,format:mode==='legacy'?'':'bill-v1',ticket:mode==='missing-ticket'?'':'bill-ticket',need_confirm:mode==='cap'})};
    };
    await vm.runInContext('startExport("bill-v1")',h.ctx);
    if(mode==='ok')assert.deepEqual(links,['/api/logs/bill/v1?ticket=bill-ticket']);
    else {assert.equal(links.length,0);assert.equal(h.failures.length,1);}
    assert.equal(h.elements.get('billBtn').disabled,false);
    assert.equal(h.elements.get('expBtn').disabled,false);
    assert.equal(h.ctx.exporting,false);
    assert.equal(h.ctx.pendingExportTicket,'');
  }
});

test('task contract explicitly selects async bill only, unknown versions fail closed',async()=>{
  for(const contract of [1,2,null,'1']){
    const h=harness(),tasks=[],links=[];
    h.ctx.runBillExport=async ticket=>{tasks.push(ticket);};
    h.ctx.document={body:{appendChild(){}},createElement(){return {click(){links.push(this.href);},remove(){}};}};
    h.ctx.fetch=async()=>({ok:true,json:async()=>({ok:true,log_contract:1,format:'bill-v1',ticket:'ticket',bill_task_contract:contract})});
    await vm.runInContext('startExport("bill-v1")',h.ctx);
    assert.equal(links.length,0);
    if(contract===1){assert.deepEqual(tasks,['ticket']);assert.equal(h.failures.length,0);}
    else{assert.equal(tasks.length,0);assert.match(h.failures[0],/不兼容/);}
  }
});
