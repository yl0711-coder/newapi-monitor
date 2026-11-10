import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

function alertsView(responses) {
  const source=readFileSync(new URL('../../monitor/alerts.js',import.meta.url),'utf8');
  const end=source.lastIndexOf('})();');
  assert.ok(end>0);
  const elements=new Map(),requests=[],timers=new Map(),listeners=new Map();let timerID=0;
  const on=(name,fn)=>listeners.set(name,[...(listeners.get(name)||[]),fn]);
  const element=id=>{
    if(!elements.has(id))elements.set(id,{textContent:'',innerHTML:'',hidden:false,value:'',disabled:false,addEventListener(){}});
    return elements.get(id);
  };
  const context=vm.createContext({window:{addEventListener:on},document:{getElementById:element,addEventListener:on,hidden:false},URLSearchParams,AbortController,
    setTimeout(fn){const id=++timerID;timers.set(id,fn);return id},clearTimeout(id){timers.delete(id)},
    fetch:async url=>{
      requests.push(new URL(url,'http://local.test'));
      // Match the endpoint's minute-boundary validation so invalid cutoff
      // requests fail as HTTP 400 instead of being accepted by the fixture.
      const cutoff=requests.at(-1).searchParams.get('cutoff_ts');
      if(cutoff!==null&&(!Number.isSafeInteger(+cutoff)||+cutoff<=0||+cutoff%60!==0)){
        return {ok:false,status:400,text:async()=>JSON.stringify({error:'cutoff_ts 必须为可信的整分钟截止时间'})};
      }
      const data=typeof responses==='function'?responses(requests.at(-1)):responses[Math.min(requests.length-1,responses.length-1)];
      return {ok:true,status:200,text:async()=>JSON.stringify(data)};
    }});
  vm.runInContext(readFileSync(new URL('../../monitor/view_refresh.js',import.meta.url),'utf8'),context);
  context.window.MonitorView.preserve=(_root,update)=>update();
  vm.runInContext(source.slice(0,end)+`
    globalThis.alertsTest={load,go,paint,resumeLive,setFilters(reason,user){fReason=reason;fUser=user;},setDate(value){date=value;}};
  `+source.slice(end),context);
  context.alertsTest.setDate('2026-10-08');
  return {ui:context.alertsTest,element,requests,activate:context.window.alertsActivate,deactivate:context.window.alertsDeactivate,
    async tick(){const [id,fn]=timers.entries().next().value;timers.delete(id);fn();await new Promise(setImmediate)},
    async focus(){for(const fn of listeners.get('focus')||[])fn();await new Promise(setImmediate)}};
}

const through=Date.parse('2026-10-08T15:56:00+08:00')/1000;
const today={enabled:true,total:7,row_total:1,rows:[{ts:through-60,reason:'route_no_channel',count:7,user_id:0}],
  generated_at:through+180,lag_seconds:0,timing:{finalize_delay_seconds:120,alignment_seconds:60,collection_poll_seconds:60},
  coverage_complete:false,statistics_complete:true,statistics_available:true,tail_syncing:true,
  from_ts:Date.parse('2026-10-08T00:00:00+08:00')/1000,to_ts:through,cutoff_ts:through,through_ts:through,target_ts:through,source:'cloudwatch'};

test('alerts shows seven known rejections while only the unfinalized tail is pending',async()=>{
  const {ui,element}=alertsView([today]);await ui.load();
  assert.match(element('alCounter').innerHTML,/被拒次数 <b>7<\/b>/);
  assert.match(element('alCounter').innerHTML,/截至 10-08 15:56，尾部同步中/);
  assert.doesNotMatch(element('alCounter').innerHTML,/未完成|—/);
  assert.equal(element('alCoverageWarning').hidden,true);
  assert.match(element('alStatus').textContent,/封口等待 120 秒/);
  assert.doesNotMatch(element('alCoverageWarning').textContent,/数据不完整|混合|旁路/);
  assert.match(element('alNotes').textContent,/10-08 00:00 至 10-08 15:56/);
});

test('alerts one-minute lag keeps the usable prefix instead of hiding counts',async()=>{
  const {ui,element}=alertsView([{...today,to_ts:through-60,through_ts:through-60}]);await ui.load();
  assert.match(element('alCounter').innerHTML,/15:55，尾部同步中/);
  assert.match(element('alCounter').innerHTML,/被拒次数 <b>7<\/b>/);
  assert.doesNotMatch(element('alCounter').innerHTML,/未完成/);
});

test('alerts historical complete zero is a known zero and has no pending-tail message',async()=>{
  const {ui,element}=alertsView([{...today,total:0,row_total:0,rows:[],coverage_complete:true,tail_syncing:false}]);await ui.load();
  assert.equal(element('alCoverageWarning').hidden,true);
  assert.match(element('alCounter').innerHTML,/被拒次数 <b>0<\/b>/);
  assert.doesNotMatch(element('alCounter').innerHTML,/同步中|已采集部分|未完成/);
  assert.match(element('alTableBody').innerHTML,/当前统计范围内无记录/);
});

test('alerts real gap or mixed source retains numeric partial counts and a prominent warning',async()=>{
  for(const source of ['cloudwatch','mixed']){
    const {ui,element}=alertsView([{...today,statistics_complete:false,source,coverage_note:'连续覆盖存在缺口。'}]);await ui.load();
    assert.equal(element('alCoverageWarning').hidden,false);
    assert.match(element('alCoverageWarning').textContent,/数据不完整/);
    assert.match(element('alCounter').innerHTML,/已采集部分/);
    assert.match(element('alCounter').innerHTML,/被拒次数 <b>7<\/b>/);
    assert.doesNotMatch(element('alCounter').innerHTML,/尾部同步中|未完成/);
    assert.equal(element('alCoverageWarning').textContent.includes('旁路采集器'),source==='mixed');
  }
});

test('alerts ongoing collection failure cannot be hidden by an available prefix',async()=>{
  const {ui,element}=alertsView([{...today,collection_failed:true,coverage_note:'最近一次采集失败。'}]);await ui.load();
  assert.match(element('alCoverageWarning').textContent,/采集失败/);
  assert.match(element('alCounter').innerHTML,/被拒次数 <b>7<\/b>/);
});

test('alerts normal tail stays neutral, but backlog and historical gaps warn across refreshes',async()=>{
 const {ui,element}=alertsView([
  {...today,lag_seconds:60},
  {...today,lag_seconds:180},
  {...today,lag_seconds:0,statistics_complete:false},
  {...today,lag_seconds:0,coverage_complete:true,tail_syncing:false}
 ]);
 await ui.load();assert.equal(element('alCoverageWarning').hidden,true);assert.match(element('alStatus').textContent,/正常等待/);
 await ui.load(false,true);assert.equal(element('alCoverageWarning').hidden,false);assert.match(element('alCoverageWarning').textContent,/采集积压 180 秒/);assert.match(element('alCoverageWarning').textContent,/统计窗口覆盖完整/);
 ui.setDate('2026-10-07');await ui.load();assert.match(element('alCoverageWarning').textContent,/数据不完整/);
 await ui.load(false,true);assert.equal(element('alCoverageWarning').hidden,true);
});

test('alerts first collection failure is visible even without a usable prefix',async()=>{
  const {ui,element}=alertsView([{...today,statistics_complete:false,tail_syncing:false,collection_failed:true,
    coverage_note:'前置拒绝采集最近一次失败；仅为已采集部分。'}]);await ui.load();
  assert.match(element('alCoverageWarning').textContent,/数据不完整.*采集失败/);
  assert.match(element('alCounter').innerHTML,/已采集部分.*被拒次数 <b>7<\/b>/);
  assert.doesNotMatch(element('alCoverageWarning').textContent,/已连续覆盖的时间范围/);
});

test('alerts old API partial counts are shown only as collected observations',async()=>{
  const {ui,element}=alertsView([{enabled:true,total:7,row_total:1,rows:today.rows,source:'collector'}]);await ui.load();
  assert.match(element('alCounter').innerHTML,/已采集部分/);
  assert.match(element('alCounter').innerHTML,/被拒次数 <b>7<\/b>/);
  assert.match(element('alCoverageWarning').textContent,/数据不完整/);
});

test('alerts filters pin the returned actual window and changing the date releases it',async()=>{
  const {ui,requests}=alertsView([today]);await ui.load();
  ui.setFilters('route_no_channel','0');await ui.load();
  assert.equal(requests[0].searchParams.has('cutoff_ts'),false);
  assert.equal(requests[1].searchParams.get('cutoff_ts'),String(through));
  assert.equal(requests[1].searchParams.get('reason'),'route_no_channel');
  assert.equal(requests[1].searchParams.get('user_id'),'0');
  ui.go('2026-10-07');
  assert.equal(requests[2].searchParams.has('cutoff_ts'),false);
});

test('alerts unready first page can keep paging and filtering after recovery without inventing a second-level cutoff',async()=>{
  const snapshotTo=through+160; // 15:58:40: valid in the opaque cursor, not as cutoff_ts.
  const recoveredThrough=through+240;
  const first={...today,statistics_complete:false,through_ts:0,to_ts:snapshotTo,cutoff_ts:undefined,
    has_more:true,next_cursor:'initial-second-level-window'};
  const recovered={...first,statistics_complete:true,through_ts:recoveredThrough,target_ts:recoveredThrough,
    next_cursor:'same-second-level-window-next-page'};
  const filtered={...today,to_ts:recoveredThrough,cutoff_ts:recoveredThrough,through_ts:recoveredThrough,target_ts:recoveredThrough};
  const {ui,requests,element}=alertsView([first,recovered,{...recovered,has_more:false},filtered,filtered]);
  await ui.load();
  await ui.load(true); // Coverage recovers, but pagination keeps the original seconds-based window.
  assert.equal(requests[1].searchParams.get('cursor'),'initial-second-level-window');
  await ui.load(true);
  assert.equal(requests[2].searchParams.get('cursor'),'same-second-level-window-next-page');
  ui.setFilters('route_no_channel','0');await ui.load();
  for(const request of requests.slice(0,4)){
    assert.equal(request.searchParams.has('cutoff_ts'),false,'only a server-approved cutoff may be submitted');
  }
  assert.equal(requests[3].searchParams.get('reason'),'route_no_channel');
  assert.equal(requests[3].searchParams.get('user_id'),'0');
  assert.equal(requests[3].searchParams.has('cursor'),false);
  // The new filtered response explicitly approves a minute boundary. Only now may we reuse it.
  ui.setFilters('','0');await ui.load();
  assert.equal(requests.length,5);
  assert.equal(requests[4].searchParams.get('cutoff_ts'),String(recoveredThrough));
  assert.doesNotMatch(element('alStatus').textContent,/读取失败/);
});

test('alerts complete responses without cutoff_ts never infer it from to_ts',async()=>{
  for(const end of [through,through+40]){
    const {ui,requests}=alertsView([{...today,coverage_complete:true,to_ts:end,cutoff_ts:undefined}]);
    await ui.load();ui.setFilters('route_no_channel','0');await ui.load();
    assert.equal(requests[1].searchParams.has('cutoff_ts'),false);
  }
});

test('alerts clears a previously approved cutoff when the server no longer returns it',async()=>{
  const {ui,requests}=alertsView([today,{...today,statistics_complete:false,cutoff_ts:undefined},today]);
  await ui.load();ui.setFilters('route_no_channel','0');await ui.load();
  assert.equal(requests[1].searchParams.get('cutoff_ts'),String(through));
  ui.setFilters('','0');await ui.load();
  assert.equal(requests[2].searchParams.has('cutoff_ts'),false);
});

test('alerts mixed-source counts retain a server-approved cutoff across filtering',async()=>{
  const {ui,requests,element}=alertsView([{...today,statistics_complete:false,source:'mixed',cutoff_ts:through}]);
  await ui.load();ui.setFilters('route_no_channel','0');await ui.load();
  assert.equal(requests[1].searchParams.get('cutoff_ts'),String(through));
  assert.match(element('alCounter').innerHTML,/已采集部分/);
});

test('alerts a fixed historical snapshot is not described as an actively syncing tail',async()=>{
  const {ui,element}=alertsView([{...today,tail_syncing:false}]);await ui.load();
  assert.match(element('alCounter').innerHTML,/固定统计窗口/);
  assert.doesNotMatch(element('alCoverageWarning').textContent,/尾部同步中|数据不完整/);
});

test('alerts timer, focus and page return advance the source window while preserving date and filters',async()=>{
  let sourceThrough=through;
  const v=alertsView(request=>{
    const end=Number(request.searchParams.get('cutoff_ts'))||sourceThrough;
    return {...today,to_ts:end,cutoff_ts:end,through_ts:end,target_ts:sourceThrough,total:end-through+7};
  });
  v.ui.setFilters('route_no_channel','0');v.activate();await new Promise(setImmediate);
  assert.match(v.element('alCounter').innerHTML,/被拒次数 <b>7<\/b>/);
  for(const refresh of [()=>v.tick(),()=>v.focus(),async()=>{v.deactivate();v.activate();await new Promise(setImmediate)}]){
    sourceThrough+=300;await refresh();
    const query=v.requests.at(-1).searchParams;
    assert.equal(query.has('cutoff_ts'),false,'live refresh must release the previous snapshot cutoff');
    assert.equal(query.get('from'),'2026-10-08');assert.equal(query.get('to'),'2026-10-08');
    assert.equal(query.get('reason'),'route_no_channel');assert.equal(query.get('user_id'),'0');
    assert.match(v.element('alCounter').innerHTML,new RegExp('被拒次数 <b>'+(sourceThrough-through+7)+'</b>'));
  }
});

test('alerts pagination retains its snapshot across automatic refresh until explicitly returning to latest',async()=>{
  let sourceThrough=through;
  const v=alertsView(request=>{
    const pinned=request.searchParams.has('cursor')||request.searchParams.has('cutoff_ts');
    const end=pinned?through:sourceThrough;
    return {...today,to_ts:end,cutoff_ts:end,through_ts:end,has_more:true,next_cursor:'page-snapshot',total:end-through+7};
  });
  v.activate();await new Promise(setImmediate);sourceThrough+=300;
  await v.ui.load(true);
  assert.equal(v.requests.at(-1).searchParams.get('cursor'),'page-snapshot');
  assert.equal(v.requests.at(-1).searchParams.get('cutoff_ts'),String(through));
  await v.tick();
  assert.equal(v.requests.length,2,'automatic refresh must not discard paginated rows');
  await v.ui.resumeLive();
  assert.equal(v.requests.at(-1).searchParams.has('cursor'),false);
  assert.equal(v.requests.at(-1).searchParams.has('cutoff_ts'),false);
  assert.match(v.element('alCounter').innerHTML,/被拒次数 <b>307<\/b>/);
});
