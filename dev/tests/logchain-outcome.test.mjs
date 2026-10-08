import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

function requestView() {
  const source=readFileSync(new URL('../../monitor/logchain.js',import.meta.url),'utf8');
  const end=source.lastIndexOf('})();');
  assert.ok(end>0,'production logchain module closure exists');
  const context=vm.createContext({window:{},document:{},URLSearchParams});
  vm.runInContext(source.slice(0,end)+'\nglobalThis.requestTest={lc,groupRequests,requestOutcome,requestGroupHTML};\n'+source.slice(end),context);
  return context.requestTest;
}

const failed={id:10,request_id:'retry-request',created_at:1000,type:5,channel_id:1,upstream_status_code:503};
const succeeded={id:11,request_id:'retry-request',created_at:1000,type:2,channel_id:2,anomaly_tags:[]};

test('error+anomaly filtering must not turn a successful retry into a final failure',()=>{
  const {lc,groupRequests,requestOutcome,requestGroupHTML}=requestView();
  assert.equal(lc.scope,'err_anom');
  // The normal retry result exists, but is excluded by the default API scope.
  const allLogs=[succeeded,failed];
  const visible=allLogs.filter(row=>row.type===5||(row.anomaly_tags||[]).length>0);
  lc.hasMore=false; // All matching problems returned is not all request logs.
  const [group]=groupRequests(visible);
  assert.equal(group.rows.length,1);
  assert.equal(requestOutcome(group).text,'当前可见日志的最后状态：错误 · HTTP 503');
  assert.doesNotMatch(requestOutcome(group).text,/最终/);
  const html=requestGroupHTML(group);
  assert.match(html,/成功记录可能被过滤，未取得完整请求链路/);
  assert.doesNotMatch(html,/最终失败/);
});

test('same-second retry success uses the larger ID without inventing chain completeness',()=>{
  const {groupRequests,requestOutcome}=requestView();
  for(const rows of [[succeeded,failed],[failed,succeeded]]) {
    const [group]=groupRequests(rows);
    assert.equal(requestOutcome(group).text,'当前可见日志的最后状态：正常');
    assert.equal(requestOutcome(group).cls,'lc-req-ok');
  }
});

test('pagination and a Request ID filter cannot certify a final request outcome',()=>{
  const {lc,groupRequests,requestOutcome,requestGroupHTML}=requestView();
  lc.filters.request_id=failed.request_id;
  lc.evidenceVerified=true; // Verified edge evidence is not a complete logs chain.
  const [group]=groupRequests([failed]);
  for(const hasMore of [true,false]) {
    lc.hasMore=hasMore;
    assert.equal(requestOutcome(group).text,'当前可见日志的最后状态：错误 · HTTP 503');
    const html=requestGroupHTML(group);
    assert.match(html,/当前可见日志的最后状态/);
    assert.doesNotMatch(html,/最终失败|最终成功/);
    assert.equal(html.includes('该请求可能不完整'),hasMore);
  }
});

test('interleaved Request IDs can leave any group split across pages',()=>{
  const {lc,groupRequests,requestGroupHTML}=requestView();
  lc.hasMore=true;
  const groups=groupRequests([failed,{...failed,id:9,request_id:'another-request'}]);
  assert.equal(groups.length,2);
  for(const group of groups) assert.match(requestGroupHTML(group),/该请求可能不完整/);
});

test('visible outcome keeps timestamp ahead of ID and describes anomalies without a billing claim',()=>{
  const {groupRequests,requestOutcome}=requestView();
  const later={...succeeded,id:1,created_at:1001,quota:0,anomaly_tags:['undelivered_unbilled']};
  const [group]=groupRequests([failed,later]);
  const outcome=requestOutcome(group);
  assert.equal(outcome.cls,'lc-req-warn');
  assert.match(outcome.text,/^当前可见日志的最后状态：写入消费日志，但有交付\/计费异常/);
  assert.doesNotMatch(outcome.text,/最终|已记账/);
});

test('missing Request IDs stay unlinked and an empty group has no claimed outcome',()=>{
  const {groupRequests,requestOutcome}=requestView();
  const groups=groupRequests([{...failed,request_id:''},{...succeeded,request_id:''}]);
  assert.equal(groups.length,2);
  assert.ok(groups.every(group=>group.unlinkable));
  assert.ok(groups.every(group=>requestOutcome(group).text.startsWith('当前可见日志的最后状态：')));
  assert.equal(requestOutcome({rows:[]}).text,'—');
});
