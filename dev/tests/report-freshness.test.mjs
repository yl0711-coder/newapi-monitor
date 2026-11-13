import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const read=name=>readFileSync(new URL('../../monitor/'+name,import.meta.url),'utf8');
function frtView(){
 const nodes=new Map(),get=id=>{if(!nodes.has(id)){const classes=new Set();nodes.set(id,{textContent:'',innerHTML:'',classList:{toggle:(key,on)=>on?classes.add(key):classes.delete(key),contains:key=>classes.has(key)}})}return nodes.get(id)};
 const context=vm.createContext({window:{},document:{getElementById:get},URLSearchParams});
 vm.runInContext(read('view_refresh.js'),context);context.window.MonitorView.preserve=(_root,update)=>update();
 const source=read('model_frt.js'),end=source.lastIndexOf('})();');
 vm.runInContext(source.slice(0,end)+'\nglobalThis.renderFRT=renderContent;\n'+source.slice(end),context);
 return {render:context.renderFRT,banner:get('frtCoverage')};
}
const through=1793070000;
const report=(patch={})=>({from:'2026-10-26T03:00:00Z',to:'2026-10-27T03:00:00Z',actual_through_ts:through,lag_seconds:60,timing:{finalize_delay_seconds:120,alignment_seconds:60,collection_poll_seconds:60},coverage:{complete:true,note:'统计窗口覆盖完整。'},channels:[],...patch});
test('FRT distinguishes closed coverage from normal tail, backlog, failure and history gaps',()=>{
 const view=frtView();
 for(const [data,warn,text]of[
  [report(),false,/正常等待/],
  [report({lag_seconds:180}),true,/采集积压 180 秒/],
  [report({lag_seconds:0,collection_failed:true}),true,/采集失败/],
  [report({lag_seconds:0,coverage:{complete:false,note:'历史区间存在缺口。'}}),true,/历史区间存在缺口/],
  [report({from:'2026-10-20T00:00:00Z',to:'2026-10-21T00:00:00Z',actual_through_ts:1792540800,lag_seconds:0}),false,/2026\/10\/21/]
 ]){view.render(data);assert.equal(view.banner.classList.contains('gap'),warn);assert.match(view.banner.textContent,text);assert.match(view.banner.textContent,/数据截至/);assert.doesNotMatch(view.banner.textContent,/数据延迟\/覆盖不完整/)}
});
test('Token upstream identifies current and deleted snapshots without claiming historical routing',()=>{
 const source=read('token_anomaly.js'),start=source.indexOf('function tokenUpstream('),end=source.indexOf('function renderList(',start);
 const context=vm.createContext({esc:s=>String(s).replaceAll('<','&lt;'),time:n=>String(n)});vm.runInContext(source.slice(start,end),context);
 assert.equal(context.tokenUpstream({}),'未知');
 assert.equal(context.tokenUpstream({upstream:'unproven.test'}),'未知');
 for(const [upstream_source,label]of[['local_current_snapshot','当前渠道快照'],['local_last_snapshot','删除前最后快照']]){
  const text=context.tokenUpstream({upstream:'api.vendor.test',upstream_source,upstream_observed_at:123});
  assert.match(text,/api.vendor.test/);assert.ok(text.includes(label));assert.match(text,/请求时归属未确认/);assert.match(text,/快照时间：123/);
 }
});
