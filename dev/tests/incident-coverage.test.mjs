import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function view(){
 const nodes=new Map(),get=id=>{if(!nodes.has(id))nodes.set(id,{textContent:'',innerHTML:'',hidden:false});return nodes.get(id)};
 const context=vm.createContext({window:{},document:{getElementById:get},URLSearchParams});
 const source=readFileSync(new URL('../../monitor/incident.js',import.meta.url),'utf8'),end=source.lastIndexOf('})();');
 vm.runInContext(source.slice(0,end)+'\nglobalThis.testView={state,render,coverageText};\n'+source.slice(end),context);
 return {...context.testView,get};
}
test('incident coverage distinguishes a completed range, real gap and normal finalization',()=>{
 const v=view();
 const base={enabled:true,from_ts:1793548800,to_ts:1793581200,generated_at:1793583000,coverage:{complete:true,status:'complete',actual_through_ts:1793581200,global_complete_through_ts:1793542800,lag_seconds:0}};
 for(const [coverage,expected] of [
  [base.coverage,/覆盖完整/],
  [{...base.coverage,complete:false,status:'incomplete',lag_seconds:300},/落后目标 300 秒/],
  [{...base.coverage,complete:false,status:'finalizing'},/正常等待封口/]
 ]){
  v.state.report={...base,coverage};v.state.query={from:base.from_ts,to:base.to_ts};v.state.rows=[];v.render();
  assert.match(v.coverageText(v.state.report),expected);
  assert.match(v.get('imUpdated').textContent,/所选范围完整处理截止/);
  assert.match(v.get('imRange').textContent,/全局历史完整处理截止/);
  assert.doesNotMatch(v.get('imList').innerHTML,/38,400|38400/);
  if(coverage.status==='finalizing')assert.doesNotMatch(v.get('imList').innerHTML,/落后目标|没有已记录事故/);
 }
});
