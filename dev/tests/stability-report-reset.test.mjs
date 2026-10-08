import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function deferred(){
  let resolve;
  const promise=new Promise(done=>{resolve=done});
  return {promise,resolve};
}

function fixture(){
  const requests=[];
  const elements=new Map();
  const element=id=>{
    if(!elements.has(id))elements.set(id,{innerHTML:''});
    return elements.get(id);
  };
  const context=vm.createContext({
    AbortController,URLSearchParams,
    document:{querySelector(){return null},getElementById:element,addEventListener(){}},
    window:{},
    fetch(url,options){
      const pending=deferred();
      requests.push({url,options,respond:data=>pending.resolve({ok:true,status:200,json:async()=>data})});
      return pending.promise;
    },
  });
  const source=readFileSync(new URL('../../monitor/stability.js',import.meta.url),'utf8');
  const end=source.lastIndexOf('})();');
  // Isolate the report/detail lifecycle from charts; keep the real group renderer.
  vm.runInContext(source.slice(0,end)+
    '\nrenderReport=renderGroups;globalThis.fixture={st,loadReport,ensureGroupDetail,groupClick,renderGroups};\n'+
    source.slice(end),context);
  const api=context.fixture;
  api.st.report={groups:[group()],meta:{}};
  return {api,requests,element};
}

function group(){return {name:'test-group',requests:10,problems:0,timeline:[],channels:[],models:[]}}
function report(){return {groups:[group()],meta:{},filters:{}}}

test('report refresh, filter change and reset collapse old details without eager requests',async()=>{
  const {api,requests,element}=fixture();
  for(const filters of [{group:'',channel:'',model:''},{group:'test-group',channel:'37',model:'model-a'},{group:'',channel:'',model:''}]){
    api.st.filters={vendor:'',...filters};
    api.st.expanded.add('test-group');
    const count=requests.length;
    const loading=api.loadReport();
    assert.equal(api.st.expanded.size,0);
    requests[count].respond(report());
    await loading;
    assert.equal(requests.length,count+1,'refresh must not eagerly fetch all details');
    assert.doesNotMatch(element('stGroupList').innerHTML,/详情尚未加载|▾/);
    assert.match(element('stGroupList').innerHTML,/▸ test-group/);
  }
  const clicked=api.groupClick({target:{closest:selector=>selector==='[data-st-group]'?{dataset:{stGroup:'test-group'}}:null}});
  assert.equal(api.st.expanded.has('test-group'),true);
  assert.match(requests.at(-1).url,/\/stability\/detail\?/);
  requests.at(-1).respond({group:group()});
  await clicked;
  assert.equal(api.st.report.groups[0]._detail_loaded,true);
  assert.doesNotMatch(element('stGroupList').innerHTML,/详情尚未加载/);
});

test('late old detail response cannot repopulate a reset report',async()=>{
  const {api,requests,element}=fixture();
  api.st.expanded.add('test-group');
  const oldDetail=api.ensureGroupDetail('test-group');
  const reload=api.loadReport();
  assert.equal(requests[0].options.signal.aborted,true);
  requests[1].respond(report());
  await reload;
  requests[0].respond({group:{...group(),requests:999}});
  assert.equal(await oldDetail,null);
  assert.equal(api.st.report.groups[0].requests,10);
  assert.equal(api.st.report.groups[0]._detail_loaded,undefined);
  assert.equal(api.st.expanded.size,0);
  assert.equal(api.st.detailLoading.size,0);
  assert.doesNotMatch(element('stGroupList').innerHTML,/详情尚未加载|999/);
});

test('a superseded report cannot restore the old group or expansion',async()=>{
  const {api,requests}=fixture();
  api.st.expanded.add('test-group');
  const first=api.loadReport();
  api.st.filters.group='other-group';
  const second=api.loadReport();
  assert.equal(requests[0].options.signal.aborted,true);
  requests[1].respond({...report(),groups:[{...group(),name:'other-group'}]});
  await second;
  requests[0].respond(report());
  await first;
  assert.equal(api.st.report.groups[0].name,'other-group');
  assert.equal(api.st.expanded.size,0);
});
