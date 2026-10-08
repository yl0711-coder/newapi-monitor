import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function fixture(){
  const elements=new Map();
  const element=id=>{if(!elements.has(id))elements.set(id,{innerHTML:''});return elements.get(id)};
  const context=vm.createContext({document:{querySelector(){return null},getElementById:element,addEventListener(){}},window:{},
    fetch(){throw Error('unnamed groups must never issue detail requests')}});
  const source=readFileSync(new URL('../../monitor/stability.js',import.meta.url),'utf8');
  const end=source.lastIndexOf('})();');
  vm.runInContext(source.slice(0,end)+'\nglobalThis.fixture={st,renderGroups,ensureGroupDetail,openDrawer};\n'+source.slice(end),context);
  return {api:context.fixture,element};
}

test('unattributed rejects stay visible without broken group controls',async()=>{
  const {api,element}=fixture();
  for(const name of ['', '  ', null]){
    const group={name,requests:0,rejected:5,problems:0,timeline:[],channels:null};
    api.st.report={groups:[group],meta:{}};
    api.renderGroups();
    const html=element('stGroupList').innerHTML;
    assert.match(html,/未归属分组/);
    assert.match(html,/5 次入口拒绝/);
    assert.match(html,/无法下钻/);
    assert.doesNotMatch(html,/data-st-group|data-st-detail|完整详情/);
    assert.equal(await api.ensureGroupDetail(name),null);
    await api.openDrawer(name,0);
    assert.equal(api.st.drawer,null);
    assert.equal(group.rejected,5);
  }
});

test('normal groups preserve detail controls and escaped labels',()=>{
  const {api,element}=fixture();
  api.st.report={groups:[{name:'<group>',requests:10,problems:1,timeline:[],channels:[],models:[]}],meta:{}};
  api.renderGroups();
  const html=element('stGroupList').innerHTML;
  assert.match(html,/data-st-group="&lt;group&gt;"/);
  assert.match(html,/data-st-detail="group"/);
  assert.doesNotMatch(html,/<group>/);
});
