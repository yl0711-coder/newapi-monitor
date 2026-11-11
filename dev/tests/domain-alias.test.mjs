import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source=name=>readFileSync(new URL(`../../monitor/${name}`,import.meta.url),'utf8');
function fixture(){
  const elements=new Map();
  const element=id=>{
    if(!elements.has(id))elements.set(id,{innerHTML:'',hidden:true,value:'',addEventListener(){}});
    return elements.get(id);
  };
  const ctx=vm.createContext({window:{},document:{getElementById:element,addEventListener(){}},esc:v=>String(v??'')});
  vm.runInContext(source('channel_data_status.js'),ctx);
  const js=source('channel_management.js'),end=js.lastIndexOf('})();');
  vm.runInContext(js.slice(0,end)+'\nglobalThis.ui={renderDomainAlias,domainAliasTargets,cm};\n'+js.slice(end),ctx);
  return {ui:ctx.ui,element};
}

test('unaccounted channel address can be aliased to a configured supplier',()=>{
  const {ui,element}=fixture();
  ui.cm.report={domains:[
    {domain:'148.163.65.162',upstream:{configured:false}},
    {domain:'rkshadow.com',upstream:{configured:true}},
    {domain:'other.example',upstream:{configured:false}},
  ]};
  ui.renderDomainAlias(ui.cm.report.domains[0],false);
  const box=element('cmDomainAlias');
  assert.equal(box.hidden,false);
  assert.match(box.innerHTML,/归并到所选上游/);
  assert.match(box.innerHTML,/<option value="rkshadow.com">/);
  assert.doesNotMatch(box.innerHTML,/other\.example/);
  ui.renderDomainAlias(ui.cm.report.domains[1],true);
  assert.equal(box.hidden,true,'a configured supplier without aliases shows nothing');
});

test('target domain lists its aliases with a remove action',()=>{
  const {ui,element}=fixture();
  ui.cm.report={domains:[{domain:'rkshadow.com',upstream:{configured:true},aliases:['148.163.65.162']}]};
  ui.renderDomainAlias(ui.cm.report.domains[0],true);
  const box=element('cmDomainAlias');
  assert.match(box.innerHTML,/148\.163\.65\.162 <button[^>]*data-cm-alias-remove="148\.163\.65\.162"/);
  ui.renderDomainAlias(null,false);
  assert.equal(box.hidden,true);
});
