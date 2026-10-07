import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

// Execute the shipped renderers, not a second implementation of their sums.
function fixture(users){
  const page=readFileSync(new URL('../../monitor/page.html',import.meta.url),'utf8');
  const elements=new Map();
  const element=id=>{
    if(!elements.has(id))elements.set(id,{innerHTML:''});
    return elements.get(id);
  };
  const context=vm.createContext({document:{getElementById:element},usageMxCache:{users},
    usageFilterGid:null,usageGrpExpanded:true,usageRowCap:5,requestAnimationFrame(){},
    fmtCost:n=>`$${n.toFixed(2)}`,fmtNum:String,esc:String});
  for(const name of ['usageAccumulateProfile','usageProfileAmount','renderUsageSummary','renderGrpCards']){
    const fn=page.match(new RegExp(`^function ${name}\\([^]*?^}`, 'm'));
    assert.ok(fn,`${name} exists`);
    vm.runInContext(fn[0],context);
  }
  vm.runInContext('renderUsageSummary();renderGrpCards();',context);
  return id=>element(id).innerHTML;
}

test('partial profiles keep known values but label both company and all-user totals',()=>{
  const users=[
    {group_id:1,group_name:'公司',total_used_usd:500,balance_usd:100,total_usd:20},
    {group_id:1,group_name:'公司',total_used_usd:null,balance_usd:null,total_usd:30},
  ];
  const before=JSON.stringify(users),html=fixture(users);
  for(const id of ['usageSummary','usageGrpCards']){
    assert.match(html(id),/\$500\.00/);
    assert.match(html(id),/\$100\.00/);
    assert.equal((html(id).match(/已取得 1\/2 人/g)||[]).length,2);
    assert.match(html(id),/\$50\.00/);
  }
  assert.equal(JSON.stringify(users),before);
});

test('unknown profiles never become zero and complete real zero stays numeric',()=>{
  for(const value of [null,undefined,NaN,Infinity,'']){
    const html=fixture([{group_id:1,total_used_usd:value,balance_usd:value,total_usd:7}]);
    for(const id of ['usageSummary','usageGrpCards']){
      assert.match(html(id),/已取得 0\/1 人/);
      assert.doesNotMatch(html(id),/\$0\.00|NaN|Infinity/);
    }
  }
  const html=fixture([{group_id:1,total_used_usd:0,balance_usd:0,total_usd:0}]);
  assert.match(html('usageSummary'),/\$0\.00/);
  assert.doesNotMatch(html('usageSummary'),/已取得/);
  assert.doesNotMatch(html('usageGrpCards'),/已取得/);
});

test('negative balance remains a known amount, not missing data',()=>{
  const html=fixture([{group_id:1,total_used_usd:25,balance_usd:-5,total_usd:10}]);
  assert.match(html('usageSummary'),/\$-5\.00/);
  assert.doesNotMatch(html('usageSummary'),/已取得/);
});
