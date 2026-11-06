import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {isAbsolute} from 'node:path';

// Actual shipped renderer, not a money/UI mock. Explicit synthetic artifacts
// come from the independent Go/rational simulation and never from a live site.
const source=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
const ctx=vm.createContext({esc:v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;'),fmtNum:String,fmtLogTime:String});
for(const name of ['monthlyMoney','monthlySummaryPrecision','monthlyDisplayUnits','monthlyDisplayCentsMoney','monthlyDisplayResidual','monthlyDisplayNonzero','monthlyNetDisplayResidual','monthlyDisplayNetColumnResidual','monthlyWalletDisplayResidual','monthlyWalletDisplayClosing','monthlyDisplayWallet','monthlyMemberDisplayProjection','monthlyTokens','renderMonthlyMembers','renderMonthlyTokenRows','renderMonthlyStatement']){
  vm.runInContext(source.match(new RegExp(`^function ${name}\\([^]*?^}`,'m'))[0],ctx);
}
const amountCells=markup=>[...markup.matchAll(/<td class="money">¥(-?[\d,]+\.\d{2})<\/td>/g)].map(m=>m[1].replaceAll(',',''));
const cents=raw=>{
  assert.match(raw,/^-?\d+\.\d{6}$/);
  const micro=BigInt(raw.replace('.','')),a=micro<0n?-micro:micro;
  return(micro<0n?-1n:1n)*((a+5000n)/10000n);
};
const rawQuota=q=>{
  const n=q*2n,a=(n<0n?-n:n).toString().padStart(7,'0');
  return(n<0n?'-':'')+a.slice(0,-6)+'.'+a.slice(-6);
};
test('client money simulation agrees with independent integer cents for 20000 signed quota vectors',()=>{
  let seed=20261027n;
  const vectors=[0n,1n,-1n,2499n,2500n,2501n,-2499n,-2500n,-2501n,502500n,-502500n,(1n<<63n)-1n,-(1n<<63n)];
  for(let i=vectors.length;i<20000;i++){
    seed=BigInt.asUintN(64,seed*6364136223846793005n+1442695040888963407n);
    vectors.push(BigInt.asIntN(64,seed));
  }
  for(const q of vectors){
    ctx.raw=rawQuota(q);
    assert.equal(vm.runInContext('monthlyDisplayUnits(raw)',ctx),cents(ctx.raw),ctx.raw);
    const formatted=vm.runInContext('monthlyMoney(raw)',ctx);
    assert.match(formatted,/^¥-?[\d,]+\.\d{2}$/);
    assert.equal(BigInt(formatted.replace(/^¥/,'').replaceAll(',','').replace('.','')),cents(ctx.raw));
    assert.equal(ctx.raw,rawQuota(q));
  }
});

const file=process.env.MONTHLY_MONEY_SIMULATION_CASES;
test('real client renderer matches independently simulated organizations, accounts and token owners',{skip:!file},t=>{
  assert.ok(isAbsolute(file));
  const batch=JSON.parse(readFileSync(file,'utf8'));
  assert.equal(batch.mode,'synthetic_money_simulation_no_production_access');
  assert.equal(batch.statements.length,600);
  let members=0,tokens=0;
  for(const entry of batch.statements){
    ctx.d=entry.data;
    const before=JSON.stringify(ctx.d),out=vm.runInContext('renderMonthlyStatement(d)',ctx);
    assert.doesNotMatch(out,/待核验|¥-?[\d,]+\.\d{3}/,entry.label);
    const summary=out.match(/月初余额（元）<\/td><td class="money">¥(-?[\d,]+\.\d{2})/);
    const closing=out.match(/class="bill-balance"[^]*?>¥(-?[\d,]+\.\d{2})<\/td>/);
    assert.equal(summary?.[1].replaceAll(',',''),entry.organization[0],entry.label);
    assert.equal(closing?.[1].replaceAll(',',''),entry.organization[5],entry.label);
    for(const member of ctx.d.members){
      const row=out.match(new RegExp(`<tr data-monthly-member="${member.user_id}">([^]*?)<\\/tr>`));
      assert.ok(row,entry.label);
      assert.deepEqual(amountCells(row[0]),entry.members[String(member.user_id)],entry.label);
      members++;
      for(const token of member.token_consumption||[]){
        const tokenRow=out.match(new RegExp(`<tr data-monthly-token-user="${member.user_id}" data-monthly-token="${token.token_id}">([^]*?)<\\/tr>`));
        assert.ok(tokenRow,entry.label);
        const actual=amountCells(tokenRow[0]);
        assert.ok(actual.length===3||actual.length===4);
        for(const [i,key]of ['amount','refund','net'].entries())assert.equal(BigInt(actual[i].replace('.','')),cents(token[key]),entry.label);
        const [gross,refund,net,tail=0n]=actual.map(v=>BigInt(v.replace('.','')));
        assert.equal(gross-refund+tail,net,entry.label);
        tokens++;
      }
    }
    assert.equal(JSON.stringify(ctx.d),before,'render mutated source: '+entry.label);
  }
  assert.equal(members,batch.accounts);assert.equal(tokens,batch.tokens);
  t.diagnostic(`seed=${batch.seed}; statements=600; accounts=${members}; tokens=${tokens}; actual production renderer matches independent cent expectations`);
});
