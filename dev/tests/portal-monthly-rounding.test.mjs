import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const source=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
function setup(d){
 const ctx=vm.createContext({d,esc:v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'),fmtNum:String,fmtLogTime:String});
 for(const name of ['monthlyMoney','monthlySummaryPrecision','monthlyDisplayUnits','monthlyDisplayCentsMoney','monthlyDisplayResidual','monthlyDisplayNonzero','monthlyNetDisplayResidual','monthlyDisplayNetColumnResidual','monthlyWalletDisplayResidual','monthlyWalletDisplayClosing','monthlyDisplayWallet','monthlyMemberDisplayProjection','monthlyTokens','renderMonthlyMembers','renderMonthlyTokenRows','renderMonthlyStatement'])vm.runInContext(source.match(new RegExp(`^function ${name}\\([^]*?^}`,'m'))[0],ctx);
 return ctx;
}
test('shared Go/browser rounding cases only explain exact source equations',()=>{
 const cases=JSON.parse(readFileSync(new URL('../../monitor/testdata/monthly_display_rounding.json',import.meta.url),'utf8')),ctx=setup();
 for(const c of cases){ctx.c=c;assert.equal(vm.runInContext('monthlyDisplayResidual(c.total,c.terms)',ctx),c.residual,c.name);}
});
test('money formatting and reconciliation share strict exact cents with Go and Excel',()=>{
 const cases=JSON.parse(readFileSync(new URL('../../monitor/testdata/monthly_money_cents.json',import.meta.url),'utf8')),ctx=setup();
 for(const c of cases){
  ctx.value=c.value;
  const got=vm.runInContext('monthlyMoney(value)',ctx),units=vm.runInContext('monthlyDisplayUnits(value)',ctx);
  if(c.want===null){assert.equal(got,'待核验',JSON.stringify(c.value));assert.equal(units,null);continue;}
  assert.equal(got.replace(/^¥/,'').replaceAll(',',''),c.want,c.value);
  assert.equal(units,BigInt(c.want.replace('.','')),c.value);
  assert.equal(vm.runInContext('monthlyMoney(value,true,6)',ctx),got,'legacy hints cannot change money');
  assert.equal(ctx.value,c.value);
 }
 for(const value of [null,undefined,1.005,NaN,Infinity,{},['1.005000']]){
  ctx.value=value;assert.equal(vm.runInContext('monthlyMoney(value)',ctx),'待核验');
 }
});
test('full-range quota values match an independent quotient/remainder oracle',()=>{
 const ctx=setup();let seed=20261027n;
 for(let i=0;i<2000;i++){
  seed=BigInt.asUintN(64,seed*6364136223846793005n+1442695040888963407n);
  const quota=BigInt.asIntN(64,seed),micro=quota*2n,absolute=micro<0n?-micro:micro;
  const digits=absolute.toString().padStart(7,'0');ctx.value=(micro<0n?'-':'')+digits.slice(0,-6)+'.'+digits.slice(-6);
  let expected=absolute/10000n;if(absolute%10000n>=5000n)expected++;
  if(micro<0n)expected=-expected;
  assert.equal(vm.runInContext('monthlyDisplayUnits(value)',ctx),expected,ctx.value);
  assert.equal(BigInt(vm.runInContext('monthlyMoney(value)',ctx).replace(/^¥/,'').replaceAll(',','').replace('.','')),expected);
 }
});
const micro=v=>BigInt(v.replace('.',''));
const cents=v=>{const n=micro(v),a=n<0n?-n:n;return(n<0n?-1n:1n)*((a+5000n)/10000n);};
test('derived balance uses rounded operands without editing consumption, credit or source wallet',()=>{
 const consumption={amount:'3026.195784',refund:'0.000000',net:'3026.195784',requests:1,tokens:1};
 const d={account:'synthetic',month:'2026-09',from:'2026-09-01',to:'2026-09-30',month_opening_balance:'2321.744170',month_end_balance:'4295.548386',consumption,credits:{available:true,rows:[{kind:'recharge',amount:'5000.000000',user_id:1,username:'account',at:1,method:'后台充值'}],recharge_count:1,recharge_total:'5000.000000',gift_total:'0.000000',known_total:'5000.000000',adjustment:'0.000000'},member_details_version:1,token_details_version:1,members:[{user_id:1,username:'account',opening:'2321.744170',credit_total:'5000.000000',adjustment:'0.000000',consumption,closing:'4295.548386',token_consumption:[{token_id:1,name:'token',amount:consumption.amount,refund:'0.000000',net:consumption.net}]}]};
 const before=JSON.stringify(d),ctx=setup(d),out=vm.runInContext('renderMonthlyStatement(d)',ctx);
 assert.doesNotMatch(out,/四舍五入尾差（元）|<th>舍入尾差（元）<\/th>/);
 assert.match(out,/¥4,295\.54/);assert.doesNotMatch(out,/¥4,295\.55/);assert.match(out,/¥3,026\.20/);
 assert.doesNotMatch(out,/¥[\d,]+\.\d{3}/);assert.equal(JSON.stringify(d),before);
 assert.equal(cents(d.month_opening_balance)+500000n-cents(consumption.amount),429554n);
});
test('derived balances refuse missing sources and real discrepancies instead of manufacturing a match',()=>{
 const ctx=setup();ctx.m={opening:'1.005000',credit_total:'1.005000',adjustment:'-0.005000',consumption:{amount:'1.005000',refund:'0.005000'},balance_correction:'0.005000',closing:'1.010000'};
 const before=JSON.stringify(ctx.m);
 assert.equal(vm.runInContext('monthlyWalletDisplayClosing(m)',ctx),'1.020000');
 assert.equal(JSON.stringify(ctx.m),before);
 ctx.m.closing='1.010002';assert.equal(vm.runInContext('monthlyWalletDisplayClosing(m)',ctx),null);
 ctx.m.closing='1.010000';ctx.m.opening=null;assert.equal(vm.runInContext('monthlyWalletDisplayClosing(m)',ctx),null);
});
test('frozen next-month cents are used in both summary and account without rerounding source balances',()=>{
 const consumption={amount:'0.005000',refund:'0.000000',net:'0.005000',requests:1,tokens:0};
 const member={user_id:1,username:'synthetic',opening:'4295.548386',credit_total:'0.000000',adjustment:'0.000000',consumption,closing:'4295.543386'};
 const d={month:'2026-09',month_opening_balance:member.opening,month_end_balance:member.closing,consumption,member_details_version:1,members:[member],credits:{available:true,recharge_count:0,recharge_total:'0.000000',gift_total:'0.000000',known_total:'0.000000',adjustment:'0.000000',rows:[]},display_balances:{version:1,organization:{opening:'4295.540000',closing:'4295.530000'},members:[{user_id:1,opening:'4295.540000',closing:'4295.530000'}]}};
 const before=JSON.stringify(d),ctx=setup(d),out=vm.runInContext('renderMonthlyStatement(d)',ctx);
 assert.match(out,/月初余额（元）<\/td><td class="money">¥4,295\.54/);
 assert.match(out,/class="bill-balance"[^]*?>¥4,295\.53<\/td>/);
 assert.match(out,/data-monthly-member="1"[^]*?¥4,295\.54[^]*?¥4,295\.53/);
 assert.doesNotMatch(out,/¥4,295\.55|四舍五入尾差（元）/);assert.equal(JSON.stringify(d),before);
 d.display_balances.organization.closing='4295.540000';
 assert.match(vm.runInContext('renderMonthlyStatement(d)',ctx),/class="bill-balance"[^]*?>待核验<\/td>/);
 d.display_balances.organization=undefined;
 assert.match(vm.runInContext('renderMonthlyStatement(d)',ctx),/class="bill-balance"[^]*?>待核验<\/td>/);
});
test('organization footer explains summing independently rounded accounts without redistributing balances',()=>{
 const zero={amount:'0.000000',refund:'0.000000',net:'0.000000'},members=[1,2].map(user_id=>({user_id,username:'account'+user_id,opening:'0.004998',credit_total:'0.000000',adjustment:'0.000000',consumption:zero,closing:'0.004998',token_consumption:[]}));
 const d={member_details_version:1,members,month_opening_balance:'0.009996',month_end_balance:'0.009996',consumption:zero,credits:{known_total:'0.000000',adjustment:'0.000000'}},before=JSON.stringify(d),ctx=setup(d);
 const model=vm.runInContext('monthlyMemberDisplayProjection(d)',ctx);
 assert.equal(model.rounding,false);assert.deepEqual(Array.from(model.residuals),['0.010000','0.000000','0.000000','0.000000','0.000000','0.010000']);
 const out=vm.runInContext('renderMonthlyMembers(d)',ctx);assert.match(out,/data-monthly-member-rounding/);assert.match(out,/data-monthly-member-total/);assert.equal(JSON.stringify(d),before);
});
test('token totals contain explicit negative rounding row, never a fake token',()=>{
 const m={user_id:1,consumption:{amount:'0.010004',refund:'0.000000',net:'0.010004'},token_consumption:[1,2].map(token_id=>({token_id,name:'token',amount:'0.005002',refund:'0.000000',net:'0.005002'}))},ctx=setup();ctx.m=m;
 const before=JSON.stringify(m),out=vm.runInContext('renderMonthlyTokenRows(m)',ctx);
 assert.equal([...out.matchAll(/data-monthly-token-user=/g)].length,2);assert.match(out,/data-monthly-token-rounding[^]*?¥-0\.01/);assert.match(out,/用户合计[^]*?¥0\.01/);assert.equal(JSON.stringify(m),before);
});
test('no zero tails, missing values or genuine discrepancies are converted into rounding adjustments',()=>{
 const ctx=setup();ctx.m={user_id:1,consumption:{amount:'0.000000',refund:'0.000000',net:'0.000000'},token_consumption:[]};
 assert.doesNotMatch(vm.runInContext('renderMonthlyTokenRows(m)',ctx),/data-monthly-token-rounding/);
 ctx.m.consumption.amount='1.000000';assert.doesNotMatch(vm.runInContext('renderMonthlyTokenRows(m)',ctx),/data-monthly-token-rounding/);
 ctx.m.consumption.amount=null;assert.doesNotMatch(vm.runInContext('renderMonthlyTokenRows(m)',ctx),/data-monthly-token-rounding/);
});

test('refund net amounts reconcile horizontally without changing the original net charge',()=>{
 const m={user_id:1,consumption:{amount:'1.005000',refund:'0.004998',net:'1.000002'},token_consumption:[{token_id:1,name:'refunded token',amount:'1.005000',refund:'0.004998',net:'1.000002'}]},ctx=setup();ctx.m=m;
 const before=JSON.stringify(m),out=vm.runInContext('renderMonthlyTokenRows(m)',ctx);
 assert.match(out,/净额舍入尾差（元）/);
 const row=out.match(/<tr data-monthly-token-user[^]*?<\/tr>/)[0],shown=[...row.matchAll(/class="money">¥(-?[\d,]+\.\d{2})<\/td>/g)].map(m=>BigInt(m[1].replaceAll(',','').replace('.','')));
 assert.equal(shown.length,4);assert.equal(shown[0]-shown[1]+shown[3],shown[2]);assert.equal(JSON.stringify(m),before);
});
