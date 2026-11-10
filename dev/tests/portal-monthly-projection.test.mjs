import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {isAbsolute} from 'node:path';

// Exercise the production renderer, including the real date/escaping helpers.
// Optional retained receipts are private local files, not committed fixtures.
const html=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
const ctx=vm.createContext({});
for(const name of ['esc','fmtNum'])vm.runInContext(html.match(new RegExp(`^const ${name}=.*$`,'m'))[0],ctx);
vm.runInContext(html.match(/^function fmtLogTime\([^]*?^.*?;}/m)[0],ctx);
for(const name of ['monthlyMoney','monthlySummaryPrecision','monthlyDisplayUnits','monthlyDisplayCentsMoney','monthlyDisplayResidual','monthlyDisplayNonzero','monthlyNetDisplayResidual','monthlyDisplayNetColumnResidual','monthlyWalletDisplayResidual','monthlyWalletDisplayClosing','monthlyDisplayWallet','monthlyMemberDisplayProjection','monthlyTokens','renderMonthlyMembers','renderMonthlyTokenRows','monthlyCentsOf','monthlyCentsMoney','monthlySubNet','renderMonthlySubscription','renderMonthlyStatement','renderMonthlyList']){
  vm.runInContext(html.match(new RegExp(`^function ${name}\\([^]*?^}`,'m'))[0],ctx);
}
const text=markup=>markup.replace(/<[^>]*>/g,'').replace(/&(amp|lt|gt|quot|#39);/g,(_,k)=>({amp:'&',lt:'<',gt:'>',quot:'"','#39':"'"}[k]));
const cells=row=>[...row.matchAll(/<td\b[^>]*>([^]*?)<\/td>/g)].map(m=>text(m[1]));
const micro=value=>{
  assert.match(value,/^-?\d+\.\d{6}$/);
  return BigInt(value.replace('.',''));
};

test('monthly token breakdown keeps same-name IDs and users separate and preserves tiny amounts',()=>{
  ctx.tokenBill={member_details_version:1,token_details_version:1,credits:{},members:[
    {user_id:201,username:'<用户甲>',opening:'10.000000',credit_total:'0.000000',adjustment:'0.000000',closing:'7.999998',consumption:{amount:'2.000006',refund:'0.000004',net:'2.000002'},token_consumption:[
      {token_id:11,name:'<同名令牌>',amount:'1.000002',refund:'0.000004',net:'0.999998'},
      {token_id:12,name:'<同名令牌>',amount:'1.000002',refund:'0.000000',net:'1.000002'},
      {token_id:0,name:'历史未记录令牌',amount:'0.000002',refund:'0.000000',net:'0.000002'},
    ]},
    {user_id:202,username:'用户乙',opening:'0.000000',credit_total:'0.000000',adjustment:'0.000000',closing:'0.000000',consumption:{amount:'0.000000',refund:'0.000000',net:'0.000000'},token_consumption:[]},
  ]};
  const shell=vm.runInContext('renderMonthlyMembers(tokenBill)',ctx);
  assert.match(shell,/用户与令牌消费明细/);
  assert.ok(shell.includes('&lt;用户甲&gt; · ID 201 · 总消费 ¥2.00'));
  assert.equal([...shell.matchAll(/<details[^>]*\bopen\b/g)].length,2);
  assert.equal([...shell.matchAll(/data-monthly-token-user=/g)].length,3);
  assert.match(shell,/本月无令牌消费或退款记录/);
  const table=vm.runInContext('tokenBill.members.map(renderMonthlyTokenRows).join("")',ctx);
  assert.doesNotMatch(table,/<同名令牌>|<用户甲>/);
  const rows=[...table.matchAll(/<tr data-monthly-token-user="(\d+)" data-monthly-token="(\d+)">([^]*?)<\/tr>/g)];
  assert.deepEqual(rows.map(r=>[r[1],r[2]]),[['201','11'],['201','12'],['201','0']]);
  assert.deepEqual(cells(rows[0][3]),['<同名令牌>','11','¥1.00','¥0.00','¥1.00']);
  assert.deepEqual(cells(rows[2][3]),['历史未记录令牌','—','¥0.00','¥0.00','¥0.00']);
  assert.match(table,/本月无令牌消费或退款记录/);
  ctx.tokenBill.token_details_version=0;
  assert.doesNotMatch(vm.runInContext('renderMonthlyMembers(tokenBill)',ctx),/用户与令牌消费明细|data-monthly-token=/);
});

test('token money alignment is scoped and overrides legacy last-column styling',()=>{
 const css=html.match(/<style>([^]*?)<\/style>/)[1];
 const old=css.indexOf('#monthlyContent .bill-sheet td:last-child{text-align:left}');
 const fixed=css.indexOf('#monthlyContent .bill-token-member td.money{text-align:right}');
 assert.ok(old>=0&&fixed>old);
 assert.doesNotMatch(css,/#monthlyContent\s+td\.money\s*\{/);
});
function money(value){
  return summaryMoney(value);
}
function summaryMoney(value,places=2){
 places=2;
 const n=micro(value),absolute=n<0n?-n:n;
 const scale=10n**BigInt(places),divisor=1000000n/scale;
 const units=(absolute+divisor/2n)/divisor;
 return '¥'+(n<0n&&units!==0n?'-':'')+(units/scale).toLocaleString('en-US')+'.'+String(units%scale).padStart(places,'0');
}
function walletClosing(m){
 const values=[m.opening,m.credit_total,m.adjustment,m.consumption.amount,m.consumption.refund,m.balance_correction??'0.000000'];
 const rounded=values.map(v=>BigInt(summaryMoney(v).replace(/^¥/,'').replaceAll(',','').replace('.','')));
 const n=rounded[0]+rounded[1]+rounded[2]-rounded[3]+rounded[4]+rounded[5],a=n<0n?-n:n;
 return '¥'+(n<0n?'-':'')+(a/100n).toLocaleString('en-US')+'.'+String(a%100n).padStart(2,'0');
}
function verifyRendered(p,expected,places=2){
  ctx.statement=p;
  assert.equal(vm.runInContext('monthlySummaryPrecision(statement)',ctx),2);
  const summary=value=>summaryMoney(value,places);
  const rendered=vm.runInContext('renderMonthlyStatement(statement)',ctx);
  const rows=[...rendered.matchAll(/<tr\b[^>]*>([^]*?)<\/tr>/g)];
  assert.doesNotMatch(rendered,/待核验/);
  assert.ok(text(rendered).includes(p.account+' · 月度消费账单'));
  assert.ok(text(rendered).includes(p.from+' 至 '+p.to));
  const funding=rows.filter(m=>/<td class="center">\d+<\/td>/.test(m[1]));
  assert.equal(funding.length,p.credits.rows.length);
  for(const [i,r] of p.credits.rows.entries()){
    const at=new Date(r.at*1000+28800000).toISOString().slice(0,19).replace('T',' ');
    assert.deepEqual(cells(funding[i][1]),[String(i+1),at,(r.username||'—')+' · ID '+r.user_id,money(r.amount),r.method,r.kind==='gift'?'赠送额度':'']);
  }
  const totals=new Map(rows.filter(m=>/class="bill-total"/.test(m[0])).map(m=>{const c=cells(m[1]);return [c[0],c.slice(1)];}));
  assert.deepEqual(totals.get('充值合计'),[summary(p.credits.recharge_total),p.credits.recharge_count+' 次']);
  for(const [label,value] of [['赠送合计',p.credits.gift_total],['到账合计',p.credits.known_total],['月初余额（元）',p.month_opening_balance]]){
    assert.equal(totals.get(label)[0],summary(value));
  }
  for(const [label,value] of [['退款金额（元）',p.consumption.refund],['余额扣减调整（元）',p.credits.adjustment]]){
    assert.equal(totals.has(label),micro(value)!==0n);
    if(micro(value)!==0n)assert.equal(totals.get(label)[0],summary(value));
  }
  if(micro(p.consumption.refund)!==0n)assert.equal(totals.get('净消费（元）')[0],summary(p.consumption.net));
  const balance=rows.find(m=>/class="bill-balance"/.test(m[0]));
  assert.deepEqual(cells(balance[1]),['月末余额（元）',walletClosing({opening:p.month_opening_balance,credit_total:p.credits.known_total,adjustment:p.credits.adjustment,consumption:p.consumption,balance_correction:p.credits.balance_correction})]);
  const metrics=rows.find(m=>/class="bill-metrics"/.test(m[0]));
  assert.equal(cells(metrics[1])[0],summary(p.consumption.amount));
  assert.equal(cells(metrics[1])[1],p.consumption.requests.toLocaleString('en-US'));
  assert.ok(metrics[1].includes('title="'+p.consumption.tokens.toLocaleString('en-US')+' Token"'));
  const members=[...rendered.matchAll(/<tr data-monthly-member="(\d+)">([^]*?)<\/tr>/g)];
  assert.equal(members.length,expected.length);
  let opening=0n,credit=0n,adjust=0n,consume=0n,refund=0n,closing=0n;
  for(const m of expected){
    const found=members.filter(r=>r[1]===String(m.user_id));
    assert.equal(found.length,1,'each immutable user ID occurs exactly once');
    assert.deepEqual(cells(found[0][2]),[(m.username||'—')+'ID '+m.user_id,...[m.opening,m.credit_total,m.adjustment,m.consumption.amount,m.consumption.refund].map(money),walletClosing(m)]);
    assert.equal(micro(m.opening)+micro(m.credit_total)+micro(m.adjustment)-micro(m.consumption.amount)+micro(m.consumption.refund),micro(m.closing));
    opening+=micro(m.opening);credit+=micro(m.credit_total);adjust+=micro(m.adjustment);consume+=micro(m.consumption.amount);refund+=micro(m.consumption.refund);closing+=micro(m.closing);
  }
  for(const [got,want] of [[p.month_opening_balance,opening],[p.credits.known_total,credit],[p.credits.adjustment,adjust],[p.consumption.amount,consume],[p.consumption.refund,refund],[p.consumption.net,consume-refund],[p.month_end_balance,closing]])assert.equal(micro(got),want);
  ctx.list={rows:[{month:p.month,recharge_count:p.credits.recharge_count,recharge_total:p.credits.recharge_total,gift_total:p.credits.gift_total,credit_total:p.credits.known_total,consumption:p.consumption,money_decimal_places:places,downloadable:true}]};
  const list=vm.runInContext('renderMonthlyList(list)',ctx);
  const values=cells(list.match(/<tbody>([^]*?)<\/tbody>/)[1]);
  assert.deepEqual(values.slice(0,7),[p.month,p.credits.recharge_count.toLocaleString('en-US')+' 次',summary(p.credits.recharge_total),summary(p.credits.gift_total),summary(p.credits.known_total),summary(p.consumption.amount),p.consumption.requests.toLocaleString('en-US')]);
}

test('actual renderer keeps transfer signs, gifts, refunds, tiny balances and UTC+8 attribution',()=>{
  const members=[
    {user_id:122,username:'<转出&用户>',opening:'800.000000',credit_total:'1.000000',adjustment:'-800.000000',consumption:{amount:'1.000004',refund:'0.000002'},closing:'-0.000002'},
    {user_id:125,username:'转入',opening:'0.000000',credit_total:'800.000000',adjustment:'0.000000',consumption:{amount:'0.000004',refund:'0.000000'},closing:'799.999996'},
    {user_id:126,username:'零活动',opening:'0.000000',credit_total:'0.000000',adjustment:'0.000000',consumption:{amount:'0.000000',refund:'0.000000'},closing:'0.000000'},
  ];
  const p={account:'本机合成组织',month:'2026-09',from:'2026-09-01',to:'2026-09-30',month_opening_balance:'800.000000',month_end_balance:'799.999994',member_details_version:1,members,consumption:{amount:'1.000008',refund:'0.000002',net:'1.000006',requests:9007199254740991,tokens:9007199254740991},credits:{available:true,rows:[
    {user_id:122,username:members[0].username,at:1788192000,amount:'-800.000000',method:'余额扣减',kind:'adjustment'},
    {user_id:125,username:'转入',at:1788192000,amount:'800.000000',method:'后台充值',kind:'recharge'},
    {user_id:122,username:members[0].username,at:1788192001,amount:'1.000000',method:'活动赠送',kind:'gift'},
  ],recharge_count:1,recharge_total:'800.000000',gift_total:'1.000000',known_total:'801.000000',adjustment:'-800.000000'}};
  verifyRendered(p,members);
});

const receipt=process.env.MONTHLY_DISPLAY_STATEMENTS,expectedPath=process.env.MONTHLY_DISPLAY_EXPECTED;
test('retained real HTTP receipts render every account/month and agree with independent expectations',{skip:!receipt&&!expectedPath},t=>{
  assert.ok(receipt&&expectedPath&&isAbsolute(receipt)&&isAbsolute(expectedPath),'both explicit absolute local paths are required');
  const batch=JSON.parse(readFileSync(receipt,'utf8'));
  const expected=JSON.parse(readFileSync(expectedPath,'utf8'));
  assert.equal(batch.mode,'temporary_database_acceptance_only_not_customer_publication');
  const ready=expected.filter(e=>!e.issues?.length);
  assert.equal(batch.statements.length,ready.length);
  assert.equal(batch.excluded_months,expected.length-ready.length);
  let memberRows=0,creditRows=0;
  for(const entry of ready){
    const matches=batch.statements.filter(p=>p.account===entry.account&&p.month===entry.month);
    assert.equal(matches.length,1);
    verifyRendered(matches[0],entry.expected,entry.money_decimal_places??2);
    memberRows+=entry.expected.length;creditRows+=matches[0].credits.rows.length;
  }
  assert.equal(memberRows,batch.member_months);
  t.diagnostic(`verified ${ready.length} bills, ${memberRows} account rows, ${creditRows} funding rows; excluded ${batch.excluded_months} unresolved months`);
});
