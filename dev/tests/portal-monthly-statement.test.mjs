import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const html=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
function context(extra={}){
  const ctx=vm.createContext({esc:v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'),fmtNum:n=>String(n),fmtLogTime:n=>String(n),...extra});
  for(const name of ['monthlyMoney','monthlySummaryPrecision','monthlyDisplayUnits','monthlyDisplayCentsMoney','monthlyDisplayResidual','monthlyDisplayNonzero','monthlyNetDisplayResidual','monthlyDisplayNetColumnResidual','monthlyWalletDisplayResidual','monthlyWalletDisplayClosing','monthlyDisplayWallet','monthlyMemberDisplayProjection','monthlyTokens','renderMonthlyMembers','renderMonthlyTokenRows','monthlyCentsOf','monthlyCentsMoney','monthlySubNet','renderMonthlySubscription','renderMonthlyStatement','renderMonthlyList','monthlyBillFilename'])vm.runInContext(html.match(new RegExp(`^function ${name}\\([^]*?^}`,'m'))[0],ctx);
  return ctx;
}
const sample={account:'sample',published:true,publication:'a'.repeat(64),month:'2026-09',from:'2026-09-01',to:'2026-09-30',as_of:1,current_month:false,credits:{available:true,rows:[{at:2,amount:'20000.000000',method:'后台充值',kind:'recharge'}],recharge_count:1,recharge_total:'20000.000000',gift_total:'0.000000',known_total:'20000.000000',unknown:0,adjustment:'0.000000'},consumption:{requests:100,tokens:20000,amount:'0.000002',refund:'0.000000',net:'0.000002'},current_balance:'8000.000000',month_end_balance:'19999.999998'};

test('all shared historical precision hints display cents without changing money',()=>{
 const cases=JSON.parse(readFileSync(new URL('../../monitor/testdata/monthly_summary_precision.json',import.meta.url),'utf8'));
 for(const tc of cases){
  const money=tc.micro.map(value=>{let n=BigInt(value),sign=n<0n?'-':'';if(n<0n)n=-n;const s=n.toString().padStart(7,'0');return sign+s.slice(0,-6)+'.'+s.slice(-6);});
  const d={...structuredClone(sample),month_opening_balance:money[0],month_end_balance:money[6],credits:{...sample.credits,known_total:money[1],adjustment:money[2],balance_correction:money[5],recharge_total:money[7],gift_total:money[8]},consumption:{...sample.consumption,amount:money[3],refund:money[4],net:money[9]}};
  const before=JSON.stringify(d),ctx=context({d});
  assert.equal(vm.runInContext('monthlySummaryPrecision(d)',ctx),2,tc.name);
  const rendered=vm.runInContext('renderMonthlyStatement(d)',ctx);
  ctx.row={month:d.month,recharge_count:1,recharge_total:money[7],gift_total:money[8],credit_total:money[1],consumption:d.consumption,downloadable:true,money_decimal_places:tc.places};
  const list=vm.runInContext('renderMonthlyList({rows:[row]})',ctx);
  ctx.amount=money[3];ctx.places=tc.places;
  const display=vm.runInContext('monthlyMoney(amount,true,places)',ctx);
  assert.ok(rendered.includes(display),tc.name);assert.ok(list.includes(display),tc.name);
  assert.equal(JSON.stringify(d),before);
 }
});

test('tokens use Chinese units with exact count retained in tooltip',()=>{
 const ctx=context();
 for(const [value,want] of [[0,'0'],[999,'999'],[1200,'1.20 千'],[12345,'1.23 万'],[100000000,'1.00 亿'],[17788983329,'177.89 亿']]){
  ctx.value=value;const output=vm.runInContext('monthlyTokens(value)',ctx);
  assert.ok(output.includes('>'+want+'</span>'));assert.ok(output.includes('title="'+value+' Token"'));
 }
 ctx.value=null;assert.equal(vm.runInContext('monthlyTokens(value)',ctx),'—');
});

test('monthly view is precise, simple and does not label current balance as closing balance',()=>{
  const ctx=context({sample});
  const rendered=vm.runInContext('renderMonthlyStatement(sample)',ctx);
  assert.match(rendered,/¥20,000\.00/);assert.match(rendered,/¥0\.00/);
  assert.match(rendered,/月末余额（元）/);
  assert.doesNotMatch(rendered,/当前剩余余额|¥8,000\.00/);
  assert.doesNotMatch(rendered,/充值方式|平台充值|兑换码ID|模型价格/);
  assert.doesNotMatch(rendered,/余额快照时间|查看统计口径|不代表实际收款流水/);
});

test('unknown auxiliary statistics stay dashes while money, members and download remain available',()=>{
 for(const [requests,tokens] of [[null,100],[10,null],[null,null],[0,0]]){
  const d={...sample,month_opening_balance:'0.000000',member_details_version:2,consumption:{...sample.consumption,requests,tokens},members:[{user_id:1,username:'sample',opening:'0.000000',credit_total:'20000.000000',adjustment:'0.000000',consumption:sample.consumption,closing:'19999.999998'}]};
  const ctx=context({d,list:{rows:[{month:d.month,recharge_count:1,recharge_total:'20000.000000',consumption:d.consumption,downloadable:true}]}});
  const detail=vm.runInContext('renderMonthlyStatement(d)',ctx);
  const metrics=detail.match(/<tr class="bill-metrics">([^]*?)<\/tr>/)[1];
  assert.match(detail,/账号收支明细/);assert.match(detail,/月末余额（元）/);assert.match(metrics,/¥0\.00/);
  const cells=[...metrics.matchAll(/<td[^>]*>([^]*?)<\/td>/g)].map(m=>m[1]);
  assert.equal(cells[1],requests===null?'—':String(requests));
  if(tokens===null)assert.equal(cells[2],'—');else assert.match(cells[2],new RegExp('>'+tokens+'</span>'));
  assert.doesNotMatch(detail,/待核验|日志差额|核对失败/);
  const listing=vm.runInContext('renderMonthlyList(list)',ctx);
  assert.match(listing,/可查看 \/ 下载/);assert.doesNotMatch(listing,/待核验|数据待核验/);
 }
});

test('bill is organization scoped and recharge rows identify their member accounts',()=>{
 const ctx=context({sample:{...sample,account:'客户公司',credits:{...sample.credits,rows:[
  {at:1,amount:'10.000000',username:'member-a',method:'后台充值'},
  {at:2,amount:'20.000000',username:'<member-b>',method:'后台充值'}
 ]}}});
 const output=vm.runInContext('renderMonthlyStatement(sample)',ctx);
 assert.match(output,/客户公司 · 月度消费账单/);
 assert.match(output,/组织名称/);
 assert.match(output,/member-a/);
 assert.match(output,/&lt;member-b&gt;/);
 assert.doesNotMatch(output,/<member-b>/);
});

test('money display rounds every amount to cents without floating point errors',()=>{
 const ctx=context();
 for(const [value,want] of [['35460.175262','¥35,460.18'],['0.005000','¥0.01'],['9.995000','¥10.00'],['-0.000002','¥0.00'],['-0.000832','¥0.00'],['-2.005000','¥-2.01'],['-0.000000','¥0.00'],['9007199254740993.995000','¥9,007,199,254,740,994.00']]){
  ctx.value=value;assert.equal(vm.runInContext('monthlyMoney(value)',ctx),want);
 }
});

test('summary and detail share cents display, never mutating source values',()=>{
 const ctx=context();
 for(const [value,want] of [['35460.175262','¥35,460.18'],['0.004999','¥0.00'],['0.005000','¥0.01'],['9.995000','¥10.00'],['-0.000002','¥0.00'],['-2.005000','¥-2.01'],['-0.000000','¥0.00'],['9007199254740993.995000','¥9,007,199,254,740,994.00']]){
  ctx.value=value;
  assert.equal(vm.runInContext('monthlyMoney(value,true)',ctx),want);
  assert.equal(ctx.value,value);
 }
 for(const value of [null,undefined,'','1','1.2','NaN','<img>','1e6','+1.000000']){
  ctx.value=value;assert.equal(vm.runInContext('monthlyMoney(value,true)',ctx),'待核验');
 }
 const rendered=vm.runInContext('renderMonthlyStatement(sample)',context({sample}));
 assert.doesNotMatch(rendered,/金额按实际精度显示/);
});

test('automatic balance correction is separately shown without changing recharge or consumption',()=>{
 const d={...sample,credits:{...sample.credits,balance_correction:'1.819570'},member_details_version:1,members:[
  {user_id:43,username:'first',opening:'200000.000000',credit_total:'20000.000000',adjustment:'0.000000',balance_correction:'1.819570',consumption:{amount:'100000.000000',refund:'3.000000'},closing:'120004.819570'},
  {user_id:44,username:'second',opening:'200000.000000',credit_total:'20000.000000',adjustment:'0.000000',balance_correction:'0.000000',consumption:{amount:'100000.000000',refund:'3.000000'},closing:'120003.000000'}
 ]};
 const ctx=context({d});
 const output=vm.runInContext('renderMonthlyStatement(d)',ctx);
 assert.match(output,/余额校正（元）/);
 assert.match(output,/¥1\.82/);
 assert.match(output,/不计入充值或消费/);
 assert.match(output,/colspan="8"/);
 assert.match(output,/data-monthly-member="43"[^]*?¥100,000\.00[^]*?¥1\.82[^]*?¥120,004\.82/);
 assert.match(output,/data-monthly-member="44"[^]*?¥100,000\.00[^]*?¥0\.00[^]*?¥120,003\.00/);
 ctx.d={...sample};
 assert.doesNotMatch(vm.runInContext('renderMonthlyStatement(d)',ctx),/余额校正/);
});

test('member view keeps negative and positive accounts separate and escapes names',()=>{
 const members=[
  {user_id:122,username:'<转出>',opening:'800.000000',credit_total:'0.000000',adjustment:'-800.000000',consumption:{amount:'0.000002',refund:'0.000000'},closing:'-0.000002'},
  {user_id:125,username:'转入',opening:'0.000000',credit_total:'800.000000',adjustment:'0.000000',consumption:{amount:'0.000004',refund:'0.000000'},closing:'799.999996'},
  {user_id:126,username:'零活动',opening:'0.000000',credit_total:'0.000000',adjustment:'0.000000',consumption:{amount:'0.000000',refund:'0.000000'},closing:'0.000000'},
 ];
 const ctx=context({d:{member_details_version:1,members}});
 const output=vm.runInContext('renderMonthlyMembers(d)',ctx);
 assert.match(output,/账号收支明细/);
 assert.match(output,/&lt;转出&gt;<small>ID 122/);
 assert.doesNotMatch(output,/<转出>/);
 const transferOut=output.match(/<tr data-monthly-member="122">[^]*?<\/tr>/)[0];
 assert.match(transferOut,/¥-800\.00/);
 assert.doesNotMatch(transferOut,/¥-0\.00/);
 assert.match(transferOut,/class="money">¥0\.00<\/td><\/tr>/);
 assert.match(output,/data-monthly-member="125"[^]*?¥800\.00/);
 assert.match(output,/data-monthly-member="126"/);
 assert.match(output,/各账号独立核算/);
 ctx.d={};assert.equal(vm.runInContext('renderMonthlyMembers(d)',ctx),'');
});

test('rounding is display only, never repairs amounts or sums rounded rows',()=>{
 const d=structuredClone(sample);d.consumption.amount='0.009998';
 d.members=[{user_id:1,consumption:{amount:'0.004999'}},{user_id:2,consumption:{amount:'0.004999'}}];
 const before=JSON.stringify(d),ctx=context({d});
 const output=vm.runInContext('renderMonthlyStatement(d)',ctx);
 assert.match(output,/<tr class="bill-metrics"><td colspan="2">¥0\.01/);
 assert.equal(JSON.stringify(d),before);
 for(const value of ['0.995000','0.004000','0.991000']){
  ctx.value=value;
  assert.match(vm.runInContext('monthlyMoney(value)',ctx),/^¥\d+\.\d{2}$/);
  assert.equal(ctx.value,value);
 }
});

test('missing sources never become zeros and untrusted text is escaped',()=>{
  const ctx=context({sample:{...sample,from:'<script>alert(1)</script>',account:'<img onerror=x>',credits:{available:false,message:'<img onerror=x>'},consumption:null,current_balance:null,month_end_balance:null}});
  const rendered=vm.runInContext('renderMonthlyStatement(sample)',ctx);
  assert.match(rendered,/暂不可用（不代表零充值）/);assert.match(rendered,/待核验/);
  assert.doesNotMatch(rendered,/¥0\.00|<script>|<img/);assert.match(rendered,/&lt;script&gt;/);
});

test('reviewed local bill separates gifts from recharge and excludes current balance',()=>{
 const ctx=context({sample:{...sample,reviewed_local:true,credits:{...sample.credits,rows:[{at:1,amount:'1.000000',method:'注册赠送'},{at:2,amount:'199.000000',method:'后台充值'}],recharge_count:1,recharge_total:'199.000000',gift_total:'1.000000',known_total:'200.000000'}}});
 const output=vm.runInContext('renderMonthlyStatement(sample)',ctx);
 assert.match(output,/额度变动记录/);assert.match(output,/操作类型/);
 assert.match(output,/1 次/);
 assert.match(output,/赠送合计<\/td><td class="money">¥1\.00/);
 assert.match(output,/到账合计<\/td><td class="money">¥200\.00/);
 assert.doesNotMatch(output,/当前剩余余额|¥8,000/);
});

test('adjustments and refunds are visible, not silently folded into recharge',()=>{
  const ctx=context({sample:{...sample,credits:{...sample.credits,rows:[{at:1,amount:''},{at:2,amount:'5.000000'}],known_total:'5.000000',unknown:1,adjustment:'-2.000000'}}});
  const rendered=vm.runInContext('renderMonthlyStatement(sample)',ctx);
  assert.match(rendered,/待核验/);assert.match(rendered,/余额扣减调整/);assert.match(rendered,/¥-2.00/);
});

test('both quota change sides show their affected users without guessing transfers',()=>{
 const ctx=context({sample:{...sample,credits:{...sample.credits,rows:[
  {at:11,user_id:122,username:'<debit>',amount:'-800.000000',method:'余额扣减',kind:'adjustment'},
  {at:12,user_id:125,amount:'800.000000',method:'后台充值',kind:'recharge'}
 ],adjustment:'-800.000000'}}});
 const output=vm.runInContext('renderMonthlyStatement(sample)',ctx);
 assert.match(output,/被操作账号/);
 assert.match(output,/&lt;debit&gt; · ID 122/);
 assert.match(output,/— · ID 125/);
 assert.match(output,/¥-800\.00/);assert.match(output,/¥800\.00/);
 assert.doesNotMatch(output,/内部转账|<debit>/);
});

test('month switch clears old bill and discards an out-of-order response',async()=>{
  const nodes=Object.fromEntries(['monthlyMonth','monthlyContent','monthlyStatus','monthlyDownload','monthlyListBar','monthlyDetailBar','monthlyPager','monthlyPrev','monthlyNext'].map(n=>[n,{value:'2026-09',style:{},textContent:'',innerHTML:'old data'}]));
  const requests=[];
  const ctx=context({$:id=>nodes[id],cstDayStr:()=> '2026-10-19',AbortController,URLSearchParams,setTimeout,clearTimeout,api:(url,opt)=>new Promise(resolve=>requests.push({url,opt,resolve}))});
  vm.runInContext('let monthlySequence=0,monthlyAbort=null,monthlyDownloadAbort=null,monthlyDetail=null;',ctx);
  vm.runInContext(html.match(/^function monthlyBegin\([^]*?^}/m)[0],ctx);
  vm.runInContext(html.match(/^async function loadMonthlyStatement\([^]*?^}/m)[0],ctx);
  const first=vm.runInContext('loadMonthlyStatement("2026-08")',ctx);
  assert.equal(nodes.monthlyContent.innerHTML,'');
  const second=vm.runInContext('loadMonthlyStatement("2026-09")',ctx);
  assert.equal(requests[0].opt.signal.aborted,true);
  assert.equal(requests[1].url,'/api/monthly-statement?month=2026-09');
  assert.equal(requests[1].opt.headers['X-Monthly-Statistics'],'nullable-v1');
  requests[1].resolve({ok:true,data:sample});await second;
  requests[0].resolve({ok:true,data:{...sample,from:'2026-08-01'}});await first;
  assert.match(nodes.monthlyContent.innerHTML,/2026-09-01/);assert.doesNotMatch(nodes.monthlyContent.innerHTML,/2026-08-01/);
  assert.equal(nodes.monthlyDownload.disabled,false);
});

test('list contains per-month summaries and detail actions, null totals are not zero',()=>{
  const ctx=context({d:{rows:[{month:'2026-09',recharge_count:3,recharge_total:'60000.000000',consumption:sample.consumption,downloadable:true},{month:'2026-08',recharge_count:null,recharge_total:null,consumption:null,downloadable:false}]}});
  const output=vm.runInContext('renderMonthlyList(d)',ctx);
  assert.match(output,/充值总额（元）/);assert.match(output,/消费总额（元）/);
  assert.match(output,/data-month-detail="2026-09"/);assert.match(output,/¥60,000.00/);assert.match(output,/数据待核验/);
});

test('list negotiates nullable statistics through the real API helper',async()=>{
 const nodes=Object.fromEntries(['monthlyMonth','monthlyContent','monthlyStatus','monthlyDownload','monthlyListBar','monthlyDetailBar','monthlyPager','monthlyPrev','monthlyNext'].map(n=>[n,{value:'2026-09',style:{},textContent:'',innerHTML:''}]));
 let requested;
 const ctx=context({$:id=>nodes[id],cstDayStr:()=> '2026-10-21',AbortController,URLSearchParams,setTimeout,clearTimeout,fetch:async(url,opt)=>{
  requested={url,opt};return {status:200,json:async()=>({ok:true,data:{rows:[{month:'2026-09',recharge_count:1,recharge_total:'20000.000000',consumption:{...sample.consumption,requests:null,tokens:null},downloadable:true}]}})};
 }});
 vm.runInContext('let monthlySequence=0,monthlyAbort=null,monthlyDownloadAbort=null,monthlyDetail=null,monthlyPrevious=[],monthlyListCursor="",monthlyNextCursor="";',ctx);
 for(const name of ['api','lastClosedMonth','monthlyBegin','loadMonthlyList'])vm.runInContext(html.match(new RegExp(`^(?:async )?function ${name}\\([^]*?^}`,'m'))[0],ctx);
 await vm.runInContext('loadMonthlyList()',ctx);
 assert.equal(requested.url,'/api/monthly-statements?month=2026-09');
 assert.equal(requested.opt.headers.Accept,'application/json');
 assert.equal(requested.opt.headers['X-Monthly-Statistics'],'nullable-v1');
 assert.match(nodes.monthlyContent.innerHTML,/可查看 \/ 下载/);
 assert.equal(nodes.monthlyStatus.textContent,'');
});

test('closed-month maximum uses Beijing month and handles January',()=>{
  const ctx=context({cstDayStr:()=> '2026-01-01'});
  vm.runInContext(html.match(/^function lastClosedMonth\([^]*?^}/m)[0],ctx);
  assert.equal(vm.runInContext('lastClosedMonth()',ctx),'2025-12');
  ctx.cstDayStr=()=> '2026-10-31';assert.equal(vm.runInContext('lastClosedMonth()',ctx),'2026-09');
});

test('one unavailable month keeps other bills usable and does not show false zero amounts',()=>{
 const ctx=context({d:{rows:[{month:'2026-09',recharge_count:1,recharge_total:'10.000000',consumption:sample.consumption,downloadable:true}],unavailable_months:['2026-08','<bad>']}});
 const output=vm.runInContext('renderMonthlyList(d)',ctx);
 assert.match(output,/data-month-detail="2026-09"/);
 assert.match(output,/2026-08、&lt;bad&gt;/);
 assert.doesNotMatch(output,/data-month-detail="2026-08"|<bad>/);
 ctx.d={rows:[],unavailable_months:['2026-08']};
 const empty=vm.runInContext('renderMonthlyList(d)',ctx);
 assert.match(empty,/本页账单暂不可用/);assert.doesNotMatch(empty,/¥0\.00|没有符合条件/);
});

test('verified opening balance is shown, absent legacy opening is not made zero',()=>{
 const ctx=context({sample:{...sample,month_opening_balance:'123.450000'}});
 assert.match(vm.runInContext('renderMonthlyStatement(sample)',ctx),/月初余额（元）<\/td><td class="money">¥123\.45/);
 ctx.sample={...sample};assert.doesNotMatch(vm.runInContext('renderMonthlyStatement(sample)',ctx),/月初余额（元）/);
});

test('month page remains hidden by default and uses a separate date range',()=>{
  assert.match(html,/<!--MONTHLY_STATEMENT_LINK-->/);
  assert.match(html,/id="monthlyView" style="display:none"/);
  const handler=html.match(/^async function loadMonthlyStatement\([^]*?^}/m)[0];
  assert.doesNotMatch(handler,/logQuery|rangeDates|memberUID|token_id/);
});

for(const status of [200,404,409,429,503])test(`download binds the viewed bill version and handles HTTP ${status}`,async()=>{
  const nodes={monthlyDownload:{disabled:false},monthlyStatus:{textContent:''}};
  let requested='',requestOptions,clicks=0,downloadedName;
  const ctx=context({
    $:id=>nodes[id],AbortController,URLSearchParams,
    URL:{createObjectURL:()=> 'blob:synthetic',revokeObjectURL:()=>{}},
    setTimeout:()=>1,clearTimeout:()=>{},location:{},
    document:{body:{appendChild:()=>{}},createElement:()=>({click(){clicks++;downloadedName=this.download;},remove:()=>{}})},
    fetch:async(url,opt)=>{requested=url;requestOptions=opt;return {status,ok:status===200,headers:{get:()=> 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},blob:async()=>({size:10})};},
    sample,
  });
  vm.runInContext('let monthlyDetail=sample,monthlySequence=1,monthlyDownloadAbort=null;',ctx);
  vm.runInContext(html.match(/^async function downloadMonthlyStatement\([^]*?^}/m)[0],ctx);
  await vm.runInContext('downloadMonthlyStatement()',ctx);
  const query=new URL(requested,'http://local.invalid').searchParams;
  assert.equal(query.get('publication'),sample.publication);
  assert.equal(query.get('month'),sample.month);
  assert.equal(requestOptions.headers['X-Monthly-Statistics'],'nullable-v1');
  assert.equal(clicks,status===200?1:0);
  if(status===200)assert.equal(downloadedName,'NexusAPI-sample-月度账单-2026-09.xlsx');
  assert.equal(nodes.monthlyDownload.disabled,status===404||status===409);
  if(status===404||status===409)assert.match(nodes.monthlyStatus.textContent,/变更或撤回/);
  if(status===429)assert.match(nodes.monthlyStatus.textContent,/繁忙/);
});

test('download file names use shared browser/server cases without altering account names',()=>{
 const cases=JSON.parse(readFileSync(new URL('../../monitor/testdata/monthly-bill-filenames.json',import.meta.url),'utf8'));
 const ctx=context();
 for(const c of cases){
  ctx.account=c.account;ctx.month=c.month;
  assert.equal(vm.runInContext('monthlyBillFilename(account,month)',ctx),c.want);
  assert.equal(ctx.account,c.account);
 }
 ctx.account='😀'.repeat(100);ctx.month='2026-09';
 assert.equal(vm.runInContext('monthlyBillFilename(account,month)',ctx),'NexusAPI-'+'😀'.repeat(48)+'-月度账单-2026-09.xlsx');
});

test('summary precedes funding and account details without changing any source amounts',()=>{
 const d={...sample,month_opening_balance:'10.000000',member_details_version:1,members:[{user_id:1,opening:'10.000000',credit_total:'20000.000000',adjustment:'0.000000',consumption:sample.consumption,closing:'20009.999998'}]};
 const before=JSON.stringify(d),ctx=context({d}),rendered=vm.runInContext('renderMonthlyStatement(d)',ctx);
 const labels=['组织名称','本月收支汇总','月初余额（元）','充值合计','消费统计','月末余额（元）','额度变动记录','账号收支明细'];
 for(let i=1;i<labels.length;i++)assert.ok(rendered.indexOf(labels[i-1])<rendered.indexOf(labels[i]),labels[i]);
 assert.equal(JSON.stringify(d),before);
});

test('download refuses an old detail without a publication version',async()=>{
  const nodes={monthlyDownload:{disabled:false},monthlyStatus:{textContent:''}};
  const ctx=context({$:id=>nodes[id],sample:{...sample,publication:undefined},fetch:()=>assert.fail('must not fetch')});
  vm.runInContext('let monthlyDetail=sample,monthlySequence=1;',ctx);
  vm.runInContext(html.match(/^async function downloadMonthlyStatement\([^]*?^}/m)[0],ctx);
  await vm.runInContext('downloadMonthlyStatement()',ctx);
  assert.equal(nodes.monthlyDownload.disabled,true);
  assert.match(nodes.monthlyStatus.textContent,/版本缺失/);
});

test('subscription bill: 总消费 − 退款 = 净消费, subscription shown net of its own refunds',()=>{
 const d={...structuredClone(sample),consumption:{requests:10,tokens:100,amount:'100.000000',refund:'10.000000',net:'90.000000'}};
 const sub={version:1,orders:[],users:[{user_id:1,consume_cents:5000,refund_cents:1000,tokens:[{token_id:7,name:'k',consume_cents:5000,refund_cents:1000}]}]};
 const ctx=context({d,sub});
 const out=vm.runInContext('renderMonthlyStatement(d,sub)',ctx);
 const cell=label=>{const m=new RegExp(label+'</td><td class="money">([^<]+)<').exec(out);return m&&m[1];};
 assert.match(out,/<tr class="bill-metrics"><td colspan="2">¥140\.00</);
 assert.equal(cell('其中 套餐内（元）'),'¥40.00');
 assert.equal(cell('其中 余额支付（元）'),'¥100.00');
 assert.equal(cell('退款金额（元）'),'¥10.00');
 assert.equal(cell('净消费（元）'),'¥130.00');
 assert.doesNotMatch(out,/净消费舍入尾差/);
});
