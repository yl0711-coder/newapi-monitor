// Offline preview uses the exact production renderer; optional reviewed XLSX.
// node dev/preview-monthly-statement.mjs /absolute/path/preview.html [dataset.json] [bill.xlsx]
import {readFileSync,writeFileSync} from 'node:fs';
const source=readFileSync(new URL('../monitor/portal.html',import.meta.url),'utf8');
const css=source.match(/<style>([^]*?)<\/style>/)[1];
let section=source.match(/<section id="monthlyView"[^]*?<\/section>/)[0].replace('style="display:none"','style="display:block"');
let code=source.slice(source.indexOf('// Monthly statements have'),source.indexOf('// 日志时间固定按'));
const at=value=>Date.parse(value+'+08:00')/1000;
let fixtures={};
for(const [i,month] of ['2026-09','2026-08','2026-07','2026-06','2026-05','2026-04'].entries()){
  const next=new Date(Date.UTC(2026,Number(month.slice(5)),1));
  const last=new Date(next.getTime()-86400000).toISOString().slice(0,10);
  const credit=60000-i*3000,cost=52000-i*3000;
  fixtures[month]={month,from:month+'-01',to:last,as_of:at('2026-10-19T17:00:00'),current_month:false,
    credits:{available:true,rows:[3,9,14].map((day,n)=>({at:at(`${month}-${String(day).padStart(2,'0')}T10:00:00`),amount:(n===2?credit-40000:20000).toFixed(6)})),known_total:credit.toFixed(6),unknown:0,adjustment:'0.000000',message:'按保留日志统计到账额度，不区分充值方式，不代表实际收款流水。'},
    consumption:{requests:142850-i*10000,tokens:721100000-i*10000000,amount:cost.toFixed(6),refund:'0.000000',net:cost.toFixed(6)},consumption_through:at(next.toISOString().slice(0,10)+'T00:00:00'),consumption_message:'',
    current_balance:'8000.000000',balance_as_of:at('2026-10-19T17:00:00'),balance_message:'当前余额不是所选月份的月末余额；历史月末余额暂无可靠快照。'};
}
const dataset=process.argv[3]?JSON.parse(readFileSync(process.argv[3],'utf8')):null;
const generationPreview=process.env.MONTHLY_GENERATION_PREVIEW==='1';
if(generationPreview&&dataset)throw new Error('Generation preview accepts synthetic default data only');
const xlsx=process.argv[4]?readFileSync(process.argv[4]).toString('base64'):null;
if(dataset){
  fixtures=dataset.months;
  for(const d of Object.values(fixtures)){d.account=dataset.account;d.published=!!xlsx;d.publication='a'.repeat(64);}
}
if(xlsx){
  if(Object.keys(fixtures).length!==1)throw new Error('One reviewed workbook must correspond to one month');
}
const referenceDate=dataset?dataset.captured_at.slice(0,10):'2026-10-19';
const latestMonth=Object.keys(fixtures).sort().at(-1)||'';
const banner=dataset?(dataset.synthetic?'本机交互预览 · 全部为合成数据，不是实际客户账单':`${dataset.account} · 线上数据本地预览 · 取数时间 ${dataset.captured_at}`):'本机交互预览 · 全部为合成数据，不是实际客户账单 · 未附 Excel 时不可下载';
const htmlEscape=v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
const mocks=`
const fixtures=${JSON.stringify(fixtures).replaceAll('<','\\u003c')};
const realXLSX=${JSON.stringify(xlsx)};
const $=id=>document.getElementById(id);
const esc=v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const fmtNum=n=>Number(n).toLocaleString('en-US');
const fmtLogTime=ts=>new Date(ts*1000+8*3600000).toISOString().slice(0,19).replace('T',' ');
const cstDayStr=()=> ${JSON.stringify(referenceDate)};
async function api(path){
  const url=new URL(path,'https://preview.invalid'),month=url.searchParams.get('month');
  if(month&&month>=cstDayStr().slice(0,7))return {ok:false,error:'只展示已结束月份'};
  if(url.pathname==='/api/monthly-statement')return fixtures[month]?{ok:true,data:fixtures[month]}:{ok:false,error:'本地预览没有这个月份的数据'};
  const before=url.searchParams.get('before')||${JSON.stringify(latestMonth)};
  const months=Object.keys(fixtures).sort().reverse().filter(m=>month?m===month:m<=before),page=months.slice(0,3);
  return {ok:true,data:{next:months.length>3?months[3]:'',rows:page.map(month=>{const d=fixtures[month];return {month,recharge_count:d.credits.available?(d.credits.recharge_count??d.credits.rows.length):null,recharge_total:d.credits.available&&!d.credits.unknown?(d.credits.recharge_total??d.credits.known_total):null,gift_total:d.credits.gift_total,credit_total:d.credits.known_total,consumption:d.consumption,money_decimal_places:monthlySummaryPrecision(d),downloadable:!!realXLSX&&d.credits.available&&!d.credits.unknown&&!!d.consumption};})}};
}
window.fetch=async path=>{
  const u=new URL(path,'https://preview.invalid'),d=fixtures[u.searchParams.get('month')];
  if(u.pathname!=='/api/monthly-statement/download'||!d)return new Response('',{status:404});
  if(u.searchParams.get('publication')!==d.publication)return new Response('账单版本已变更',{status:409});
  if(!d.credits.available||d.credits.unknown||!d.consumption)return new Response('数据待核验，暂不能导出',{status:422});
  if(realXLSX)return new Response(Uint8Array.from(atob(realXLSX),c=>c.charCodeAt(0)),{headers:{'content-type':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}});
  return new Response('未附已核验 Excel',{status:422});
};
`;
const output=process.argv[2]||'/private/tmp/usage-monthly-statement-preview.html';
const generationMocks=generationPreview?`
const downloadFetch=window.fetch, generationStates=new Map();
window.fetch=async (path,options={})=>{
  const url=new URL(path,'http://preview.invalid');
  if(url.pathname!=='/api/monthly-statement/generation')return downloadFetch(path,options);
  const month=options.method==='POST'?JSON.parse(options.body).month:url.searchParams.get('month');
  if(options.method==='POST')generationStates.set(month,Date.now());
  const started=generationStates.get(month);
  const state=!started?'not_requested':Date.now()-started<4000?'queued':Date.now()-started<9000?'processing':'pending_verification';
  return new Response(JSON.stringify({month,state}),{status:options.method==='POST'?202:200,headers:{'Content-Type':'application/json'}});
};
$('monthlyMonth').value=lastClosedMonth();
`:'';
const generationJS=generationPreview?readFileSync(new URL('../monitor/portal_monthly_generation_local.js',import.meta.url),'utf8'):'';
writeFileSync(output,`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Usage 月度账单列表本机预览</title><style>${css}body{max-width:1300px;margin:0 auto;padding:24px}</style><p style="color:#9a6700">${htmlEscape(banner)}${generationPreview?' · 生成进度为模拟接口，不执行真实制单':''}</p><div class="vnav" style="margin-bottom:20px"><a>用量总览</a><a>使用日志</a><a class="on">月度账单</a></div>${section}<script>${mocks}\n${code}\n${generationMocks}\nloadMonthlyList();\n${generationJS}</script></html>`);
console.log(output);
