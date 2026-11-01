import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
test('monthly summary has white content rows and restrained consistent emphasis',()=>{
 const css=source.match(/<style>([^]*?)<\/style>/)[1];
 for(const selector of ['.bill-label','.bill-total td','.bill-balance td']){
  const escaped=selector.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
  const rule=css.match(new RegExp('\\.bill-sheet '+escaped+'\\{([^}]+)\\}'))?.[1];
  assert.ok(rule,selector);
  assert.match(rule,/background:white/);
 }
 for(const selector of ['bill-metrics td','bill-balance td:last-child']){
  const rule=css.split('.bill-sheet .'+selector+'{')[1]?.split('}')[0];
  assert.match(rule,/font-size:18px/);
 }
 assert.match(css,/\.bill-sheet \.bill-section\{background:#f1f5f9;/);
});
function setup(members){
 const root={contains:n=>n.attached};
 const ctx=vm.createContext({esc:v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;'),$:()=>root});
 for(const name of ['monthlyMoney','monthlyDisplayUnits','monthlyDisplayCentsMoney','monthlyDisplayResidual','monthlyDisplayNonzero','monthlyNetDisplayResidual','monthlyDisplayNetColumnResidual','monthlyWalletDisplayResidual','monthlyWalletDisplayClosing','monthlyDisplayWallet','monthlyMemberDisplayProjection','renderMonthlyMembers','renderMonthlyTokenRows','expandMonthlyTokens']){
  vm.runInContext(source.match(new RegExp(`^function ${name}\\([^]*?^}`,'m'))[0],ctx);
 }
 ctx.monthlyDetail={member_details_version:1,token_details_version:1,credits:{},members};
 return ctx;
}
function member(user_id=201){
 return {user_id,username:'<测试账号>',opening:'10.000000',credit_total:'0.000000',adjustment:'0.000000',closing:'8.999998',consumption:{amount:'1.000002',refund:'0.000000',net:'1.000002'},token_consumption:[{token_id:11,name:'<同名令牌>',amount:'1.000002',refund:'0.000000',net:'1.000002'}]};
}
function node(index){
 const body={innerHTML:''};let writes=0;
 Object.defineProperty(body,'innerHTML',{get(){return this.value||'';},set(v){this.value=v;writes++;}});
 return {attached:true,open:true,dataset:{monthlyTokenMember:String(index)},matches:()=>true,querySelector:()=>body,body,writes:()=>writes};
}

test('initial rendering opens every account and immediately includes its token rows',()=>{
 const members=Array.from({length:100},(_,i)=>member(i+1));
 const before=JSON.stringify(members);
 const ctx=setup(members);
 const html=vm.runInContext('renderMonthlyMembers(monthlyDetail)',ctx);
 assert.equal([...html.matchAll(/data-monthly-member=/g)].length,100);
 assert.equal([...html.matchAll(/data-monthly-token-member=/g)].length,100);
 assert.equal([...html.matchAll(/<details[^>]*\bopen\b/g)].length,100);
 assert.equal([...html.matchAll(/data-token-loaded="1"/g)].length,100);
 assert.equal([...html.matchAll(/data-monthly-token-user=/g)].length,100);
 assert.doesNotMatch(html,/<测试账号>|<同名令牌>/);
 assert.equal(JSON.stringify(members),before);
});

test('default open toggle never rerenders the already populated token body',()=>{
 const ctx=setup([member()]),target=node(0);
 target.dataset.tokenLoaded='1';ctx.event={target};
 vm.runInContext('expandMonthlyTokens(event)',ctx);
 target.open=false;vm.runInContext('expandMonthlyTokens(event)',ctx);
 target.open=true;vm.runInContext('expandMonthlyTokens(event)',ctx);
 assert.equal(target.writes(),0);
});

test('legacy bills without token evidence never invent or traverse token rows',()=>{
 const m=member();Object.defineProperty(m,'token_consumption',{get(){throw Error('legacy token rows read');}});
 const ctx=setup([m]);ctx.monthlyDetail.token_details_version=0;
 assert.doesNotMatch(vm.runInContext('renderMonthlyMembers(monthlyDetail)',ctx),/bill-token-member|用户与令牌消费明细/);
});

test('opening renders only the selected account once, preserving precision and escaping names',()=>{
 const members=[member(201),member(202)],before=JSON.stringify(members),ctx=setup(members);
 const first=node(0);ctx.event={target:first};
 vm.runInContext('expandMonthlyTokens(event)',ctx);
 assert.equal(first.writes(),1);
 assert.match(first.body.innerHTML,/data-monthly-token-user="201"/);
 assert.doesNotMatch(first.body.innerHTML,/data-monthly-token-user="202"|<同名令牌>/);
 assert.match(first.body.innerHTML,/&lt;同名令牌&gt;/);
 assert.match(first.body.innerHTML,/¥1\.00/);
 assert.doesNotMatch(first.body.innerHTML,/¥[\d,]+\.\d{3}/);
 first.open=false;vm.runInContext('expandMonthlyTokens(event)',ctx);
 first.open=true;vm.runInContext('expandMonthlyTokens(event)',ctx);
 assert.equal(first.writes(),1);
 const second=node(1);ctx.event={target:second};vm.runInContext('expandMonthlyTokens(event)',ctx);
 assert.match(second.body.innerHTML,/data-monthly-token-user="202"/);
 assert.equal(JSON.stringify(members),before);
});

test('detached old month, closing events, missing details and invalid selection never render',()=>{
 for(const mutate of [n=>{n.attached=false;},n=>{n.open=false;},n=>{n.matches=()=>false;},n=>{n.dataset.monthlyTokenMember='-1';},n=>{n.dataset.monthlyTokenMember='100';},n=>{n.dataset.monthlyTokenMember='not-an-index';},n=>{n.querySelector=()=>null;}]){
  const ctx=setup([member()]),target=node(0);mutate(target);ctx.event={target};
  vm.runInContext('expandMonthlyTokens(event)',ctx);assert.equal(target.writes(),0);
 }
 const ctx=setup([member()]),target=node(0);ctx.event={target};ctx.monthlyDetail=null;
 vm.runInContext('expandMonthlyTokens(event)',ctx);assert.equal(target.writes(),0);
});

test('default open full-bill rendering retains 10000 token rows without truncation or mutation',t=>{
 const m=member();
 m.token_consumption=Array.from({length:10000},(_,i)=>({token_id:i+1,name:'token-'+(i+1),amount:'0.000002',refund:'0.000000',net:'0.000002'}));
 m.consumption={amount:'0.020000',refund:'0.000000',net:'0.020000'};
 const before=JSON.stringify(m),ctx=setup([m]);
 const start=performance.now(),rendered=vm.runInContext('renderMonthlyMembers(monthlyDetail)',ctx);
 assert.equal([...rendered.matchAll(/data-monthly-token-user=/g)].length,10000);
 assert.match(rendered,/data-monthly-token="10000"/);
 assert.match(rendered,/<details[^>]*\bopen\b/);
 assert.equal(JSON.stringify(m),before);
 t.diagnostic(`10000 default-open token rows rendered in ${(performance.now()-start).toFixed(1)} ms (Node, not browser DOM timing)`);
});

test('real toggle listener uses capture because native details toggle does not bubble',()=>{
 assert.match(source,/\$\('monthlyContent'\)\.addEventListener\('toggle',expandMonthlyTokens,true\)/);
});
