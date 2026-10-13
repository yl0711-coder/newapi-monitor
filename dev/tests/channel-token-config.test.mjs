import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

function fixture(){
  const calls=[],elements=new Map();
  const element=id=>{if(!elements.has(id))elements.set(id,{textContent:'',innerHTML:'',classList:{toggle(){}},insertAdjacentHTML(_,html){this.innerHTML=html+this.innerHTML}});return elements.get(id)};
  const f={confirm:true,response:null};
  const context=vm.createContext({window:{confirm:()=>f.confirm},document:{addEventListener(){},getElementById:element},URLSearchParams,AbortController,setTimeout,clearTimeout,location:{},
    fetch:async(url,options)=>{calls.push({url,options});return f.response?await f.response(url,options):{ok:true,status:200,json:async()=>f.config}}});
  const js=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8'),end=js.lastIndexOf('})();');
  vm.runInContext(js.slice(0,end)+'\nglobalThis.api={cm,tokenEditors,canonicalTokenInput,tokenEditorAction,tokenEditorHasPending,tokenReadinessText,renderFinanceChannelRows,financeDialogCanLeave,openFinance,openSiteFinance,closeFinance,showFinanceMessage};\n'+js.slice(end),context);
  f.api=context.api;f.calls=calls;f.element=element;
  f.config={domain:'sub2.example',channel_id:185,provider:'sub2api',account_epoch:'a'.repeat(64),upstream_token_id:'575',revision:1,state:'pending_verification',attribution_verified:false};
  f.api.cm.financeMode='domain';f.api.cm.financeDomain={domain:'sub2.example'};f.api.cm.report={finance:{can_edit:true}};
  const input={value:'',disabled:false},day={value:'2026-08-24',disabled:false},message={textContent:'',classList:{toggle(){}}};
  f.panel={dataset:{cmTokenPanel:'185'},isConnected:true,html:'',
    set innerHTML(html){this.html=html;input.value=html.match(/data-cm-token-id[^>]*value="([^"]*)"/)?.[1]||''},get innerHTML(){return this.html},
    querySelector(s){return s==='[data-cm-token-id]'?input:s==='[data-cm-token-day]'?day:s==='[data-cm-token-message]'?message:null},querySelectorAll(){return[input,day]}};
  f.input=input;f.message=message;f.day=day;
  return f;
}
test('optional IDs stay exact strings and never echo mistaken API keys',()=>{
  const {api}=fixture();
  for(const [raw,want] of [['',''],[' 00575 ','575'],['9007199254740993','9007199254740993'],['9223372036854775807','9223372036854775807']])assert.equal(api.canonicalTokenInput(raw),want);
  for(const raw of ['0','-1','1e3','1.5','sk-secret-never-echo','9223372036854775808'])assert.throws(()=>api.canonicalTokenInput(raw),error=>!error.message.includes('sk-secret'));
});
test('loading is GET only, large ID save is independent of rate settings',async()=>{
  const f=fixture();await f.api.tokenEditorAction(f.panel,'load');
  assert.equal(f.calls[0].options.method,'GET');assert.equal(f.input.value,'575');assert.match(f.panel.innerHTML,/重新读取确认处理状态/);
  f.input.value='9007199254740993';f.config={...f.config,upstream_token_id:f.input.value,revision:2};
  await f.api.tokenEditorAction(f.panel,'save');
  const call=f.calls[1],body=JSON.parse(call.options.body);
  assert.equal(call.url,'/channels/upstream/token-config');assert.equal(body.upstream_token_id,'9007199254740993');assert.equal(body.expected_revision,1);
  assert.deepEqual(Object.keys(body).sort(),['account_epoch','channel_id','domain','expected_revision','upstream_token_id']);
  assert.match(f.message.textContent,/历史账单未重算/);
});
test('unchanged optional value does not send POST; clear requires confirmation',async()=>{
  const f=fixture();await f.api.tokenEditorAction(f.panel,'load');f.input.value='00575';await f.api.tokenEditorAction(f.panel,'save');assert.equal(f.calls.length,1);
  f.input.value='';f.confirm=false;await f.api.tokenEditorAction(f.panel,'save');assert.equal(f.calls.length,1);
  f.confirm=true;f.config={...f.config,upstream_token_id:'',revision:2,state:'unconfigured'};await f.api.tokenEditorAction(f.panel,'save');
  assert.equal(JSON.parse(f.calls[1].options.body).upstream_token_id,'');assert.match(f.panel.innerHTML,/继续使用原统计方式/);
});
test('failed config read cannot silently create empty editable configuration',async()=>{
  const f=fixture();f.response=async()=>({ok:false,status:503,json:async()=>({error:'temporarily unavailable'})});
  await f.api.tokenEditorAction(f.panel,'load');await f.api.tokenEditorAction(f.panel,'save');
  assert.equal(f.calls.length,1);assert.equal(f.api.tokenEditors.get(f.panel).config,null);assert.match(f.panel.innerHTML,/重新读取/);
});
test('conflict and ambiguous POST failure preserve drafts without automatic retry',async()=>{
  for(const status of [409,503]){
    const f=fixture();await f.api.tokenEditorAction(f.panel,'load');f.input.value='574';
    f.response=async()=>({ok:false,status,json:async()=>({error:'unavailable'})});
    await f.api.tokenEditorAction(f.panel,'save');assert.equal(f.calls.length,2);assert.equal(f.input.value,'574');assert.equal(f.api.tokenEditors.get(f.panel).config.revision,1);assert.match(f.message.textContent,/不会自动重试保存/);
  }
});
test('late responses cannot overwrite a different domain or detached editor',async()=>{
  for(const change of [f=>f.api.cm.financeDomain={domain:'other.example'},f=>f.panel.isConnected=false]){
    const f=fixture();let done;f.response=()=>new Promise(resolve=>done=resolve);
    const pending=f.api.tokenEditorAction(f.panel,'load');change(f);done({ok:true,status:200,json:async()=>f.config});await pending;
    assert.equal(f.api.tokenEditors.get(f.panel).config,null);
  }
});
test('double save sends once; unresolved token edits block main rate save',async()=>{
  const f=fixture();await f.api.tokenEditorAction(f.panel,'load');f.input.value='574';assert.equal(f.api.tokenEditorHasPending(false),true);
  let done;f.response=()=>new Promise(resolve=>done=resolve);
  const pending=f.api.tokenEditorAction(f.panel,'save');await f.api.tokenEditorAction(f.panel,'save');assert.equal(f.calls.length,2);assert.equal(f.api.tokenEditorHasPending(true),true);
  done({ok:true,status:200,json:async()=>({...f.config,upstream_token_id:'574',revision:2})});await pending;assert.equal(f.api.tokenEditorHasPending(false),false);
});
test('check reads one selected CST day without saving; unsaved edits block check',async()=>{
  const f=fixture();await f.api.tokenEditorAction(f.panel,'load');f.input.value='574';await f.api.tokenEditorAction(f.panel,'check');assert.equal(f.calls.length,1);
  await f.api.tokenEditorAction(f.panel,'discard');f.response=async()=>({ok:true,status:200,json:async()=>({status:'evidence_ready',ready_hours:24,hours:Array(24).fill({}),rounding_hours:1})});
  await f.api.tokenEditorAction(f.panel,'check');const call=f.calls[1];assert.equal(call.options.method,'GET');assert.match(call.url,/token-check/);assert.equal(new URL('http://localhost'+call.url).searchParams.get('day_ts'),String(Date.parse('2026-08-24T00:00:00+08:00')/1000));assert.match(f.message.textContent,/不是渠道归属确认/);assert.match(f.message.textContent,/舍入差额/);
});
test('shared token warning and unsupported provider never promise activation',async()=>{
  const f=fixture();f.config={...f.config,shared_channel_ids:[185,186],state:'unsupported',provider:'aicodewith'};await f.api.tokenEditorAction(f.panel,'load');
  assert.match(f.panel.innerHTML,/#185、#186/);assert.match(f.panel.innerHTML,/不得重复计入消费/);assert.doesNotMatch(f.panel.innerHTML,/data-cm-token-action="check"/);
});
test('binding processing states never imply complete money or trigger work',async()=>{
  for(const [status,label] of [['pending','等待核验当前配置'],['scheduled','等待生效时间'],['active','当前令牌归属已生效'],['shared','按共享来源归属'],['disabled','未开启'],['unavailable','状态暂不可用']]){
    const f=fixture();f.config.processing={status,effective_at:1787702400,check_status:'retry'};
    await f.api.tokenEditorAction(f.panel,'load');
    assert.match(f.panel.innerHTML,new RegExp(label));assert.match(f.panel.innerHTML,/限频重试/);
    if(['scheduled','active','shared'].includes(status))assert.match(f.panel.innerHTML,/不代表账单数据已补齐/);
    assert.equal(f.calls.length,1);assert.equal(f.calls[0].options.method,'GET');
    assert.doesNotMatch(f.panel.innerHTML,/归属尚未生效|当前未启用拆分/);
  }
});
test('unknown status is safe text and evidence success does not disable active binding',async()=>{
  const f=fixture();f.config.processing={status:'<img src=x onerror=alert(1)>',effective_at:'<script>'};
  await f.api.tokenEditorAction(f.panel,'load');
  assert.doesNotMatch(f.panel.innerHTML,/<img|<script>/);assert.match(f.panel.innerHTML,/重新读取确认处理状态/);
  assert.doesNotMatch(f.api.tokenReadinessText({status:'evidence_ready'}),/尚未启用/);
});
test('account recovery is separate from binding and never promises complete costs',async()=>{
  for(const [status,label] of [['pending','等待后台'],['disabled','未开启自动补扫'],['running','后台正在处理'],['reconnect','请在账户配置重新授权'],['retry','限频重试'],['paging','分页补扫中'],['waiting_second_scan','第二轮独立核验'],['waiting_legacy','原账单采集'],['observed','不代表全部历史或金额已补齐'],['unavailable','状态暂不可用']]){
    const f=fixture();f.config.processing={status:'active',recovery:{status,day_ts:1787500800,retry_at:1787702400}};
    await f.api.tokenEditorAction(f.panel,'load');
    assert.match(f.panel.innerHTML,/当前令牌归属已生效/);assert.match(f.panel.innerHTML,/账户令牌证据补扫/);assert.match(f.panel.innerHTML,new RegExp(label));
    assert.match(f.panel.innerHTML,/北京时间/);
    if(['retry','paging','waiting_second_scan','waiting_legacy'].includes(status))assert.match(f.panel.innerHTML,/以后台调度为准/);
    else assert.doesNotMatch(f.panel.innerHTML,/最早继续检查/);
    assert.equal(f.calls.length,1);assert.equal(f.calls[0].options.method,'GET');
  }
});
test('recovery unknown status and unsafe dates stay sanitized',async()=>{
  const f=fixture();f.config.processing={status:'active',recovery:{status:'<img src=x>',day_ts:'<script>',retry_at:Number.MAX_SAFE_INTEGER}};
  await f.api.tokenEditorAction(f.panel,'load');assert.doesNotMatch(f.panel.innerHTML,/<img|<script>|Invalid Date|最早继续检查|对应日期/);assert.match(f.panel.innerHTML,/状态暂不可用/);
});
test('bad save response never replaces verified configuration scope',async()=>{
  const f=fixture();await f.api.tokenEditorAction(f.panel,'load');f.input.value='574';f.config={...f.config,domain:'other.example'};
  await f.api.tokenEditorAction(f.panel,'save');assert.equal(f.api.tokenEditors.get(f.panel).config.domain,'sub2.example');assert.match(f.message.textContent,/返回配置不完整/);
});
test('row renders optional isolated panel and escapes upstream channel labels',()=>{
  const f=fixture();f.api.cm.financeChannels=[{id:185,name:'<img onerror=alert(1)>',current:true,status:1,groups:[],configured_groups:[]}];f.api.renderFinanceChannelRows();
  const html=f.element('cmFinanceDomainChannelRows').innerHTML;
  assert.match(html,/data-cm-token-panel="185" hidden/);assert.match(html,/上游令牌 ID（可选）/);assert.doesNotMatch(html,/<img/);assert.match(html,/data-cm-domain-rate="multiplier"/);
});
test('rate submission blocks token actions, dialog close and scope switches',async()=>{
  const f=fixture();await f.api.tokenEditorAction(f.panel,'load');
  f.api.cm.report.domains=[{key:'other',domain:'other.example',configured:true}];
  f.element('cmFinanceSave').disabled=true;f.input.value='574';
  await f.api.tokenEditorAction(f.panel,'save');
  f.api.closeFinance();f.api.openFinance('other');f.api.openSiteFinance();
  assert.equal(f.calls.length,1);assert.equal(f.api.cm.financeDomain.domain,'sub2.example');assert.equal(f.api.cm.financeMode,'domain');
  assert.equal(f.api.tokenEditors.get(f.panel).config.upstream_token_id,'575');
  f.element('cmFinanceSave').disabled=false;f.confirm=false;
  f.api.openFinance('other');f.api.openSiteFinance();
  assert.equal(f.api.cm.financeDomain.domain,'sub2.example');assert.equal(f.input.value,'574');
  f.confirm=true;assert.equal(f.api.financeDialogCanLeave(),true);
});
test('resolved token drafts clear only their own footer notice',async()=>{
  for(const action of ['save','discard','load']){
    const f=fixture();await f.api.tokenEditorAction(f.panel,'load');f.input.value='574';
    f.api.tokenEditorHasPending(false);assert.match(f.element('cmFinanceMessage').textContent,/请先单独保存/);
    if(action==='save')f.config={...f.config,upstream_token_id:'574',revision:2};
    await f.api.tokenEditorAction(f.panel,action);assert.equal(f.element('cmFinanceMessage').textContent,'');
    f.api.showFinanceMessage('倍率保存失败，请核对配置',true);
    await f.api.tokenEditorAction(f.panel,'discard');assert.equal(f.element('cmFinanceMessage').textContent,'倍率保存失败，请核对配置');
  }
});
test('unresolved token failure keeps its footer guard until input is resolved',async()=>{
  const f=fixture();await f.api.tokenEditorAction(f.panel,'load');f.input.value='574';
  f.api.tokenEditorHasPending(false);f.response=async()=>({ok:false,status:409,json:async()=>({error:'conflict'})});
  await f.api.tokenEditorAction(f.panel,'save');assert.match(f.element('cmFinanceMessage').textContent,/请先单独保存/);
  f.input.value='00575';await f.api.tokenEditorAction(f.panel,'save');assert.equal(f.element('cmFinanceMessage').textContent,'');
});
