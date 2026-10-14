import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

test('ordinary log cell exit cannot dereference an absent tooltip source',()=>{
  const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
  const handlers={};
  let closes=0;
  const ctx=vm.createContext({$(){return {addEventListener(name,fn){handlers[name]=fn;}};},logRawTipSource:null,logRawTip:null,scheduleLogRawTipClose(){closes++;}});
  for(const name of ['mouseout','focusout']){
    vm.runInContext(page.match(new RegExp(`^\\$\\('logTb'\\)\\.addEventListener\\('${name}',e=>\\{[^]*?^\\}\\);`,'m'))[0],ctx);
  }
  const ordinary={target:{closest(){return null;}},relatedTarget:{}};
  assert.doesNotThrow(()=>handlers.mouseout(ordinary));
  assert.doesNotThrow(()=>handlers.focusout(ordinary));
  assert.equal(closes,0);
  const child={},source={contains(node){return node===child;}};
  ctx.logRawTipSource=source;
  const event=next=>({target:{closest(){return source;}},relatedTarget:next});
  handlers.mouseout(event(child));assert.equal(closes,0);
  ctx.logRawTip={contains(node){return node===child;}};
  source.contains=()=>false;
  handlers.mouseout(event(child));assert.equal(closes,0);
  handlers.mouseout(event({}));assert.equal(closes,1);
  handlers.focusout(event(null));assert.equal(closes,2);
});

test('list timing follows official rc26: positive FRT only, otherwise neutral N/A',()=>{
  const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
  const ctx=vm.createContext({});
  for(const name of ['timeColorClass','frtVariant']) vm.runInContext(page.match(new RegExp(`^function ${name}[^\\n]+`,'m'))[0],ctx);
  for(const name of ['respTimeVariant','formatLogDuration','timeCell']) vm.runInContext(page.match(new RegExp(`^function ${name}\\([^]*?^}`,'m'))[0],ctx);
  const render=row=>{ctx.row=row;return vm.runInContext('timeCell(row)',ctx);};
  const base={type:2,is_stream:true,use_time:2,first_byte_ms:999};
  for(const value of ['0.25','125.5','1e3']) {
    const html=render({...base,first_byte_evidence:{state:'valid',value}});
    assert.doesNotMatch(html,/未记录|记录异常/);
    assert.ok(html.includes(`title="首字延迟 ${Number(value)} 毫秒"`));
    assert.match(html,/耗时 2.0s/);
  }
  for(const state of ['missing','null']) assert.match(render({...base,first_byte_evidence:{state,value:'999'}}),/首字 不适用/);
  assert.match(render({...base,first_byte_evidence:{state:'invalid'}}),/首字 不适用/);
  for(const value of ['0','',null,'-1','Infinity','<script>']) assert.match(render({...base,first_byte_evidence:{state:'valid',value}}),/首字 不适用/);
  assert.match(render({...base,first_byte_ms:0}),/首字 不适用/);
  assert.doesNotMatch(render(base),/未记录|记录异常/);
  assert.doesNotMatch(render({...base,is_stream:false,first_byte_evidence:{state:'valid',value:'0'}}),/首字/);
  for(const type of [1,3,4,6,7])assert.doesNotMatch(render({...base,type}),/耗时|首字|流式/);
});

test('expanded labels cannot inherit table nowrap or overflow into numeric values',()=>{
  const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
  const rule=selector=>page.match(new RegExp(`\\.${selector}\\{([^}]+)\\}`))[1];
  assert.match(rule('dr-row'),/white-space:normal/);
  assert.match(rule('dr-label'),/white-space:normal/);
  assert.match(rule('dr-label'),/overflow-wrap:anywhere/);
  assert.match(rule('dr-label'),/min-width:0/);
  assert.match(rule('dr-value'),/white-space:normal/);
  assert.match(page,/class="dr-value" style="white-space:pre-line"/);
});

test('narrow usage logs release pinned columns and stack detail values without hiding facts',()=>{
  const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
  const block=page.match(/@media\(max-width:600px\)\{\s*\.logtable[^]*?\n  \}/)[0];
  assert.match(block,/td:first-child/);
  assert.match(block,/td:last-child/);
  assert.match(block,/position:static;box-shadow:none/);
  assert.match(block,/\.log-expand-inner \.dr-row\{grid-template-columns:minmax\(0,1fr\)/);
  assert.match(block,/\.log-expand-inner \.dr-label\{text-align:left\}/);
  assert.doesNotMatch(block,/display:none|visibility:hidden/);
});

test('expanded details follow viewport width without replacing content or fetching data',()=>{
  const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
  const items=[{style:{width:'1200px'},textContent:'original charge'},{style:{width:'1200px'},textContent:'original formula'}];
  const wrap={clientWidth:320,querySelectorAll(selector){assert.equal(selector,'.log-expand-inner');return items;}};
  let current=wrap;
  const ctx=vm.createContext({document:{querySelector(selector){assert.equal(selector,'.logwrap');return current;}}});
  vm.runInContext(page.match(/^function resizeLogDetails\([^]*?^}/m)[0],ctx);
  const run=()=>vm.runInContext('resizeLogDetails()',ctx);
  run();assert.deepEqual(items.map(x=>x.style.width),['320px','320px']);
  wrap.clientWidth=1100;run();assert.deepEqual(items.map(x=>x.style.width),['1100px','1100px']);
  for(const width of [0,-1,NaN,undefined]){wrap.clientWidth=width;run();assert.equal(items[0].style.width,'1100px');}
  current=null;run();
  assert.deepEqual(items.map(x=>x.textContent),['original charge','original formula']);
  assert.match(page,/setTimeout\(\(\)=>\{resizeLogDetails\(\);/);
  assert.match(page,/if\(logs\)\{[^\n]+resizeLogDetails\(\);\}/);
});

test('input help preserves source counts and does not infer cache semantics from request path',()=>{
  const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
  assert.match(page,/保留本条日志的输入 Tokens 原值/);
  assert.match(page,/不能仅凭请求路径判断/);
  assert.doesNotMatch(page,/\/v1\/messages 的输入 tokens 仅统计非缓存输入/);
});

function render(row,entry='buildLogDetailDesc(0)'){
  const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
  const ctx=vm.createContext({logRowsCache:[row],fmtNum:String,DASH:'—'});
  vm.runInContext(page.match(/^const esc=.*$/m)[0],ctx);
  for(const name of ['detailRow','detailRowPre','renderUsageLogEvidence','buildLogDetailDesc','logTokenNumber','logInputCell','logCostUSD','logRecordedToken','logRecordedCost']){
    const fn=page.match(new RegExp(`^function ${name}\\([^]*?^}`, 'm'));
    assert.ok(fn,`${name} missing`);
    vm.runInContext(fn[0],ctx);
  }
  return vm.runInContext(entry,ctx);
}

test('customer details explain original usage and charge even when formula is unavailable',()=>{
  const row={type:2,prompt_tokens:100,completion_tokens:0,cost_usd:0.123456,usage_details:{legacy_formula_unsafe:true,billing:{state:'unsupported',message:'完整公式暂未支持'},items:[]}};
  const html=render(row);
  assert.match(html,/输入 \/ 输出 Tokens（日志原值）/);
  assert.match(html,/100 \/ 0/);assert.match(html,/记录费用（美元）/);assert.match(html,/\$0\.123456/);
  assert.match(html,/完整公式暂未支持/);
  const partial=render({...row,source_facts:{prompt_present:false,completion_present:true,quota_present:false}});
  assert.match(partial,/— \/ 0/);assert.doesNotMatch(partial,/\$0\.123456/);
  const error=render({...row,type:5,detail:'request failed'});
  assert.match(error,/记录费用（美元）/);assert.match(error,/\$0\.123456/);
  assert.match(error,/不能据此判断是否另有消费扣费/);
  assert.match(render({type:3,detail:'adjustment <unsafe>\nsecond line'}),/adjustment &lt;unsafe&gt;\nsecond line/);
});

test('amounts follow rc26 displayable types, without treating refunds as consumption',()=>{
  const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
  const table={innerHTML:''};
  const ctx=vm.createContext({$:()=>table,hideLogRawTip(){},mergeLogFilterOptions(){},fmtLogTime:()=>'',typeTag:String,timeCell:()=>'',logInputCell:()=>'',logTokenNumber:String,DASH:'—'});
  vm.runInContext(page.match(/^const esc=.*$/m)[0],ctx);
  for(const name of ['logCostUSD','logRecordedToken','logRecordedCost','renderLogRows'])vm.runInContext(page.match(new RegExp(`^function ${name}\\([^]*?^}`,'m'))[0],ctx);
  for(const type of [0,1,2,3,4,5,6,7]){
    ctx.rows=[{type,cost_usd:0.123456}];vm.runInContext('renderLogRows(rows)',ctx);
    if([0,2,5,6].includes(type))assert.match(table.innerHTML,/log-cost">\$0\.123456/);
    else assert.match(table.innerHTML,/log-cost">—/);
  }
  ctx.rows=[{type:2,cost_usd:0}];vm.runInContext('renderLogRows(rows)',ctx);
  assert.match(table.innerHTML,/log-cost">\$0\.000000/);
  ctx.rows=[{type:2,cost_usd:0,source_facts:{quota_present:false}}];vm.runInContext('renderLogRows(rows)',ctx);
  assert.match(table.innerHTML,/log-cost">—/);
});

test('subscription and tool markers describe recorded charges without adding them again',()=>{
  const row={type:2,cost_usd:0.15,billing_source:'subscription',has_tool_surcharge:true};
  const before=JSON.stringify(row),html=render(row,'logRecordedCost(logRowsCache[0])');
  assert.match(html,/>订阅</);assert.match(html,/订阅抵扣：\$0.150000/);
  assert.match(html,/工具费 \+/);assert.match(html,/不重复加算/);
  assert.equal(JSON.stringify(row),before);
  const details=render({...row,sub_settlement:'预扣：120 额度\n最终抵扣：100 额度'});
  assert.match(details,/订阅抵扣（额度折美元）/);assert.match(details,/\$0.150000/);
  const refund=render({...row,type:6,billing_source:'',has_tool_surcharge:false});
  assert.match(refund,/退款金额（美元）/);assert.doesNotMatch(refund,/记录费用（美元）/);
});

test('cache-write summary remains exact and diagnostic fields are escaped',()=>{
  const row={type:2,request_id:'local-request',request_path:'/v1/private-admin-path',prompt_tokens:1234,completion_tokens:56,usage_details:{cache_write:{state:'valid',value:'9007199254740993',source:'split_only'}},ip:'192.0.2.1',upstream_request_id:'<bad-id>',source_field_notice:'<notice>'};
  assert.match(render(row,'logInputCell(logRowsCache[0])'),/1,234/);
  assert.match(render(row,'logInputCell(logRowsCache[0])'),/缓存创建小计 9,007,199,254,740,993/);
  const html=render(row);assert.match(html,/Request ID/);assert.match(html,/local-request/);assert.match(html,/192.0.2.1/);
  assert.match(html,/&lt;notice&gt;/);assert.doesNotMatch(html,/上游 Request ID|bad-id|请求路径|private-admin-path|<notice>/);
  const login=render({type:7,detail:'登录成功',request_details:[{label:'登录方式',value:'password',state:'valid'}]});
  assert.match(login,/登录成功/);assert.match(login,/password/);assert.doesNotMatch(login,/记录费用（美元）/);
  for(const type of [1,3,4,7]){
    assert.equal(render({type,prompt_tokens:0},'logRecordedToken(logRowsCache[0],false)'),'—');
    assert.equal(render({type,prompt_tokens:0},'logInputCell(logRowsCache[0])'),'—');
  }
  const page=readFileSync(new URL('../../monitor/portal.html',import.meta.url),'utf8');
  assert.doesNotMatch(page,/费用仅展示消费记录/);
  assert.match(page,/退款不算消费，订阅抵扣不等于钱包扣款/);
});

test('old rows keep existing details, formulas and cache rendering',()=>{
  const html=render({type:2,cache_read_tokens:10,cache_write_tokens:20,billing_process:'legacy formula'});
  assert.match(html,/缓存 Tokens/); assert.match(html,/缓存创建 Tokens/); assert.match(html,/legacy formula/);
});

test('input cache summary uses exact evidence and does not resurrect invalid fallback values',()=>{
  const row={type:2,prompt_tokens:12,cache_read_tokens:999,usage_details:{cache_read:{state:'valid',value:'9007199254740993'}}};
  assert.match(render(row,'logInputCell(logRowsCache[0])'),/9,007,199,254,740,993/);
  row.usage_details.cache_read={state:'invalid'};
  assert.doesNotMatch(render(row,'logInputCell(logRowsCache[0])'),/999|缓存读/);
  delete row.usage_details;
  assert.match(render(row,'logInputCell(logRowsCache[0])'),/缓存读 999/);
});

test('new evidence suppresses unsafe legacy formula and preserves exact numeric text',()=>{
  const row={type:2,cache_write_tokens:90,billing_process:'wrong formula',usage_details:{legacy_formula_unsafe:true,notice:'以实际扣费为准',items:[{label:'缓存创建总量',value:'9007199254740993',state:'valid'},{label:'缓存读取',value:'0',state:'valid'},{label:'未知',value:'未记录',state:'null'}]}};
  const before=JSON.stringify(row),html=render(row);
  assert.doesNotMatch(html,/wrong formula|缓存创建 Tokens/);
  assert.match(html,/9007199254740993/); assert.match(html,/>0<\/span>/); assert.match(html,/未记录/);
  assert.equal(JSON.stringify(row),before);
});

test('all new customer text is escaped and other log types do not show consumption evidence',()=>{
  const details={notice:'<img src=x onerror=alert(1)>',items:[{label:'<script>x</script>',value:'<svg onload=x>',state:'valid'}]};
  const html=render({type:2,usage_details:details});
  assert.doesNotMatch(html,/<img|<svg|<script/); assert.match(html,/&lt;img/); assert.match(html,/&lt;script/);
  assert.doesNotMatch(render({type:6,usage_details:details}),/&lt;img/);
});

test('verified formula and status display together, unverified formula stays hidden',()=>{
  const row={type:2,billing_process:'checked formula',usage_details:{legacy_formula_unsafe:false,billing:{state:'matched',message:'复核一致 <unsafe>'},items:[]}};
  const html=render(row);
  assert.match(html,/checked formula/); assert.match(html,/复核一致 &lt;unsafe&gt;/);
  row.usage_details.legacy_formula_unsafe=true;
  row.usage_details.billing={state:'mismatch',message:'实际扣费未改动'};
  const mismatch=render(row);
  assert.doesNotMatch(mismatch,/checked formula/); assert.match(mismatch,/实际扣费未改动/);
});

test('verified USD explanation is readable by default while technical arithmetic stays collapsed',()=>{
  const row={type:2,prompt_tokens:52696,completion_tokens:1831,cost_usd:0.008396,billing_process:'internal 500000 额度',usage_details:{legacy_formula_unsafe:false,billing:{state:'matched',readable:'输入价格：$2 / 1M tokens\n（输入 216 tokens / 1M tokens × $2）× 分组倍率 0.35 ≈ $0.008396\n实际扣费：$0.008396',message:'按日志复核一致'},items:[{label:'内部倍率',value:'1',state:'valid'}]}};
  const before=JSON.stringify(row),html=render(row),parts=html.split('<details');
  assert.match(parts[0],/计费过程/);assert.match(parts[0],/输入价格：\$2/);assert.match(parts[0],/实际扣费：\$0\.008396/);
  assert.doesNotMatch(parts[0],/500000|额度|内部倍率/);
  assert.match(html,/<details class="log-billing-advanced"><summary>查看详细计费依据（可选）<\/summary>/);
  assert.doesNotMatch(html,/<details[^>]*\bopen\b/);assert.match(parts[1],/internal 500000 额度|内部倍率/);
  assert.equal(JSON.stringify(row),before);
  row.usage_details.legacy_formula_unsafe=true;
  assert.doesNotMatch(render(row),/输入价格：\$2|internal 500000/);
  row.usage_details.legacy_formula_unsafe=false;row.usage_details.billing.state='mismatch';
  assert.doesNotMatch(render(row),/输入价格：\$2|internal 500000/);
});

test('readable billing text is escaped without interpreting HTML or expressions',()=>{
  const html=render({type:2,usage_details:{billing:{state:'matched',readable:'<img src=x onerror=bad>'},items:[]}});
  assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);
});

test('tier prices remain qualified as base prices, never display the old zero-rate formula',()=>{
  const row={type:2,billing_process:'输入 × 0 = $0',usage_details:{legacy_formula_unsafe:true,billing:{state:'unsupported',message:'尚未完成完整费用复核；实际扣费未改动'},items:[{label:'阶梯档位（按日志匹配）',value:'第 1 档',state:'valid'},{label:'阶梯缓存读取基础单价（$/百万 Tokens，未乘分组倍率）',value:'0.2',state:'valid'}]}};
  const html=render(row);
  assert.match(html,/第 1 档/); assert.match(html,/未乘分组倍率/); assert.match(html,/0\.2/);
  assert.match(html,/尚未完成完整费用复核/); assert.doesNotMatch(html,/输入 × 0 = \$0/);
});

test('verified tier formula displays the recorded cache normalization and original charge',()=>{
  const row={type:2,billing_process:'输入含缓存口径\n（计价输入 80 × 2 + 缓存读取 20 × 0.2）÷ 1000000 × 分组倍率 1 × 500000\n实际扣费：82 ÷ 500000 = $0.000164',usage_details:{legacy_formula_unsafe:false,billing:{state:'matched',message:'按日志字段复核一致；不代表完整审计'},items:[]}};
  const before=JSON.stringify(row),html=render(row);
  assert.match(html,/计价输入 80/);assert.match(html,/缓存读取 20/);assert.match(html,/\$0\.000164/);
  assert.match(html,/不代表完整审计/);assert.equal(JSON.stringify(row),before);
});

test('safe stream state appears on consumption and error rows, never treats missing state as success',()=>{
  const details=[{label:'流式状态',value:'未记录，不能判定是否完整结束',state:'missing'},{label:'首字延迟（毫秒，日志原值）',value:'0',state:'valid'}];
  for(const type of [2,5]){
    const row={type,request_details:details};const before=JSON.stringify(row),html=render(row);
    assert.match(html,/不能判定是否完整结束/);assert.match(html,/>0<\/span>/);
    assert.doesNotMatch(html,/正常（日志状态）/);assert.equal(JSON.stringify(row),before);
  }
  for(const type of [1,3,4,6])assert.doesNotMatch(render({type,request_details:details}),/不能判定是否完整结束/);
});

test('request detail text is escaped and historical responses without request_details still render',()=>{
  const html=render({type:5,detail:'legacy error',request_details:[{label:'<script>x</script>',value:'<img src=x onerror=bad>',state:'valid'}]});
  assert.match(html,/legacy error/);assert.match(html,/&lt;script/);assert.doesNotMatch(html,/<script|<img/);
  assert.match(render({type:5,detail:'legacy error'}),/legacy error/);
});

test('media and old/new tool units stay distinct, without adding subtotals to usage',()=>{
  for(const unit of ['$/次','$/千次']){
    const row={type:2,prompt_tokens:100,completion_tokens:20,cost_usd:0.123456,billing_process:'unsafe audio formula',usage_details:{legacy_formula_unsafe:true,billing:{state:'unsupported',message:'暂不推算扣费'},items:[
      {label:'音频输入 Tokens（分项原值）',value:'9007199254740993',state:'valid'},
      {label:'音频输出 Tokens（分项原值）',value:'0',state:'valid'},
      {label:'文本输入 Tokens（分项原值）',value:'未记录',state:'missing'},
      {label:`图片生成单价（${unit}，未乘分组倍率）`,value:'0.04',state:'valid'},
      {label:'多模态分项说明',value:'不能再次累加',state:'valid'}
    ]}};
    const before=JSON.stringify(row),html=render(row);
    assert.ok(html.includes(unit));assert.match(html,/未乘分组倍率|不能再次累加/);
    assert.match(html,/9007199254740993/);assert.match(html,/>0<\/span>/);assert.match(html,/未记录/);
    assert.doesNotMatch(html,/unsafe audio formula/);assert.equal(JSON.stringify(row),before);
  }
});

test('legacy cache formula shows its accounting basis, while ambiguous history keeps only actual charge',()=>{
  const row={type:2,prompt_tokens:100,completion_tokens:10,cost_usd:0.000264,billing_process:'输入含缓存口径：原始输入 100 − 缓存读取 20 − 缓存创建 40 = 普通输入 40\n（普通输入 40 + 缓存读取 20 × 倍率 0.1 + 缓存创建 40 × 倍率 1.25 + 输出 10 × 输出倍率 4）× 模型倍率 1 × 分组倍率 1\n实际扣费：132 ÷ 500000 = $0.000264',usage_details:{legacy_formula_unsafe:false,billing:{state:'matched',message:'与原始扣费一致'},items:[]}};
  const before=JSON.stringify(row),html=render(row);
  assert.match(html,/普通输入 40/);assert.match(html,/\$0\.000264/);assert.equal(JSON.stringify(row),before);
  row.usage_details={legacy_formula_unsafe:true,billing:{state:'unsupported',message:'缓存分项超过输入总量，旧新版本处理不同；费用仍以实际扣费为准'},items:[]};
  const ambiguous=render(row);
  assert.match(ambiguous,/旧新版本处理不同/);assert.doesNotMatch(ambiguous,/普通输入 40/);
});
