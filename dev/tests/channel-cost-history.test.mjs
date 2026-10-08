import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const domain={domain:'directory.example',key:'domain:directory.example',vendors:[{channels:[{id:59,name:'current',current:true}]}]};
const sourceRef='a'.repeat(64);
const directory=[{id:59,name:'current',current:true},{id:64,name:'internal-only',current:true},{id:69,name:'retired',current:false}];
const sourceRow={source_ref:sourceRef,source_ref_kind:'newapi_token_id',hmac_key_id:'test-key',historical_binding_count:0,
  source_groups:['group'],upstream_models:['model'],dimension_count:1,requests:1,first_hour:3600,last_hour:3600};

// Execute the actual module with only its unrelated full-page paint replaced.
// The select readers, renderers, preview, confirmation and request builders are
// production functions; this fixture never permits real network calls.
function fixture({match=64,shared=false,confirm=true,fetchOverride,site=domain,sourcePayload}={}){
  const calls=[],alerts=[],prompts=[],confirmations=[];
  const fields=new Map([
    ['[data-cost-mode]',{value:'unallocated'}],
    ['[data-cost-channel]',{value:'0'}],
    ['[data-cost-history-channel]',{value:'69'}],
  ]);
  const row={querySelector:selector=>fields.get(selector)};
  const buttons={history:{disabled:false,closest:()=>row},future:{disabled:false,closest:()=>row}};
  const document={addEventListener(){},getElementById(){return null},querySelector:selector=>selector.includes('history-binding')?buttons.history:buttons.future};
  const apiData=sourcePayload||{account_epoch:'e'.repeat(64),sources:[structuredClone(sourceRow)],historical_channels:structuredClone(directory)};
  const cstInput=ts=>new Date((ts+8*3600)*1000).toISOString().slice(0,16);
  fields.set('[data-cost-history-from]',{value:cstInput(apiData.sources[0].first_hour)});
  fields.set('[data-cost-history-to]',{value:cstInput(apiData.sources[0].last_hour+3600)});
  const fetchImpl=async(url,options={})=>{
    calls.push({url,...options});
    if(fetchOverride){const overridden=await fetchOverride(url,options);if(overridden)return overridden}
    if(url.startsWith('/channels/cost/sources?'))return response(apiData);
    if(url.startsWith('/channels/cost/proposals?'))return response({proposals:[],versions:[]});
    if(url==='/channels/cost/historical-bindings/preview')return response({binding:{ValidFrom:3600,ValidTo:7200},channel_name:'retired',
      evidence_hours:1,evidence_requests:1,has_local_activity:true,local_active_hours:1,local_requests:1,
      temporal_overlap_quality:'strong',activity_coverage:1,cost_activity_coverage:1,
      evidence_billed_cost:{micro_usd:'1250000',display:'$1.250000'},active_billed_cost:{micro_usd:'1250000',display:'$1.250000'},
      inactive_billed_cost:{micro_usd:'0',display:'$0.000000'}});
    if(url==='/channels/cost/historical-bindings')return response({ok:true,evidence_hours:1});
    if(url==='/channels/cost/bindings')return response({ok:true});
    throw new Error('unexpected request '+url);
  };
  const context=vm.createContext({document,window:{prompt:message=>{prompts.push(message);return 'test-only audit'},confirm:message=>{confirmations.push(message);return confirm}},
    alert:message=>alerts.push(message),CSS:{escape:value=>value},location:{},URLSearchParams,AbortController,fetch:fetchImpl});
  const js=readFileSync(new URL('../../monitor/channel_management.js',import.meta.url),'utf8'),end=js.lastIndexOf('})();');
  assert.ok(end>0);
  vm.runInContext(js.slice(0,end)+'\nrender=()=>{};globalThis.testAPI={cm,costSourceRows,costHistoricalChannels,costHistoryHourInput,parseCostHistoryHour,saveCostBinding,saveHistoricalCostBinding,loadCostLedger};\n'+js.slice(end),context);
  const api=context.testAPI;
  const data={accountEpoch:apiData.account_epoch,sources:apiData.sources,historicalChannels:apiData.historical_channels,
    ownership:{exact_unique:shared?0:1,exact_shared:shared?1:0,matches:[{source_ref:apiData.sources[0].source_ref,state:shared?'exact_shared':'exact_unique',candidates:[{channel_id:match,name:'candidate'}]}]}};
  api.cm.report={domains:[site],finance:{can_edit:true},cost_closure:{enabled:true,domains:[site.domain]}};
  api.cm.costLedger.set(site.key,data);
  return {api,data,apiData,fields,buttons,calls,alerts,prompts,confirmations};
}

const response=(data,status=200)=>({ok:status>=200&&status<300,status,json:async()=>data});
const selectHTML=(html,attribute)=>html.match(new RegExp(`<select ${attribute}[^>]*>([^]*?)</select>`))?.[1]||'';
const writes=calls=>calls.filter(call=>call.method==='POST');

test('hidden current and deleted channels are available only in the independent history selector',()=>{
  const {api,data,calls}=fixture(),before=JSON.stringify(data);
  const html=api.costSourceRows(domain,data),future=selectHTML(html,'data-cost-channel'),history=selectHTML(html,'data-cost-history-channel');
  assert.match(future,/value="59"/);assert.doesNotMatch(future,/value="64"|value="69"/);
  assert.match(history,/value="64" selected/);assert.match(history,/value="69"[^]*?已删除，仅历史/);
  assert.match(html,/已作为历史回填候选，未来配置保持不变/);
  assert.match(html,/value="unallocated" selected/);
  assert.equal(JSON.stringify(data),before);assert.equal(calls.length,0);
});

test('a deleted exact candidate can be prefilled historically without making it a future option',()=>{
  const {api,data}=fixture({match:69});
  const html=api.costSourceRows(domain,data);
  assert.match(selectHTML(html,'data-cost-history-channel'),/value="69" selected/);
  assert.doesNotMatch(selectHTML(html,'data-cost-channel'),/value="69"/);
});

test('unknown/shared candidates never create or preselect historical options; names are escaped',()=>{
  for(const options of [{match:999},{shared:true}]){
    const {api,data}=fixture(options);
    data.historicalChannels.push(null,{id:'bad',name:'invalid'},{id:80,name:'</option><script>bad</script>',current:false});
    const html=api.costSourceRows(domain,data),history=selectHTML(html,'data-cost-history-channel');
    assert.doesNotMatch(history,/selected|value="999"|value="NaN"|<script>/);
    assert.match(history,/&lt;script&gt;/);
  }
});

test('existing binding is not overwritten by a differing exact candidate',()=>{
  const {api,data}=fixture();
  data.sources[0].current_binding={LocalChannelID:59,AllocationMode:'allocated'};
  const html=api.costSourceRows(domain,data);
  assert.match(selectHTML(html,'data-cost-history-channel'),/value="59" selected/);
  assert.doesNotMatch(selectHTML(html,'data-cost-history-channel'),/value="64" selected/);
});

test('older, empty and truncated directories are explicit and never imply complete options',()=>{
  const {api,data}=fixture();
  delete data.historicalChannels;
  let html=api.costSourceRows(domain,data);
  assert.match(html,/历史渠道目录暂不可用/);
  assert.doesNotMatch(selectHTML(html,'data-cost-history-channel'),/value="64"|value="69"/);
  data.historicalChannels=[];
  html=api.costSourceRows(domain,data);
  assert.match(html,/data-cm-cost-history-binding[^>]*disabled/);
  data.historicalChannels=directory;data.historicalChannelsTruncated=true;
  assert.match(api.costSourceRows(domain,data),/历史渠道目录超过展示上限/);
});

test('loading the ledger retains its independent directory and does not write',async()=>{
  const {api,data,calls}=fixture();
  await api.loadCostLedger(domain.key);
  const loaded=api.cm.costLedger.get(domain.key);
  assert.equal(JSON.stringify(loaded.historicalChannels),JSON.stringify(directory));
  assert.equal(loaded.ownership,data.ownership);assert.equal(writes(calls).length,0);
});

test('history submission uses only its own selection even while future mode is unallocated',async()=>{
  const {api,fields,calls,confirmations,alerts}=fixture();
  await api.saveHistoricalCostBinding(domain.key,0);
  const posted=writes(calls);
  assert.deepEqual(posted.map(call=>call.url),['/channels/cost/historical-bindings/preview','/channels/cost/historical-bindings']);
  for(const call of posted)assert.equal(JSON.parse(call.body).local_channel_id,69);
  assert.equal(fields.get('[data-cost-mode]').value,'unallocated');
  assert.equal(fields.get('[data-cost-channel]').value,'0');
  assert.match(confirmations[0],/时段和金额重叠只是复核证据/);
  assert.match(confirmations[0],/上游金额：\$1\.250000/);
  assert.doesNotMatch(confirmations[0],/NaN|undefined/);
  assert.match(alerts[0],/已核验小时由后台排队重算，其余等待证据核验/);
});

test('future submission does not read the historical selection',async()=>{
  const {api,fields,calls}=fixture();
  fields.get('[data-cost-mode]').value='allocated';fields.get('[data-cost-channel]').value='59';
  await api.saveCostBinding(domain.key,0);
  const posted=writes(calls);
  assert.equal(posted.length,1);assert.equal(posted[0].url,'/channels/cost/bindings');
  assert.equal(JSON.parse(posted[0].body).local_channel_id,59);
  assert.equal(fields.get('[data-cost-history-channel]').value,'69');
});

test('existing historical segment keeps bounded continuation available',()=>{
  const {api,data}=fixture();data.sources[0].historical_binding_count=1;
  const html=api.costSourceRows(domain,data);
  assert.match(html,/已有 1 段历史归属/);
  assert.match(html,/data-cost-history-from value="1970-01-01T09:00"/);
  assert.match(html,/data-cost-history-to value="1970-01-01T10:00"/);
  assert.doesNotMatch(html,/data-cm-cost-history-binding[^>]*disabled/);
  assert.match(html,/单次最多 90 天/);
});

test('historical hour input is strict Beijing time independent of browser timezone',()=>{
  const {api}=fixture();
  assert.equal(api.parseCostHistoryHour('1970-01-01T09:00'),3600);
  assert.equal(api.costHistoryHourInput(3600),'1970-01-01T09:00');
  for(const value of ['2026-02-30T09:00','2026-10-06T09:30','2026-13-01T09:00','','2026-10-06T09:00Z'])assert.ok(Number.isNaN(api.parseCostHistoryHour(value)));
  for(const ts of [NaN,Infinity,-3600,3601])assert.equal(api.costHistoryHourInput(ts),'');
});

test('invalid ranges never request a preview or audit reason',async()=>{
  for(const end of ['', '1970-01-01T08:00', '1970-01-01T09:00', '1970-01-01T10:30']){
    const f=fixture();f.fields.get('[data-cost-history-to]').value=end;
    await f.api.saveHistoricalCostBinding(domain.key,0);
    assert.equal(f.calls.length,0);assert.equal(f.prompts.length,0);assert.match(f.alerts[0],/有效的北京时间整小时时段/);
  }
});

test('save freezes narrowed preview bounds even if fields change during preview',async()=>{
  let f;f=fixture({fetchOverride:url=>{
    if(url.endsWith('/preview'))f.fields.get('[data-cost-history-to]').value='1970-01-10T00:00';
  }});
  f.fields.get('[data-cost-history-from]').value='1970-01-01T08:00';
  f.fields.get('[data-cost-history-to]').value='1970-01-01T11:00';
  await f.api.saveHistoricalCostBinding(domain.key,0);
  const posted=writes(f.calls).map(call=>JSON.parse(call.body));
  assert.equal(posted.length,2);
  assert.deepEqual([posted[0].valid_from,posted[0].valid_to],[0,10800]);
  assert.deepEqual([posted[1].valid_from,posted[1].valid_to],[3600,7200]);
  assert.match(f.confirmations[0],/1970-01-01 09:00 至 1970-01-01 10:00/);
});

test('older or invalid preview cannot silently expand the selected range',async()=>{
  for(const binding of [{ValidFrom:0,ValidTo:7200},{ValidFrom:3600,ValidTo:10800},{ValidFrom:3601,ValidTo:7200},{}]){
    const f=fixture({fetchOverride:url=>url.endsWith('/preview')?response({binding}):null});
    await f.api.saveHistoricalCostBinding(domain.key,0);
    assert.equal(writes(f.calls).length,1);assert.equal(f.confirmations.length,0);
    assert.match(f.alerts[0],/预演范围与所选时段不一致/);assert.equal(f.buttons.history.disabled,false);
  }
});

test('unknown selection, cancelled confirmation and preview failure cannot write history',async()=>{
  const invalid=fixture();invalid.fields.get('[data-cost-history-channel]').value='999';
  await invalid.api.saveHistoricalCostBinding(domain.key,0);
  assert.equal(invalid.calls.length,0);assert.equal(invalid.prompts.length,0);assert.match(invalid.alerts[0],/目录中的历史归属渠道/);
  const cancelled=fixture({confirm:false});await cancelled.api.saveHistoricalCostBinding(domain.key,0);
  assert.deepEqual(writes(cancelled.calls).map(call=>call.url),['/channels/cost/historical-bindings/preview']);
  const failed=fixture({fetchOverride:url=>url.endsWith('/preview')?response({error:'historical evidence unavailable'},409):null});
  await failed.api.saveHistoricalCostBinding(domain.key,0);
  assert.deepEqual(writes(failed.calls).map(call=>call.url),['/channels/cost/historical-bindings/preview']);
  assert.match(failed.alerts[0],/historical evidence unavailable/);
});

test('real sealed snapshot response renders and confirms without saving',{
  skip:!process.env.MONITOR_CHANNEL_HISTORY_ACCEPTANCE_RECEIPT,
},async()=>{
  const receipt=JSON.parse(readFileSync(process.env.MONITOR_CHANNEL_HISTORY_ACCEPTANCE_RECEIPT,'utf8'));
  assert.equal(receipt.mode,'readonly_local_history_directory');
  assert.equal(receipt.original_unchanged,true);assert.equal(receipt.both_writes_refused,true);
  const site={domain:'4sapi.com',key:'domain:4sapi.com',vendors:[{channels:[{id:59,name:'visible current channel',current:true}]}]};
  const {api,data,fields,calls,alerts,confirmations}=fixture({site,sourcePayload:receipt.source_payload,confirm:false,
    fetchOverride:url=>url.endsWith('/preview')?response(receipt.preview):null});
  const html=api.costSourceRows(site,data);
  assert.match(selectHTML(html,'data-cost-history-channel'),/value="64" selected/);
  assert.doesNotMatch(selectHTML(html,'data-cost-channel'),/value="64"|value="69"/);
  fields.get('[data-cost-history-channel]').value='64';
  await api.saveHistoricalCostBinding(site.key,0);
  assert.equal(alerts.length,0);assert.equal(confirmations.length,1);
  assert.match(confirmations[0],/上游证据：11 小时 \/ 43 请求/);
  assert.ok(confirmations[0].includes(receipt.preview.evidence_billed_cost.display));
  assert.doesNotMatch(confirmations[0],/NaN|undefined/);
  assert.deepEqual(writes(calls).map(call=>call.url),['/channels/cost/historical-bindings/preview']);
  assert.equal(JSON.parse(calls[0].body).domain,site.domain);
  assert.equal(JSON.parse(calls[0].body).local_channel_id,64);
});
