import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

const source = name => readFileSync(new URL(`../../monitor/${name}`, import.meta.url), 'utf8');

test('upstream diagnostics renders safe instructions and tolerates missing checks',()=>{
  const {context,element}=dashboard();
  context.channelTest.renderUpstreamDiagnostic({provider:'tokenforce',confidence:'suspected',checks:[{name:'余额',status:'error',message:'<img src=x onerror=alert(1)>',action:'重新登录'}],instructions:['查找 orgId'],scope:'仅单页'});
  const html=element('cmUpstreamDiagnostic').innerHTML;
  assert.match(html,/仅疑似/);assert.match(html,/重新登录/);assert.match(html,/orgId/);assert.doesNotMatch(html,/<img/);
  context.channelTest.renderUpstreamDiagnostic({provider:'unknown',checks:null,instructions:null});
  assert.match(element('cmUpstreamDiagnostic').innerHTML,/尚未识别/);
});

test('upstream diagnostics sends only address and domain; ignores late response after reset',async()=>{
  let finish,request;
  let jsonStarted;
  const ready=new Promise(resolve=>{jsonStarted=resolve});
  const {context,element}=dashboard(async(url,options)=>{
    request={url,...options};
    return {ok:true,json:()=>new Promise(resolve=>{finish=resolve;jsonStarted()})};
  });
  const ui=context.channelTest;ui.cm.upstreamDomain={domain:'example.com'};ui.cm.upstreamConfig={provider:'newapi'};
  element('cmUpstreamBaseURL').value='https://panel.example.com';
  element('cmUpstreamAccessToken').value='DO-NOT-SEND';
  const pending=ui.diagnoseUpstream();await ready;
  assert.equal(request.url,'/channels/upstream/diagnose');
  assert.deepEqual(JSON.parse(request.body),{domain:'example.com',base_url:'https://panel.example.com'});
  ui.resetUpstreamDiagnostic();
  finish({provider:'newapi',checks:[],instructions:[]});await pending;
  assert.equal(element('cmUpstreamDiagnostic').hidden,true);
  assert.equal(element('cmUpstreamDiagnostic').innerHTML,'');
  assert.equal(request.signal.aborted,true);
});

test('diagnostic browser timeout leaves room for the server queue budget',async()=>{
  const {context,element}=dashboard(async()=>({ok:true,json:async()=>({provider:'unknown',checks:[],instructions:[]})}));
  let timeout;
  context.setTimeout=(_callback,ms)=>{timeout=ms;return 1};
  context.channelTest.cm.upstreamDomain={domain:'example.com'};
  context.channelTest.cm.upstreamConfig={};
  element('cmUpstreamBaseURL').value='https://example.com';
  await context.channelTest.diagnoseUpstream();
  const serverSeconds=Number(source('upstream_diagnostic_transport.go').match(/const diagnosticTimeout = (\d+) \* time.Second/)[1]);
  assert.ok(timeout>=serverSeconds*1000+5000,'browser must allow the server to return timeout advice');
  assert.equal(element('cmUpstreamDetect').disabled,false);
});

// Execute production renderers against a minimal DOM sink. No network, login,
// timers or production data are used; assertions inspect the generated HTML.
function dashboard(fetchImpl = () => {throw Error('network forbidden in renderer tests');}) {
  const elements = new Map();
  const document = {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, {innerHTML: '', textContent: '', value: '', options: [], hidden: false, children: [], dataset: {}, style: {setProperty() {}}, setAttribute() {}, removeAttribute() {},
        querySelectorAll: () => [], querySelector: selector => document.getElementById(id + selector)});
      return elements.get(id);
    },
    addEventListener() {},
  };
  const chartOptions=[];
  const context = vm.createContext({document, window: {}, fetch: fetchImpl, AbortController, URLSearchParams,
    setTimeout() {}, clearTimeout() {}, echarts: {init: () => ({clear() {}, setOption(option) {chartOptions.push(option)}, resize() {}})}});
  vm.runInContext(source('channel_data_status.js'), context);
  const channel = source('channel_management.js');
  const end = channel.lastIndexOf('})();');
  assert.ok(end > 0, 'channel module closure must exist');
  vm.runInContext(channel.slice(0, end) + '\nglobalThis.channelTest={loadReport,render,navigateDataStatus,usageMetric,renderUpstreamFunds,dateTime,shortDateTime,domainCard,freshness,cm,filteredDomains,domainSortDescription,renderUpstreamDiagnostic,diagnoseUpstream,resetUpstreamDiagnostic};\n' + channel.slice(end), context);

  const page = source('page.html');
  vm.runInContext(page.match(/^function problemBuckets\(.*$/m)[0], context);
  vm.runInContext(source('stability.js').match(/^function comparisonPendingText\([^]*?^}/m)[0], context);
  for (const name of ['syncTime', 'renderLocks', 'renderInfraOverview', 'problemSpan', 'problemDetail', 'renderBanner', 'renderSummary', 'dimensionLimitNotice', 'clearModelResult']) {
    const declaration = page.match(new RegExp(`^function ${name}\\([^]*?^}`, 'm'));
    assert.ok(declaration, `production function ${name} must exist`);
    vm.runInContext(declaration[0], context);
  }
  for (const name of ['esc', 'fmtAge', 'infraBadge', 'infraDot', 'countItem', 'fmtNum', 'fmtRate', 'fmtUSD', 'fmtTtft', 'fmtTok', 'fmtLat', 'clock']) {
    const declaration = page.match(new RegExp(`^const ${name}=.*$`, 'm'));
    assert.ok(declaration, `production formatter ${name} must exist`);
    vm.runInContext(declaration[0], context);
  }
  vm.runInContext('function undelivFlagged(){return []} function undelivWatch(){return []} function renderUndeliv(){} function renderAttnStat(){}', context);
  const capacity = source('capacity.js');
  const capacityEnd = capacity.lastIndexOf('})();');
  assert.ok(capacityEnd > 0);
  vm.runInContext(capacity.slice(0, capacityEnd) + '\nglobalThis.capacityTest={renderSources,renderKPIs,renderTraffic,infraSeries,age,load,state};\n' + capacity.slice(capacityEnd), context);
  const governance=source('group_governance.js'),governanceEnd=governance.lastIndexOf('})();');
  vm.runInContext(governance.slice(0,governanceEnd)+'\nglobalThis.governanceTest={renderHeader,state};\n'+governance.slice(governanceEnd),context);
  return {context, chartOptions, element: id => document.getElementById(id), html: id => document.getElementById(id).innerHTML};
}

test('RPM, TPM and stability have independent axes and preserve null gaps',()=>{
  const {context,chartOptions}=dashboard();
  context.capacityTest.renderTraffic({series:[{ts:300,business_rpm:10,tpm:1000000,stability_pct:99},{ts:600,business_rpm:null,tpm:null}]});
  const option=chartOptions.at(-1);
  assert.equal(option.yAxis[0].name,'RPM');assert.equal(option.yAxis[1].name,'TPM');
  assert.equal(option.series.find(x=>x.name==='TPM').yAxisIndex,1);
  assert.equal(option.series.find(x=>x.name==='平台日志稳定率').yAxisIndex,2);
  assert.equal(option.series[0].data[1][1],null);
});

test('governance has no fake zero before first snapshot and sits last in both menus',()=>{
  const {context,element}=dashboard();
  for(const enabled of [false,true]){
    context.governanceTest.state.report={enabled,state:{current_group_count:0}};
    context.governanceTest.renderHeader();
    assert.equal(element('ggTotal').textContent,'—');
  }
  context.governanceTest.state.report={enabled:true,state:{last_success_at:1,current_group_count:0}};
  context.governanceTest.renderHeader();assert.equal(element('ggTotal').textContent,'0');
  for(const menu of source('page.html').matchAll(/<nav class="tabs[^]*?<\/nav>/g)){
    const tabs=[...menu[0].matchAll(/data-tab="([^"]+)"/g)].map(m=>m[1]);
    assert.equal(tabs.at(-1),'group-governance');
  }
});

test('sync includes missing active accounts and explains unrepresentable daily windows',()=>{
  const {context}=dashboard();
  const rows=context.window.channelDataStatus.issues({meta:{data_coverage:{complete:true}},domains:[
    {domain:'missing.example',enabled_channels:2,upstream:{configured:false}},
    {domain:'daily.example',upstream:{configured:true,balance_usd:20,usage_sync_enabled:true},upstream_usage:{available:false,integrity_status:'window_mismatch'}}
  ]});
  assert.equal(rows.length,2);assert.match(rows[0].detail,/2 个启用渠道未配置/);
  assert.match(rows[1].detail,/自然日账单无法精确拆分/);
  assert.doesNotMatch(rows[1].detail,/暂无消费账单/);
});

test('local model preview never masquerades as a production sampler failure or active sampling', () => {
  const {context, html, element} = dashboard();
  context.renderBanner({view:'observed',local_snapshot_only:true,sampling_active:false,by_channel:null,by_model:null});
  assert.match(html('bannerMain'), /本机快照预览/);
  assert.doesNotMatch(html('bannerMain'), /实时采样不可用或已停止/);
  assert.match(html('heartbeat'), /未采集/);
  assert.equal(element('banner').className,'banner warn');
  context.renderBanner({view:'observed',local_snapshot_only:false,sampling_active:false});
  assert.match(html('bannerMain'), /实时采样不可用或已停止/);
  assert.equal(element('banner').className,'banner bad');
});

test('observed mode does not mask detected errors with a provisional-data notice', () => {
  const {context, html, element} = dashboard();
  for (const health of ['bad','warn']) {
    context.renderBanner({view:'observed',sampling_active:true,data_complete:false,
      summary:{total:10},by_channel:[{health}],by_model:null});
    assert.equal(element('banner').className,'banner '+health);
    assert.match(html('bannerMain'),health==='bad'?/1 项错误/:/1 项波动/);
    assert.match(html('bannerMain'),/未定稿/);
  }
  context.renderBanner({view:'observed',sampling_active:true,summary:{total:10}});
  assert.match(html('bannerMain'),/实时观察/);
  assert.doesNotMatch(html('bannerMain'),/运行正常/);
});

test('model error attention does not relabel delivery-only warnings as request failures',()=>{
  const {context,html}=dashboard();
  for(const error_health of ['good',undefined]){
    context.renderBanner({view:'observed',sampling_active:true,summary:{total:300},by_channel:[{
      key:'1',label:'delivery-only',health:'warn',error_health,failed:0,anomaly:3,total:300
    }]});
    assert.doesNotMatch(html('bannerMain'),/1 项波动|1 项错误/);
    assert.doesNotMatch(html('attnList'),/delivery-only/);
  }
  assert.doesNotMatch(source('page.html'),/自动禁用\/切流已介入/);
});

test('model error attention excludes anomaly minutes but retains real failures',()=>{
  const {context,html}=dashboard();
  vm.runInContext("const errText=()=>''; const expanded=new Set();",context);
  context.renderBanner({view:'observed',sampling_active:true,summary:{total:100},by_channel:[{
    key:'1',label:'actual-errors',health:'warn',error_health:'bad',failed:20,anomaly:1,total:100,
    spark:[{ts:1800000000,success:0,failed:0,anomaly:1}]
  }]});
  assert.match(html('bannerMain'),/1 项错误/);
  assert.match(html('attnList'),/actual-errors/);
  assert.match(html('attnList'),/错误 20\/100/);
  assert.doesNotMatch(html('attnList'),/<details/);
  context.renderBanner({view:'observed',sampling_active:true,summary:{total:100},by_channel:[{
    key:'1',label:'actual-errors',health:'warn',error_health:'bad',failed:20,anomaly:1,total:100,
    spark:[{ts:1800000000,success:0,failed:0,anomaly:1},{ts:1800000060,success:0,failed:20,anomaly:0}]
  }]});
  assert.match(html('attnList'),/<details/);
  assert.match(html('attnList'),/错误×20/);
  assert.doesNotMatch(html('attnList'),/交付异常×/);
});

test('complete historical bills cannot conceal stopped or failed usage synchronization', () => {
  const {context} = dashboard();
  for (const state of ['stale','error','reconnect','global_off']) {
    const report={meta:{data_coverage:{complete:true}},domains:[{domain:'upstream.test',
      upstream:{configured:true,balance_usd:10,status:'ok',usage_sync_enabled:true,usage_effective_status:state},
      upstream_usage:{available:true,complete:true,cost_usd:5,adjusted_cost_available:true,adjusted_cost_usd:5}}]};
    const issues=context.window.channelDataStatus.issues(report);
    assert.equal(issues.length,1,state+' must remain visible despite a complete historical bill');
    assert.match(issues[0].detail,/同步|认证/);
  }
});

test('comparison pending explanation distinguishes current incompleteness from prior gaps', () => {
  const {context} = dashboard();
  const previous={complete:true,missing_hours:0};
  assert.match(context.comparisonPendingText({comparison_coverage:previous,data_coverage:{complete:false,missing_hours:2}}),/当前区间.*2 小时/);
  assert.match(context.comparisonPendingText({comparison_coverage:previous,data_coverage:{complete:true,provisional_seconds:3600}}),/当前区间仍在汇总/);
  assert.match(context.comparisonPendingText({data_coverage:{complete:true},comparison_coverage:{complete:false,missing_hours:3}}),/上一周期小时待补 3 个/);
});

function modelDashboard(fetchImpl){
  const view=dashboard(fetchImpl), {context}=view;
  vm.runInContext(`let modelLoadSeq=0,modelAbort=null,modelQueryKey='',WINDOW=60;
    let lastTrend=[],trendChart=null,latChart=null,ttftChart=null,sloGauge=null;
    globalThis.applied=[];function apply(s){applied.push(s)}
    function renderCompare(){} function renderRejections(){}`,context);
  const declaration=source('page.html').match(/^async function load\([^]*?^}/m);
  assert.ok(declaration);
  vm.runInContext(declaration[0],context);
  view.element('modelView').value='observed';
  return view;
}

function usageDashboard(fetchImpl, detail){
  const view=dashboard(fetchImpl),{context}=view;
  vm.runInContext(`let usageSeq=0,usageAbort=null,usageMxCache=null;
    globalThis.usageDetail=null;globalThis.usageApplied=[];
    function usageRangeParams(){return new URLSearchParams()}
    function usageSetDataNotice(){} function usageSetMemberMatrixState(){}
    function usageResetMemberMatrix(){} function renderUsageMemRank(){} function usageRenderMemberMatrix(){}
    function renderUsageMatrix(mx){usageApplied.push(mx)}
    function renderUsage(stats){usageApplied.push(stats)}`,context);
  context.usageDetail=detail;
  for(const name of ['usageBeginRequest','usageLoadMatrix','usageLoadDetail']){
    const declaration=source('page.html').match(new RegExp(`^(?:async )?function ${name}\\([^]*?^}`,'m'));
    assert.ok(declaration);vm.runInContext(declaration[0],context);
  }
  return view;
}

test('usage late JSON and malformed old responses cannot overwrite a newer customer or range', async()=>{
  for(const detail of [null,{type:'user',id:1},{type:'token',id:3,uid:1},{type:'group',id:1}]){
    for(const failOldBody of [false,true]){
      const body=deferred(),parsing=deferred();let calls=0;
      const {context}=usageDashboard(async()=>++calls===1
        ?{ok:true,status:200,json:()=>{parsing.resolve();return body.promise;}}
        :response({matrix:{cost:7},stats:{cost:7}}),detail);
      const load=detail?context.usageLoadDetail:context.usageLoadMatrix;
      const old=load();await parsing.promise;await load();
      if(failOldBody)body.reject(Error('malformed old body'));else body.resolve({matrix:{cost:999},stats:{cost:999}});
      await old;
      assert.deepEqual(Array.from(context.usageApplied,x=>x.cost),[7]);
    }
  }
});

test('usage malformed current JSON is an error, never a successful empty financial result', async()=>{
  for(const detail of [null,{type:'user',id:1}]){
    const {context,html}=usageDashboard(async()=>({ok:true,status:200,json:async()=>{throw Error('invalid JSON')}}),detail);
    await (detail?context.usageLoadDetail():context.usageLoadMatrix());
    assert.equal(context.usageApplied.length,0);
    assert.match(html('usageErr'),/加载失败/);
  }
});

test('model query change clears stale values and failed responses cannot restore them', async () => {
  const pending=deferred();
  const {context,html,element}=modelDashboard(()=>pending.promise);
  element('summary').innerHTML='OLD MONEY';element('tbModel').innerHTML='OLD MODEL';
  const loading=context.load();
  assert.doesNotMatch(html('summary'),/OLD MONEY/);
  assert.equal(html('tbModel'),'');
  assert.match(element('genAt').textContent,/正在读取/);
  pending.reject(Error('offline'));
  await loading;
  assert.match(html('errBox'),/offline/);
  assert.match(element('genAt').textContent,/未展示旧区间/);
  assert.equal(context.applied.length,0);
});

test('model late JSON or errors cannot overwrite a newer query', async () => {
  for(const fail of [false,true]){
    const oldBody=deferred(),started=deferred();let calls=0;
    const {context}=modelDashboard(()=>++calls===1?Promise.resolve({ok:true,status:200,json(){started.resolve();return oldBody.promise;}}):Promise.resolve(response({snapshot:{marker:'new'}})));
    const old=context.load();await started.promise;
    vm.runInContext('WINDOW=120',context);await context.load();
    if(fail)oldBody.reject(Error('late failure'));else oldBody.resolve({snapshot:{marker:'old'}});
    await old;
    assert.equal(context.applied.length,1);
    assert.equal(context.applied[0].marker,'new');
  }
});

test('channel late bodies and late failures cannot overwrite newer report money', async () => {
  for(const fail of [false,true]){
    const oldBody=deferred(),started=deferred();let calls=0;
    const report={meta:{from:'new',to:'new',data_coverage:{complete:true}},domains:[],summary:{usage:{}}};
    const {context,html}=dashboard((url,options)=>{
      assert.equal(options.cache,'no-store');
      if(url.startsWith('/channels/economics'))return Promise.resolve(response({enabled:false}));
      if(++calls===1)return Promise.resolve({ok:true,status:200,json(){started.resolve();return oldBody.promise;}});
      return Promise.resolve(response(report));
    });
    const api=context.channelTest,old=api.loadReport();await started.promise;
    api.cm.hours=48;await api.loadReport();
    if(fail)oldBody.reject(Error('late failure'));else oldBody.resolve({...report,meta:{...report.meta,from:'old'}});
    await old;
    assert.equal(api.cm.report.meta.from,'new');
    assert.doesNotMatch(html('cmBody'),/late failure/);
  }
});

test('model cards distinguish unknown, signed empty and partial observations', () => {
  const {context, html} = dashboard();
  context.renderSummary({total: 0, cost_usd: 0}, false);
  assert.match(html('summary'), /区间完整性未确认/);
  assert.doesNotMatch(html('summary'), /\$0\.00|0\.0%|color:#10b981/);
  context.renderSummary(null, false);
  assert.doesNotMatch(html('summary'), /\$0\.00|0\.0%/);
  context.renderSummary({total: 0, cost_usd: 0}, true);
  assert.match(html('summary'), /\$0\.00/);
  assert.match(html('summary'), /无请求样本/);
  assert.doesNotMatch(html('summary'), /0\.0%/);
  context.renderSummary({total: 10, success_rate: 90, cost_usd: 12.3}, false);
  assert.match(html('summary'), /90\.0%/);
  assert.match(html('summary'), /\$12\.30/);
  assert.match(html('summary'), /仅已采集记录/);
});

test('dimension truncation is explicit only when the server found a sentinel row', () => {
  const {context} = dashboard();
  assert.equal(context.dimensionLimitNotice(undefined), '');
  assert.equal(context.dimensionLimitNotice({limit: 200, truncated: false}), '');
  assert.match(context.dimensionLimitNotice({limit: 200, truncated: true}), /前 200 项.*总览与总趋势包含全部记录/);
});

test('upstreams default to name order with unconfigured inactive domains last', () => {
  const {context} = dashboard();
  const api = context.channelTest, cm = api.cm;
  const domain = (name, active, config = {}, cost = 0) => ({key: name, domain: name, configured: true, ...config,
    vendors: [{name: 'OpenAI', channels: [{id: name, name, current: true, status: active ? 1 : 2,
      usage: {requests: 1, tokens: 1, cost_usd: cost}, groups: []}]}]});
  cm.report = {meta: {data_coverage: {complete: true}}, domains: [
    domain('aaa-unused.test', false, {}, 999),
    domain('z-active.test', true, {}, 10),
    domain('b-account.test', false, {upstream: {configured: true}}, 5),
    domain('a-rates.test', false, {rate_config: {configured_channels: 1}}, 2),
    domain('aaa-history.test', false, {}, 900),
  ]};
  // A historical channel with stale status=1 is not currently usable.
  Object.assign(cm.report.domains[4].vendors[0].channels[0], {current: false, status: 1});
  const names = () => Array.from(api.filteredDomains(), d => d.domain);
  assert.equal(cm.sort, 'name');
  assert.deepEqual(names(), ['a-rates.test', 'b-account.test', 'z-active.test', 'aaa-history.test', 'aaa-unused.test']);
  assert.match(api.domainSortDescription(), /名称 A–Z.*置底/);
  cm.sort = 'cost';
  assert.deepEqual(names(), ['z-active.test', 'b-account.test', 'a-rates.test', 'aaa-unused.test', 'aaa-history.test']);
  cm.report.meta.data_coverage.complete = false;
  assert.deepEqual(names(), ['z-active.test', 'b-account.test', 'a-rates.test', 'aaa-unused.test', 'aaa-history.test']);
  cm.filters.search = 'aaa-unused';
  assert.deepEqual(names(), ['aaa-unused.test'], 'bottom entries remain searchable, not deleted');
  assert.equal(cm.report.domains[0].sortAtEnd, undefined, 'source snapshot must remain unchanged');
});

test('filtering out a usable channel cannot reclassify its upstream as unused', () => {
  const {context} = dashboard();
  const api = context.channelTest;
  const channel = status => ({id: status, name: 'channel', current: true, status, usage: {}, groups: []});
  api.cm.report = {domains: [{domain: 'z.test', vendors: [{name: 'v', channels: [channel(1), channel(2)]}]}]};
  api.cm.filters.status = 'disabled';
  const rows = api.filteredDomains();
  assert.equal(rows[0].vendors[0].channels.length, 1);
  assert.equal(rows[0].sortAtEnd, false);
});

// Deferred responses reproduce races even if the transport ignores abort.
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => {resolve = yes; reject = no;});
  return {promise, resolve, reject};
}
const capacityReport = requests => ({meta: {sources: {}, from_ts: 100, to_ts: 200, bucket_seconds: 60},
  summary: {current_business_rpm: requests}, options: {}, series: [], ingress: [], infra: [], components: []});
const response = report => ({ok: true, status: 200, json: async () => report});

test('capacity clears old results immediately and does not revive them after a failed query', async () => {
  const pending = deferred();
  const {context, element, html} = dashboard(() => pending.promise);
  const c = context.capacityTest;
  c.state.report = capacityReport(999);c.state.loaded = true;
  const options = {users: [{key: 'u', label: 'test user'}]};c.state.options = options;
  for (const id of ['capKpis', 'capRanking', 'capComponents']) element(id).innerHTML = 'OLD';
  element('capRangeMeta').textContent = 'OLD';
  element('capEconomics').querySelector('span').textContent = 'OLD';
  let cleared = 0;
  for (const id of ['capTrafficChart', 'capIngressChart', 'capInfraChart']) c.state.charts[id] = {clear() {cleared++;}};
  const loading = c.load();
  assert.equal(c.state.report, null);assert.equal(c.state.loaded, false);assert.equal(cleared, 3);
  assert.equal(c.state.options, options, 'filter options must survive while result values are cleared');
  for (const id of ['capKpis', 'capRanking', 'capComponents']) assert.doesNotMatch(html(id), /OLD/);
  assert.match(element('capRangeMeta').textContent, /正在加载/);
  assert.doesNotMatch(element('capEconomics').querySelector('span').textContent, /OLD/);
  pending.reject(Error('query failed'));
  await loading;
  assert.equal(c.state.report, null);
  assert.match(html('capError'), /query failed/);
  assert.match(element('capRangeMeta').textContent, /未展示旧查询结果/);
});

test('capacity newest query wins over late bodies and late errors', async () => {
  for (const lateFailure of [false, true]) {
    const body = deferred(), parsing = deferred();
    let calls = 0;
    const {context, html} = dashboard(async () => ++calls === 1
      ? {ok: true, status: 200, json: () => {parsing.resolve();return body.promise;}} : response(capacityReport(7)));
    const c = context.capacityTest;
    const old = c.load();
    await parsing.promise;
    await c.load();
    if (lateFailure) body.reject(Error('old failure'));else body.resolve(capacityReport(999));
    await old;
    assert.equal(c.state.report.summary.current_business_rpm, 7);
    assert.equal(c.state.loaded, true);
    assert.doesNotMatch(html('capError'), /old failure/);
    assert.doesNotMatch(html('capKpis'), /999/);
  }
});

test('observed model view survives null dimensions and labels incomplete facts', () => {
  const {context, html} = dashboard();
  context.renderBanner({view: 'observed', data_complete: false, sampling_active: true, by_channel: null, by_model: null});
  assert.match(html('bannerMain'), /实时观察/);
  assert.match(html('bannerMain'), /尚未定稿/);
  context.renderBanner({view: 'observed', data_complete: false, sampling_active: false});
  assert.match(html('bannerMain'), /采样不可用或已停止/);
  context.renderBanner({view: 'finalized', data_complete: true, finalization_delayed: true});
  assert.match(html('bannerMain'), /历史已定稿窗口/);
});

test('capacity distinguishes available samples from interval completeness', () => {
  const {context, html} = dashboard();
  context.capacityTest.renderSources({business_log: {configured: true, available: true, coverage_complete: false, age_sec: 60}});
  assert.match(html('capSources'), /区间完整性未确认，均值暂不发布/);
  context.capacityTest.renderKPIs({summary: {current_business_rpm: 3, peak_tpm: 500}, meta: {sources: {}}});
  assert.match(html('capKpis'), /最新观测 RPM/);
  assert.match(html('capKpis'), /已观测分钟峰值 TPM/);
  assert.doesNotMatch(html('capKpis'), /真实分钟峰值|最近完整分钟/);
  context.capacityTest.renderSources({business_log: {configured: true, available: true, coverage_complete: true}});
  assert.match(html('capSources'), /日志区间完整/);
});

test('no observations or incomplete sampling cannot produce an all-clear message', () => {
  const {context, html} = dashboard();
  for (const snapshot of [
    {view:'observed', sampling_active:false, summary:{total:0}},
    {view:'finalized', data_complete:true, sampling_active:true, summary:{total:0}},
  ]) {
    context.renderBanner(snapshot);
    assert.match(html('attnList'), /无法判断/);
    assert.doesNotMatch(html('attnList'), /✓|无错误/);
  }
  context.renderBanner({view:'observed', sampling_active:true, summary:{total:10}});
  assert.match(html('attnList'), /不代表区间完整/);
  context.renderBanner({view:'finalized', data_complete:true, finalization_delayed:true, summary:{total:10}});
  assert.match(html('attnList'), /不能判断当前状态/);
  context.renderBanner({view:'finalized', data_complete:true, sampling_active:true, summary:{total:10}});
  assert.match(html('attnList'), /✓ 已定稿窗口/);
});

test('capacity unknown ages never become negative or non-finite durations', () => {
  const {context} = dashboard();
  for (const age of [-1, null, undefined, NaN, Infinity, '60']) assert.equal(context.capacityTest.age(age), '—');
  assert.equal(context.capacityTest.age(0), '0秒');
  assert.equal(context.capacityTest.age(60), '60秒');
  assert.equal(context.capacityTest.age(120), '2分钟');
});

test('resource chart keeps all current resources without mixing metric units', () => {
  const {context} = dashboard();
  const current = Array.from({length: 12}, (_, i) => 'ecs/worker-' + i);
  const rows = [...current, 'old-lightsail'].flatMap(resource => [
    {resource, metric: 'cpu', ts: 1800000000, value: 20},
    {resource, metric: 'resp_ms', ts: 1800000000, value: 999},
  ]);
  const currentCPU = context.capacityTest.infraSeries(rows, current, '@current', 'cpu');
  assert.equal(currentCPU.length, 12);
  assert.ok(currentCPU.every(s => s.data[0][1] === 20 && s.name !== 'old-lightsail'));
  assert.equal(context.capacityTest.infraSeries(rows, current, '@history', 'cpu').length, 13);
  const historical = context.capacityTest.infraSeries(rows, current, 'old-lightsail', 'resp_ms');
  assert.equal(historical.length, 1);
  assert.equal(historical[0].data[0][1], 999);
});

test('natural-day mismatch is explained on sync page, not in business amount slots', () => {
  const {context} = dashboard();
  const domain = {key: 'test', domain: 'example.test', configured: true,
    vendors: [], usage: {requests: 0, tokens: 0, cost_usd: 0},
    upstream: {configured: true, usage_sync_enabled: true, balance_usd: 10},
    upstream_usage: {available: true, integrity_status: 'window_mismatch', cost_usd: 999, granularity: 'day'}};
  const rendered = context.channelTest.domainCard(domain, 0, {}, false);
  assert.doesNotMatch(rendered, /账单数据异常|\$999|时间范围不匹配|已结束的完整自然日/);
  const status = context.window.channelDataStatus.render({domains: [domain], meta: {data_coverage: {complete: true}}});
  assert.match(status, /sync-status bad/);
  assert.match(status, /已结束的完整自然日/);
});

test('daily bill stays visible after refresh with scope, never contaminates hourly totals', () => {
  const {context,html}=dashboard(),ui=context.channelTest;
  const usage={requests:10,tokens:100,cost_usd:20};
  const daily={key:'spring',domain:'aicodewith.com',configured:true,usage,vendors:[],
    upstream:{configured:true,usage_sync_enabled:true,balance_usd:50,status:'ok',usage_status:'ok'},
    upstream_usage:{available:true,integrity_status:'window_mismatch',granularity:'day',cost_usd:0},
    natural_day_bill:{from_ts:1788624000,to_ts:1788776343,usage:{available:true,complete:true,cost_usd:770.3163,
      adjusted_cost_available:true,adjusted_cost_usd:385.15815,recharge_ratio:2,integrity_status:'complete',data_until:1788776223}}};
  const hourly={key:'hour',domain:'hour.example',configured:true,usage,vendors:[],
    upstream:{configured:true,usage_sync_enabled:true,balance_usd:20},
    upstream_usage:{available:true,complete:true,cost_usd:10,adjusted_cost_available:true,adjusted_cost_usd:5}};
  for(const [index,domain] of [daily,hourly].entries())domain.vendors=[{name:'vendor',channels:[{id:index+1,name:'channel',current:true,status:1,usage,groups:[]}]}];
  ui.cm.report={meta:{from:'2026-09-06',to:'2026-09-07',data_coverage:{complete:true}},summary:{usage},domains:[daily,hourly]};
  for(const amount of [770.3163,780.3163]){
    daily.natural_day_bill.usage.cost_usd=amount;
    ui.render();
    assert.match(html('cmBody'),new RegExp('\\$'+amount.toFixed(2).replace('.','\\.')));
    assert.match(html('cmBody'),/\$385\.16/);
    assert.match(html('cmBody'),/所涉自然日上游消费/);
    assert.match(html('cmBody'),/18:17:03/);
    assert.match(html('cmBody'),/非所选小时区间金额/);
    assert.match(html('cmSummary'),/<b>\$10\.00<\/b>/);
    assert.match(html('cmSummary'),/<b>\$5\.00<\/b>/);
    assert.doesNotMatch(html('cmSummary'),/\$770|\$780|\$385/);
    assert.match(html('cmSummary'),/1 个自然日账单另见下方，不计入本汇总/);
  }
  assert.equal(context.window.channelDataStatus.issues(ui.cm.report).length,0);
  const status=context.window.channelDataStatus.render(ui.cm.report);
  assert.doesNotMatch(status,/sync-status bad/);
  assert.match(status,/不计入精确区间汇总/);
  daily.upstream.usage_effective_status='reconnect';
  assert.match(context.window.channelDataStatus.issues(ui.cm.report)[0].detail,/认证已失效/);
  daily.natural_day_bill.usage.complete=false;
  assert.match(ui.domainCard(daily,0,usage,false),/已取得账单合计/);
});

test('Spring observed hourly amount is numeric with a quiet reconciliation note',()=>{
  const {context}=dashboard();
  const domain={key:'spring',domain:'aicodewith.com',usage:{},vendors:[],
    upstream:{configured:true,balance_usd:10,usage_sync_enabled:true},
    upstream_usage:{available:true,complete:false,provisional:true,granularity:'hour',cost_usd:12.34,
      adjusted_cost_available:true,adjusted_cost_usd:6.17,data_until:1788771600}};
  const card=context.channelTest.domainCard(domain,0,{},false);
  assert.match(card,/\$12\.34/);
  assert.match(card,/日账单待核对/);
  assert.doesNotMatch(card,/所涉自然日上游消费/);
});

test('daily source projection does not bypass invalid money or missing recharge evidence',()=>{
  const {context}=dashboard(),quality=context.window.channelDataStatus;
  const domain={key:'bad',domain:'bad.example',usage:{},vendors:[],upstream:{configured:true,balance_usd:10,usage_sync_enabled:true},
    upstream_usage:{available:true,integrity_status:'invalid_amount'},
    natural_day_bill:{from_ts:1788624000,to_ts:1788710400,usage:{available:true,complete:true,cost_usd:9000,integrity_status:'complete'}}};
  assert.doesNotMatch(context.channelTest.domainCard(domain,0,{},false),/9,000/);
  assert.equal(quality.billView(domain).daily,null);
  domain.upstream_usage.integrity_status='window_mismatch';
  assert.match(quality.issues({meta:{data_coverage:{complete:true}},domains:[domain]})[0].detail,/充值比例证据/);
  domain.natural_day_bill.usage.integrity_status='overlapping_buckets';
  assert.doesNotMatch(context.channelTest.domainCard(domain,0,{},false),/9,000/);
  assert.match(quality.issues({meta:{data_coverage:{complete:true}},domains:[domain]})[0].detail,/时间桶重叠/);
});

test('partial channel records render numeric KPIs and shares with one quiet diagnostic link', () => {
  const {context, html} = dashboard(), api = context.channelTest;
  const usage = {requests: 1234, tokens: 5000, cost_usd: 12.34};
  const domain = {key: 'example.test', domain: 'example.test', configured: true,
    upstream: {configured: true, enabled: true, usage_sync_enabled: true, balance_usd: 100},
    upstream_usage: {available: true, complete: false, cost_usd: 50, adjusted_cost_available: true, adjusted_cost_usd: 25},
    vendors: [{name: 'vendor', channels: [{id: 1, name: 'channel', current: true, status: 1, usage, groups: []}]}]};
  api.cm.report = {meta: {from: 'start', to: 'end', data_coverage: {complete: false, completed_hours: 23, expected_hours: 24, missing_hours: 1}},
    summary: {usage}, domains: [domain]};
  api.render();
  assert.match(html('cmSummary'), /1,234/);
  assert.match(html('cmSummary'), /\$12\.34/);
  assert.match(html('cmSummary'), /<b>\$50\.00<\/b>/);
  assert.match(html('cmSummary'), /<b>\$25\.00<\/b>/);
  assert.match(html('cmBody'), /100\.0%/);
  for (const section of ['cmSummary', 'cmBody']) assert.doesNotMatch(html(section), /已同步|暂不发布|补齐中|补全中|等待同步|排名暂不可用/);
  assert.equal((html('cmSummary').match(/href="#tab=sync"/g)||[]).length, 1);
  assert.match(html('cmSummary'), /cm-data-note/);
  const status = context.window.channelDataStatus.render(api.cm.report);
  assert.match(status, /sync-status bad/);
  assert.match(status, /23\/24/);
  assert.match(status, /example.test/);
  api.cm.expandedDomains.add('example.test');
  assert.doesNotThrow(() => api.render(), 'expanded groups must also render partial values');
});

test('upstream consumption freshness is a small note below both unchanged amounts', () => {
  const {context}=dashboard(),api=context.channelTest;
  const domain={key:'freshness',domain:'freshness.example',vendors:[],usage:{requests:1,tokens:2,cost_usd:3},
    upstream:{configured:true,enabled:true,usage_sync_enabled:true,usage_worker_enabled:true,usage_fresh:false,usage_effective_status:'stale',usage_data_until:1788832800,balance_usd:40},
    upstream_usage:{available:true,complete:true,cost_usd:12.34,adjusted_cost_available:true,adjusted_cost_usd:6.17,recharge_ratio:2}};
  const html=api.domainCard(domain,0,domain.usage,false);
  for(const [column,amount] of [['spend','12.34'],['adjusted','6.17']]){
    const slot=html.match(new RegExp(`<span class="cm-domain-upstream-${column}"[^]*?</span>`))[0];
    assert.ok(slot.includes(`<b>$${amount}</b><em class="cm-domain-metric-note neutral cm-bill-sync-note">`));
    assert.match(slot,/消费同步延迟/);assert.match(slot,/消费截至/);assert.match(slot,/上游官网核对/);
    assert.doesNotMatch(slot,/<b>[^<]*(同步|官网)/);
  }
  domain.upstream.usage_fresh=true;domain.upstream.usage_effective_status='ok';
  assert.doesNotMatch(api.domainCard(domain,0,domain.usage,false),/cm-bill-sync-note/);
});

test('bill freshness respects synchronization, provisional and historical coverage separately', () => {
  const {context}=dashboard(),note=context.window.channelDataStatus.billSyncNote;
  const baseline=()=>({upstream:{configured:true,usage_sync_enabled:true,usage_worker_enabled:true,usage_fresh:true,usage_effective_status:'ok',usage_data_until:1788832800},
    upstream_usage:{available:true,complete:true,cost_usd:0,integrity_status:'complete',data_until:1788710400}});
  assert.equal(note(baseline()),'','zero cost and a completed historical selection are not stale');
  for(const [state,expected] of [['error','同步失败'],['reconnect','需重新连接'],['global_off','已暂停'],['disabled','已停用'],['unsupported','不支持'],['queued','待更新'],['pending','待更新'],['stale','同步延迟']]){
    const domain=baseline();domain.upstream.usage_effective_status=state;
    assert.ok(note(domain).includes(expected),state);
    assert.match(note(domain),/上游官网核对/);
  }
  const incomplete=baseline();incomplete.upstream_usage.complete=false;
  assert.match(note(incomplete),/区间消费尚未同步完整/);
  incomplete.upstream_usage.provisional=true;
  assert.match(note(incomplete),/待日账单核对/);
  incomplete.upstream_usage={available:false,integrity_status:'window_mismatch'};
  incomplete.natural_day_bill={usage:{available:true,complete:true,cost_usd:2}};
  assert.equal(note(incomplete),'','supported natural-day display must use its own coverage');
  const missing=baseline();missing.upstream_usage.available=false;
  assert.match(note(missing),/暂无区间消费账单/);
  missing.upstream.usage_worker_enabled=false;
  assert.match(note(missing),/已暂停/);
  missing.upstream.usage_sync_enabled=false;
  assert.match(note(missing),/未开启/);
});

test('consumption freshness never substitutes balance timestamps or the selected historical endpoint', () => {
  const {context}=dashboard(),note=context.window.channelDataStatus.billSyncNote;
  const domain={upstream:{configured:true,usage_fresh:false,usage_effective_status:'stale',last_success_at:1788832800},
    upstream_usage:{available:true,complete:true,cost_usd:12,data_until:1788710400}};
  assert.doesNotMatch(note(domain),/消费截至|最近同步|Invalid Date/);
  domain.upstream.usage_data_until=1e99;
  domain.upstream.usage_last_success_at=1788832800;
  assert.match(note(domain),/最近同步/);assert.doesNotMatch(note(domain),/消费截至|Invalid Date/);
  domain.upstream.usage_data_until=1788832800;
  assert.match(note(domain),/消费截至/);assert.doesNotMatch(note(domain),/最近同步/);
  domain.upstream.usage_effective_status='<img src=x onerror=alert(1)>';
  assert.doesNotMatch(note(domain),/<img/);
});

test('diagnostic link activates the SPA tab and preserves modified native link clicks', () => {
  const {context} = dashboard();
  const navigations=[];
  context.window.monitorNavigate=tab=>navigations.push(tab);
  let prevented=0;
  const event={target:{closest:selector=>selector==='.cm-data-note a'},preventDefault(){prevented++;}};
  context.channelTest.navigateDataStatus(event);
  assert.deepEqual(navigations,['sync']);
  assert.equal(prevented,1);
  context.channelTest.navigateDataStatus({...event,ctrlKey:true});
  assert.equal(navigations.length,1);
});

test('channel unknown and signed empty values remain distinct; incomplete pending hour is red in sync', () => {
  const {context} = dashboard(), api = context.channelTest, quality = context.window.channelDataStatus;
  api.cm.report = {meta: {data_coverage: {complete: false, completed_hours: 0}}};
  for (const unknown of [null, undefined, NaN, Infinity, '', 0]) assert.equal(api.usageMetric(unknown, String), '—');
  assert.equal(api.usageMetric(8, String), '8');
  api.cm.report.meta.data_coverage.complete = true;
  assert.equal(api.usageMetric(0, String), '0');
  assert.match(quality.render(api.cm.report), /sync-status ok/);
  assert.doesNotMatch(quality.note(api.cm.report), /可能更新/);
  api.cm.report.meta.data_coverage = {complete: false, latest_hour_pending: true, completed_hours: 23, expected_hours: 24, missing_hours: 1};
  assert.match(quality.render(api.cm.report), /sync-status bad/);
  assert.match(quality.render(api.cm.report), /并非已确认丢失/);
  const injected = quality.render({domains: [{domain: '<script>oops</script>', upstream: {configured: true}}]});
  assert.doesNotMatch(injected, /<script>/);
});

test('exclusive midnight cutoff still identifies yesterday as historical', () => {
  const {context} = dashboard();
  const today = new Intl.DateTimeFormat('en-CA', {timeZone: 'Asia/Shanghai'}).format(new Date());
  const cutoff = Date.parse(today + 'T00:00:00+08:00') / 1000;
  const rendered = context.channelTest.freshness({to: today + ' 00:00', to_ts: cutoff, data_until: cutoff,
    data_coverage: {complete: true}});
  assert.match(rendered, /历史区间/);
});

function funds(overrides = {}) {
  return {capability: 'supported', state: {status: 'ok'}, query_complete: true,
    amounts_complete: true, usd_totals_complete: true,
    summary: {credited_usd: 0, debited_usd: 0, refunded_usd: 0, unknown_amount_events: 0},
    events: [], ...overrides};
}

test('unknown fund amounts cannot become a complete zero-dollar total', () => {
  const {context, html} = dashboard();
  context.channelTest.renderUpstreamFunds(funds({amounts_complete: false, usd_totals_complete: false,
    summary: {unknown_amount_events: 12}, events: [{kind: 'topup', occurred_at: 1788552000}]}));
  const rendered = html('cmUpstreamFunds');
  assert.match(rendered, /金额待核验/);
  assert.match(rendered, /金额未解析/);
  assert.match(rendered, /12 条/);
  assert.match(rendered, /明确入账（USD）<b>—<\/b>/);
  assert.match(rendered, /不能作为区间完整合计/);
});

test('native currency is visible without manufacturing USD conversion', () => {
  const {context, html} = dashboard();
  context.channelTest.renderUpstreamFunds(funds({usd_totals_complete: false,
    upstream_totals: [{currency: 'CNY', credited: 2000}]}));
  assert.match(html('cmUpstreamFunds'), /¥2,000 CNY/);
  assert.match(html('cmUpstreamFunds'), /明确入账（USD）<b>—<\/b>/);
  assert.doesNotMatch(html('cmUpstreamFunds'), /金额待核验/);
});

test('only covered and monetarily verified records may publish zero totals', () => {
  const {context, html} = dashboard();
  context.channelTest.renderUpstreamFunds(funds());
  assert.match(html('cmUpstreamFunds'), /明确入账（USD）<b>\$0\.00<\/b>/);
  for (const overrides of [{query_complete: false}, {amounts_complete: undefined}, {usd_totals_complete: undefined}]) {
    context.channelTest.renderUpstreamFunds(funds(overrides));
    assert.match(html('cmUpstreamFunds'), /明确入账（USD）<b>—<\/b>/);
  }
});

test('amount warnings do not hide a failed sync or quarantined evidence', () => {
  const {context, html} = dashboard();
  context.channelTest.renderUpstreamFunds(funds({state: {status: 'reconnect'},
    amounts_complete: false, usd_totals_complete: false, summary: {quarantined_events: 1},
    events: [{kind: 'topup', amount_known: true, amount_usd: 999, reparse_error: 'bad evidence'}]}));
  assert.match(html('cmUpstreamFunds'), /chip bad/);
  assert.match(html('cmUpstreamFunds'), /需重新连接 · 金额待核验/);
  assert.match(html('cmUpstreamFunds'), /已隔离（不计入汇总）/);
  assert.doesNotMatch(html('cmUpstreamFunds'), /\$999/);
});

test('lock rows and overview distinguish missing evidence from an unexpected response', () => {
  const {context, html} = dashboard();
  context.renderLocks({locks: [{target: 'retired', status: 'nosample', locked: false, http_code: 0, age_sec: -1}]});
  assert.match(html('tbLock'), /未判定/);
  assert.doesNotMatch(html('tbLock'), /可绕过|锁失效/);
  context.renderInfraOverview({overview: {status: 'nosample', locks_total: 2, locks_ok: 0, locks_bad: 0, locks_unknown: 2}, data_age_sec: -1});
  assert.match(html('infraCounts'), /2 个未判定/);
  assert.doesNotMatch(html('infraCounts'), /失效|响应异常/);
  context.renderLocks({locks: [{target: 'origin', status: 'bad', locked: false, http_code: 500, age_sec: 1}]});
  assert.match(html('tbLock'), /拦截响应异常（需核验）/);
  assert.doesNotMatch(html('tbLock'), /可绕过/);
  context.renderLocks({locks: [{target: 'origin', status: 'ok', locked: true, http_code: 403, age_sec: 1}]});
  assert.match(html('tbLock'), /锁生效/);
});

test('paused scheduling sentinels never render Invalid Date', () => {
  const {context} = dashboard();
  for (const format of [context.syncTime, context.channelTest.dateTime, context.channelTest.shortDateTime]) {
    for (const value of [2 ** 62, Infinity, 'bad timestamp']) assert.equal(format(value), '未安排');
    for (const value of [null, undefined, 0, -1]) assert.equal(format(value), '—');
    assert.doesNotMatch(format(1788552000), /Invalid Date|未安排|^—$/);
  }
});
