import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source=name=>readFileSync(new URL(`../../monitor/${name}`,import.meta.url),'utf8');
function fixture(){
  const context=vm.createContext({window:{},Intl,nfmt:n=>Number(n).toLocaleString('en-US'),pct:n=>`${n}%`});
  vm.runInContext(source('quantile_display.js'),context);
  return context;
}
const tail={lower:10000,upper:3600000,open_tail:true,valid:true};

test('tail estimates display proven bounds, finite estimates remain explicitly approximate',()=>{
  const {format}=fixture().window.MonitorQuantiles;
  assert.equal(format(3420.5,tail,1000,'s',10),'(10, 3,600] s（分桶范围）');
  assert.equal(format(3420500,tail,1,'ms',10000),'(10,000, 3,600,000] ms（分桶范围）');
  assert.equal(format(2.7,{lower:2000,upper:3000,open_tail:false,valid:true},1000,'s',10),'≈2.7 s');
  assert.equal(format(10,null,1000,'s',10),'≈10 s');
  assert.equal(format(3420.5,null,1000,'s',10),'>10 s（旧分桶）');
  assert.equal(format(2000,null,1,'s',60),'>60 s（旧分桶）');
  assert.equal(format(10.0009,{...tail,upper:10001},1000,'s',10),'(10, 10.001] s（分桶范围）');
  for(const value of [null,undefined,'',NaN,Infinity,0,-1])assert.equal(format(value,tail,1,'ms',10000),'—');
  for(const bounds of [{...tail,valid:false},{...tail,lower:Infinity},{...tail,upper:0},{...tail,lower:-1}]){
    assert.equal(format(3420,bounds,1000,'s',10),'—');
  }
});

test('stability preserves coverage gating, exact maximum and exact slow count',()=>{
  const context=fixture();
  const fn=source('stability.js').match(/^function stabilityTTFTSummary\([^]*?^}/m);
  vm.runInContext(fn[0],context);
  const metrics={frt_observed:100,frt_p50_ms:1805000,frt_p95_ms:3420500,frt_p99_ms:3564100,
    frt_max_ms:3600000,frt_over_3s:100,frt_over_3s_pct:100,
    frt_quantiles_ms:{p50:tail,p95:tail,p99:tail}};
  const view=context.stabilityTTFTSummary(metrics,true);
  assert.equal((view.title.match(/分桶范围/g)||[]).length,3);
  assert.doesNotMatch(view.title,/3,420,500/);
  assert.match(view.detail,/最大 3,600,000 ms · 超3秒 100 次/);
  const incomplete=context.stabilityTTFTSummary(metrics,false);
  assert.equal(incomplete.title,'P50 — · P95 — · P99 —');
  assert.doesNotMatch(incomplete.detail,/3,600,000|100 次/);
});

test('stability formatter does not replace model monitoring renderers',()=>{
  const page=source('page.html');
  assert.doesNotMatch(page,/fmtFRTQuantile|fmtLatencyQuantile/);
  assert.match(page,/fmtTtft\(frtField/);
  assert.match(page,/fmtLat\(r\.p95\)/);
  assert.ok(page.indexOf('/quantile_display.js')<page.indexOf('/stability.js'));
});

test('stability FRT card separates three compact metrics and preserves coverage gating',()=>{
  const context=fixture();
  context.esc=value=>String(value).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
  context.stabilityActualWindow=()=> '2026/10/8 00:00:00 至 2026/10/9 00:00:00';
  for(const name of ['stabilityTTFTSummary','renderStabilityFRTCard']){
    vm.runInContext(source('stability.js').match(new RegExp(`^function ${name}\\([^]*?^}`,'m'))[0],context);
  }
  const metrics={frt_observed:100,frt_p50_ms:1805000,frt_p95_ms:3420500,frt_p99_ms:3564100,
    frt_max_ms:3600000,frt_over_3s:100,frt_quantiles_ms:{p50:tail,p95:tail,p99:tail}};
  for(const complete of [true,false]){
    const html=context.renderStabilityFRTCard(context.stabilityTTFTSummary(metrics,complete),{});
    for(const label of ['P50','P95','P99'])assert.match(html,new RegExp(`<dt>${label}</dt><dd>`));
    assert.equal((html.match(/<dd>/g)||[]).length,3);
    assert.equal((html.match(/<em>/g)||[]).length,3);
    assert.doesNotMatch(html,/<b[ >]/);
    assert.match(html,/有效窗口 2026\/10\/8/);
    if(complete){assert.equal((html.match(/分桶范围/g)||[]).length,3);assert.match(html,/stability-frt-values bad/);}
    else {assert.equal((html.match(/<dd>—<\/dd>/g)||[]).length,3);assert.doesNotMatch(html,/3,600,000|values bad/);}
  }
  const empty=context.renderStabilityFRTCard(context.stabilityTTFTSummary({},true),{});
  assert.match(empty,/已覆盖 · 无有效样本/);
  const injected=context.stabilityTTFTSummary({},false);
  injected.percentiles[0].value='<img src=x>';
  assert.doesNotMatch(context.renderStabilityFRTCard(injected,{}),/<img/);
  const css=source('stability.css');
  assert.match(css,/\.stability-frt-values dd\{[^}]*font-size:14px[^}]*overflow-wrap:anywhere/);
  assert.match(css,/\.stability-frt-values>div\{[^}]*minmax\(0,1fr\)/);
  assert.match(css,/\.stability-kpi b\{[^}]*font-size:27px/);
  assert.match(source('stability.js'),/join\(''\)\+renderStabilityFRTCard\(ttft,d.meta\)/);
});

test('FRT layout responds to the available content width without changing other KPI regions',()=>{
  const css=source('stability.css'),js=source('stability.js');
  assert.match(js,/<div class="stability-kpi-region"><section class="stability-kpis" id="stKpis"><\/section><\/div>/);
  assert.equal((js.match(/class="stability-kpi-region"/g)||[]).length,1);
  assert.match(css,/\.stability-kpi-region\{container:stability-kpi-area \/ inline-size;min-width:0\}/);
  assert.match(css,/repeat\(auto-fit,minmax\(min\(100%,240px\),1fr\)\)/);
  assert.match(css,/repeat\(4,minmax\(0,1fr\)\) minmax\(280px,1\.35fr\)/);
  assert.match(css,/@container stability-kpi-area \(max-width:1100px\)\{[^]*?\.stability-frt-kpi\{grid-column:1\/-1\}/);
  assert.match(css,/@container stability-kpi-area \(max-width:680px\)\{[^]*?\.stability-frt-values\{grid-template-columns:1fr\}/);
});
