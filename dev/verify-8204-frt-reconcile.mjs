// 独立生产源对账：页面 FRT 三级树 vs production logs 的独立聚合。
// source-*.json 必须由 dev/frt-source-reconcile 通过 monitor_ro 连接生成，
// 并与 frt-*.json 使用同一个固定 to；不能用 capacity_user_minute_samples 代替。
import assert from 'node:assert/strict';
import {existsSync, readFileSync, writeFileSync} from 'node:fs';
import path from 'node:path';

const args=process.argv.slice(2),at=args.indexOf('--dir');
assert.ok(at>=0&&args[at+1],'--dir 必须指定');
const dir=path.resolve(args[at+1]);
const outputAt=args.indexOf('--out'),output=outputAt>=0?path.resolve(args[outputAt+1]):path.join(dir,'reconcile.json');
const read=name=>JSON.parse(readFileSync(path.join(dir,name),'utf8'));
const near=(a,b)=>b===0?a===0:Math.abs(a-b)/Math.abs(b)<1e-9;
const epoch=v=>{if(Number.isFinite(Number(v)))return Number(v);const n=Date.parse(v);assert.ok(Number.isFinite(n),`无效时间 ${v}`);return Math.floor(n/1000)};
const key=(c,g,m)=>`${c}\u0000${g}\u0000${m}`;
const groupKey=(c,g)=>`${c}\u0000${g}`;
const allowedReasons=new Set(['no_valid_samples','historical_sum_unavailable','invalid_sum','historical_unsupported','invalid_histogram']);
const results=[];
const status=existsSync(path.join(dir,'frt-status.json'))?read('frt-status.json'):null;

for(const label of ['24h','3d','7d']){
  const report=read(`frt-${label}.json`),sourcePayload=read(`source-${label}.json`);
  const sourceRows=Array.isArray(sourcePayload)?sourcePayload:sourcePayload.rows;
  assert.ok(Array.isArray(sourceRows),`${label} 缺少 production.logs rows`);
  assert.equal(sourcePayload.source??'production.logs','production.logs',`${label} source 必须是 production.logs`);
  assert.equal(Number(sourcePayload.to_ts??epoch(report.to)),epoch(report.to),`${label} source 与页面不是同一个 to`);
  assert.equal(Number(sourcePayload.from_ts??epoch(report.from)),epoch(report.from),`${label} source 与页面不是同一个 from`);
  const leaf=new Map(),byGroup=new Map(),byChannel=new Map();
  let sourceSum=0,sourceObserved=0;
  const add=(map,k,row)=>{const x=map.get(k)??{sum:0,observed:0};x.sum+=row.sum_ms;x.observed+=row.observed;map.set(k,x)};
  for(const row of sourceRows){
    const r={channel_id:Number(row.channel_id),grp:String(row.grp??row.group??''),model_name:String(row.model_name??row.model??''),sum_ms:Number(row.sum_ms),observed:Number(row.observed??row.frt_sum_observed)};
    assert.ok(Number.isSafeInteger(r.channel_id),`${label} SQL channel_id 非整数`);
    assert.ok(Number.isSafeInteger(r.observed)&&r.observed>=0,`${label} SQL observed 必须是非负整数`);
    assert.ok(Number.isSafeInteger(r.sum_ms)&&r.sum_ms>=0,`${label} SQL sum_ms 必须是非负整数`);
    assert.ok(r.observed===0?r.sum_ms===0:r.sum_ms>0,`${label} SQL 总和与有效样本不一致`);
    add(leaf,key(r.channel_id,r.grp,r.model_name),r);add(byGroup,groupKey(r.channel_id,r.grp),r);add(byChannel,String(r.channel_id),r);
    sourceSum+=r.sum_ms;sourceObserved+=r.observed;
  }
  let pageSum=0,pageObserved=0,nodes=0,computable=0;
  const notComputable={},seenLeaves=new Set();
  const recordUnavailable=(node,level,want,where)=>{
    const reason=node.frt.mean_unavailable_reason||node.frt.unavailable_reason||'(missing)';
    assert.ok(allowedReasons.has(reason),`${label} ${level} ${where} 不可计算原因不受支持：${reason}`);
    assert.ok(want,`${label} ${level} ${where} 不可计算节点缺少 production.logs SQL 明细`);
    const pageObserved=Number(node.frt.observed??0);
    if(reason==='no_valid_samples'){
      assert.equal(pageObserved,0,`${label} ${level} ${where} 无有效样本节点的页面 observed 必须为 0`);
      assert.equal(want.observed,0,`${label} ${level} ${where} 无有效样本节点的 SQL observed 必须为 0`);
    }else if(reason==='historical_sum_unavailable'){
      // A missing historical sum with a valid histogram still has a real
      // denominator.  The independent source must agree on that denominator;
      // only the mean is withheld because the persisted sum is unavailable.
      assert.ok(pageObserved>0,`${label} ${level} ${where} ${reason} 必须保留有效样本数`);
      assert.equal(pageObserved,want.observed,`${label} ${level} ${where} 不可计算节点样本数与 production.logs 不一致`);
    }else if(reason==='invalid_sum'){
      // The histogram denominator is still real; only the persisted sum or
      // its denominator failed the page validity predicate.  The independent
      // source row is mandatory, but its denominator may differ: that
      // difference is exactly one of the invalid-sum signals under review.
      assert.ok(pageObserved>0,`${label} ${level} ${where} invalid_sum 必须保留有效样本数`);
    }else{
      // historical_unsupported/invalid_histogram make the local histogram
      // itself unusable.  A source row is still required, but there is no
      // page denominator to compare; the explicit reason prevents it being
      // mistaken for a zero-valued healthy node.
    }
    notComputable[reason]=(notComputable[reason]??0)+1;
  };
  for(const channel of report.channels??[]){
    const channelWant=byChannel.get(String(channel.channel_id));
    for(const group of channel.groups??[]){
      const groupName=String(group.group??''),groupWant=byGroup.get(groupKey(channel.channel_id,groupName));
      for(const model of group.models??[]){
        nodes++;
        const modelKey=key(channel.channel_id,groupName,String(model.model??'')),want=leaf.get(modelKey);
        assert.ok(want,`${label} 模型节点在 production.logs SQL 中不存在：${modelKey.replace(/\0/g,'/')}`);seenLeaves.add(modelKey);
        if(model.frt.mean_ms==null){recordUnavailable(model,'模型',want,modelKey.replace(/\0/g,'/'));continue}
        assert.ok(want.observed>0,`${label} 模型 ${modelKey.replace(/\0/g,'/')} 页面可计算但 SQL 无有效样本`);
        assert.ok(near(model.frt.mean_ms,want.sum/want.observed),`${label} 模型 ${modelKey.replace(/\0/g,'/')} 均值不一致`);
        assert.equal(model.frt.observed,want.observed,`${label} 模型 ${modelKey.replace(/\0/g,'/')} 样本数不一致`);
      }
      nodes++;
      if(group.frt.mean_ms==null)recordUnavailable(group,'分组',groupWant,`${channel.channel_id}/${groupName}`);
      else{assert.ok(groupWant,`${label} 分组 ${channel.channel_id}/${groupName} 缺少 SQL 明细`);assert.ok(groupWant.observed>0,`${label} 分组 ${channel.channel_id}/${groupName} 页面可计算但 SQL 无有效样本`);assert.ok(near(group.frt.mean_ms,groupWant.sum/groupWant.observed),`${label} 分组均值不一致`);assert.equal(group.frt.observed,groupWant.observed,`${label} 分组样本数不一致`)}
    }
    nodes++;
    if(channel.frt.mean_ms==null){recordUnavailable(channel,'渠道',channelWant,String(channel.channel_id));continue}
    computable++;assert.ok(channelWant,`${label} 渠道 ${channel.channel_id} 缺少 SQL 明细`);assert.ok(channelWant.observed>0,`${label} 渠道 ${channel.channel_id} 页面可计算但 SQL 无有效样本`);assert.ok(near(channel.frt.mean_ms,channelWant.sum/channelWant.observed),`${label} 渠道 ${channel.channel_id} 均值不一致`);assert.equal(channel.frt.observed,channelWant.observed,`${label} 渠道 ${channel.channel_id} 样本数不一致`);pageSum+=channelWant.sum;pageObserved+=channelWant.observed;
  }
  for(const sourceKey of leaf.keys())assert.ok(seenLeaves.has(sourceKey),`${label} production.logs SQL 存在页面遗漏的模型节点：${sourceKey.replace(/\0/g,'/')}`);
  assert.ok(pageSum<=sourceSum&&pageObserved<=sourceObserved,`${label} 页面有效样本不能超过 production.logs 原始聚合`);
  const sample=(report.channels??[]).find(c=>c.frt.mean_ms!=null&&c.frt.observed>1000);assert.ok(sample,`${label} 需要一个大样本渠道核对 P95/最大值`);
  for(const [name,value] of [['p95',sample.frt.p95_ms],['p99',sample.frt.p99_ms],['max',sample.frt.max_ms]])assert.notEqual(sample.frt.mean_ms,value,`${label} 均值误用了 ${name}`);
  assert.ok(sample.frt.mean_ms<=sample.frt.max_ms,`${label} 均值超过最大值`);
  if(status){
    const w=status.windows?.find(w=>w.window===label);assert.ok(w,`${label} 缺少状态汇总`);
    assert.equal(w.from_ts,epoch(report.from));assert.equal(w.to_ts,epoch(report.to));
    assert.equal(w.frt_sum_ms,pageSum,`${label} 状态总和与页面有效渠道不一致`);
    assert.equal(w.frt_sum_observed,pageObserved,`${label} 状态有效样本与页面不一致`);
    assert.equal(w.computable_channels,computable);assert.equal(w.total_channels,report.channels.length);
  }
  results.push({window:label,from:report.from,to:report.to,coverage:report.coverage?.status,nodes,channels:(report.channels??[]).length,computable,not_computable:notComputable,page_sum_ms:pageSum,page_observed:pageObserved,source_sum_ms:sourceSum,source_observed:sourceObserved,mean_ms:pageObserved?pageSum/pageObserved:null,status_checked:!!status});
  console.log(`PASS ${label}：production.logs 与页面三级节点一致；有效样本 ${pageObserved}，不可计算 ${JSON.stringify(notComputable)}`);
}
for(const label of ['24h','3d','7d']){
  const a=read(`frt-${label}.json`),b=read(`frt2-${label}.json`);
  assert.equal(a.from,b.from,`${label} 重复读取的左边界变化`);
  assert.equal(a.to,b.to,`${label} 重复读取的右边界变化`);
  // Compare the complete ordered tree, including zero-sample leaves and
  // unavailable reasons. A channel-only signature misses changed children.
  assert.deepEqual(a.channels,b.channels,`${label} 固定同一 to 的完整三级结构/指标/不可计算原因不一致`);
}
writeFileSync(output,JSON.stringify({generated_at:new Date().toISOString(),source:'production.logs',results},null,2));
console.log('PASS：production.logs 独立 SQL 三级对账完成，已写入 '+output);
