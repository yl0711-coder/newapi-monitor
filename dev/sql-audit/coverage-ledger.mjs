// Read retained evidence once, entry by entry. Does not open any database,
// rerun applications or duplicate the large raw capture files.
import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';

export async function scanCaptureLines(lines, onEntry) {
  let header = [], metadata, active = false, ended = false, closed = false, object = null, count = 0;
  for await (const line of lines) {
    if (!active) {
      if (/^  "entries": (\[|null)/.test(line)) {
        metadata = JSON.parse(header.join('\n') + '\n  "entries": []\n}');
        active = true;
        if (/\[\]|null/.test(line)) ended = true;
      } else header.push(line);
      continue;
    }
    if (ended) {
      if (!closed && line.trim()==='}') closed=true;
      else if (line.trim()!=='') throw new Error('unexpected data after capture entries');
      continue;
    }
    if (object === null && line === '    {') object = [];
    if (object !== null) {
      object.push(line);
      if (object.reduce((n, s) => n + s.length, 0) > 2 * 1024 * 1024) throw new Error('capture entry exceeds bounded parser budget');
      if (/^    }[,]?$/.test(line)) {
        const entry = JSON.parse(object.join('\n').replace(/,\s*$/, ''));
        if (typeof entry.file !== 'string' || typeof entry.function !== 'string' || typeof entry.status !== 'string') throw new Error('invalid capture entry');
        await onEntry(entry, metadata);
        count++; object = null;
      }
    } else if (line === '  ]') ended = true;
    else if (line.trim() !== '') throw new Error('unexpected capture layout');
  }
  const expected = Object.values(metadata?.counts ?? {}).reduce((n, v) => n + v, 0);
  if (!active || !ended || !closed || object !== null || count !== expected) throw new Error(`incomplete capture: parsed ${count}, expected ${expected}`);
  return {test_exit: metadata.test_exit, records: count};
}

export function functionName(value) {
  // Runtime generic instantiations embed fully qualified type names (and
  // nested []/map brackets). Their dots are not the function-name separator.
  let plain='',depth=0;
  for(const char of value){
    if(char==='['){depth++;continue;}
    if(char===']'){
      if(depth===0)return '__unresolved_generic_runtime_name__';
      depth--;continue;
    }
    if(depth===0)plain+=char;
  }
  if(depth!==0)return '__unresolved_generic_runtime_name__';
  return plain.replace(/\.func\d+(?:\.\d+)*$/, '').split('.').at(-1);
}

// Keep the original broad inventory/statuses for traceability. A Raw builder
// and its later Scan are not two executed queries; preparation/control/writes
// also need their own review, not a fake read-query completion percentage.
export function executionRole(e) {
  if(e.kind==='raw_builder')return 'sql_builder_not_execution';
  if(e.kind==='connection_access')return 'connection_management';
  if(e.kind==='orm_write_candidate'||e.kind==='execute_candidate')return 'write_or_command_execution_candidate';
  if(e.kind==='database_sql_candidate'){
    if(['Prepare','PrepareContext'].includes(e.api))return 'statement_preparation_not_execution';
    if(e.operation==='EXPLAIN')return 'diagnostic_query_execution';
    return 'read_execution_candidate';
  }
  if(e.kind==='orm_read_candidate')return 'read_execution_candidate';
  return 'unresolved_role';
}

export function resolveStaticGaps(repo) {
  const dir=path.join(repo,'docs/sql-audit-20261126');
  const read=n=>JSON.parse(fs.readFileSync(path.join(dir,n),'utf8'));
  const old=read('internal-static-plans-20261127.json');
  const current=read('internal-static-plans-current-20261127.json');
  const mysql=read('resolved-mysql-plans-20261127.json');
  if(mysql.stopped)throw new Error('cannot close gaps from stopped MySQL diagnostic');
  const local=read('missing-monitor-schema-plans-20261127.json');
  const batches=read('usage-month-dynamic-batch-20261127.json');
  const tasks=read('task-load-columns-20261127.json');
  const heads=read('funding-heads-current-plan-20261127.json');
  const normalize=s=>s.replace(/\s+/g,' ').trim();
  const sourceHashes={},entries=[];
  for(const e of current.entries.filter(e=>['requires_manual_resolution','formatted_template_requires_rendering'].includes(e.status))){
    const source=fs.readFileSync(path.join(repo,e.file),'utf8');
    // Adjacent Go constants can resolve to a fragment not present as one
    // literal substring. Require the fresh AST result, not a loose prefix.
    if(!current.entries.some(r=>r.file===e.file&&r.source_sql===e.source_sql))throw new Error('static gap expression changed: '+e.file);
    sourceHashes[e.file]=createHash('sha256').update(source).digest('hex');
    let evidence=[],resolution;
    if(e.status==='formatted_template_requires_rendering'){
      const owner=e.file.endsWith('/wallet.go')?'CaptureWallet':e.file.endsWith('/registered_wallet.go')?'CaptureRegisteredWallet':'ReadRegistrationTimes';
      evidence=mysql.records.map((r,i)=>({r,i})).filter(({r})=>r.owner===owner&&r.variant.startsWith('actual-call/')&&r.rows===Number(r.variant.split('members')[1])).map(({i})=>'resolved-mysql-plans-20261127.json#records/'+i);
      if(evidence.length!==2)throw new Error('batch size cases missing');
      resolution='actual successful entry point captured; members 1/200 rendered, MySQL EXPLAIN and synthetic metadata returned';
    }else{
      local.Results.forEach((r,i)=>{if(r.SQL===e.source_sql)evidence.push('missing-monitor-schema-plans-20261127.json#Results/'+i)});
      if(evidence.length)resolution='exact source SQL explained with actual GORM models in memory';
      mysql.records.forEach((r,i)=>{if(r.sql===e.source_sql)evidence.push('resolved-mysql-plans-20261127.json#records/'+i)});
      if(!resolution&&evidence.length)resolution='exact MySQL metadata statement explained and executed on synthetic source';
      if(e.file.endsWith('/usage_month_read.go')){
        if(batches.Results.length!==2||batches.Results[1].Parameters!==128||batches.Results[1].Returned!==128||!batches.Results.every(r=>r.SQL.startsWith(e.source_sql)))throw new Error('dynamic IN batch evidence missing');
        evidence=['usage-month-dynamic-batch-20261127.json'];resolution='actual loadUsageMonthBatch with 1/64 pairs, full values and indexed plans; old item was a SQL prefix';
      }
      if(e.file==='internal/monthlytasks/store.go'){
        if(tasks.Queries.length!==2||!tasks.Queries.every(q=>q.startsWith(e.source_sql)))throw new Error('allowed task columns evidence missing');
        evidence=['task-load-columns-20261127.json'];resolution='actual load() executed for both allowed columns with complete record equality and exact-key plans';
      }
      if(e.file==='internal/monthlysource/funding_month_heads.go'){
        if(heads.Parameters!==62||!normalize(heads.SQL).startsWith(normalize(e.source_sql))||heads.SourceSHA256!==sourceHashes[e.file])throw new Error('current head builder not bound to tested source');
        evidence=['funding-heads-current-plan-20261127.json'];resolution='new complete 62-scan builder, latest-head exact seeks and preserved results; not an executable SQL prefix';
      }
      if(!evidence.length){
        evidence=mysql.records.map((r,i)=>({r,i})).filter(({r})=>normalize(r.sql).includes(normalize(e.source_sql))).map(({i})=>'resolved-mysql-plans-20261127.json#records/'+i);
        if(evidence.length)resolution='prefix/CTE fragment resolved to actual complete builder output and MySQL EXPLAIN; not a broken product SQL';
      }
    }
    if(!resolution||!evidence.length)throw new Error('unresolved static gap: '+e.file+':'+e.line);
    const previous=old.entries.find(r=>r.file===e.file&&r.source_sql===e.source_sql);
    entries.push({file:e.file,line:e.line,previous_status:previous?.status??'new_current_fragment',parser_status:e.status,resolution,evidence});
  }
  if(entries.length!==18)throw new Error('static gap list changed');
  const output=path.join(dir,'static-gap-closure-20261127.json');
  fs.writeFileSync(output,JSON.stringify({warning:'Only the 18 named current static-parser gaps (17 inherited + one new head-query fragment) are resolved here. Not all SQL sites/branches/performance coverage. Original parser statuses and evidence pointers retained.',source_sha256:sourceHashes,entries},null,2)+'\n',{mode:0o600});
  return {closed:entries.length,output};
}

export async function buildLedger(repo) {
  const reports = path.join(repo, 'docs/sql-audit-20261126');
  const inventory = JSON.parse(fs.readFileSync(path.join(reports, 'inventory-execution-sites-20261127.json'), 'utf8'));
  const artifacts = [], witnesses = new Map();
  const currentHashes = new Map();
  for (const name of fs.readdirSync(reports).filter(n => /^capture-.*\.json$/.test(n)).sort()) {
    const lines = readline.createInterface({input: fs.createReadStream(path.join(reports, name), {encoding: 'utf8', highWaterMark: 65536}), crlfDelay: Infinity});
    const summary = await scanCaptureLines(lines, (e, header) => {
      if (header.test_exit !== 0 || !['explained', 'explain_error'].includes(e.status)) return;
      const key = e.file + '|' + functionName(e.function);
      let w = witnesses.get(key);
      if (!w) witnesses.set(key, w = {artifacts: new Set(), templates: new Set(), fixtures: new Set(), lines: new Set(), freshLines:new Set(), states: new Set()});
      w.artifacts.add(name); w.templates.add(e.sql_sha256); w.lines.add(e.line); w.states.add(e.status);
      if (header.source_sha256?.[e.file] && /^monitor\/[a-z0-9_.-]+\.go$/i.test(e.file)) {
        if (!currentHashes.has(e.file)) currentHashes.set(e.file, createHash('sha256').update(fs.readFileSync(path.join(repo,e.file))).digest('hex'));
        if (currentHashes.get(e.file)===header.source_sha256[e.file] && e.status==='explained' && /^(SELECT|WITH)\b/i.test(e.redacted_template.trim())) w.freshLines.add(e.line);
      }
      if (w.fixtures.size < 5) w.fixtures.add(e.fixture);
    });
    artifacts.push({file: name, ...summary});
  }
  const functions = [...witnesses].map(([key, w]) => ({key, artifacts: [...w.artifacts], template_count: w.templates.size, sample_fixtures: [...w.fixtures], prior_lines: [...w.lines].sort((a,b)=>a-b), current_source_healthy_lines:[...w.freshLines].sort((a,b)=>a-b), states: [...w.states]}));
  const sites = inventory.entries.map(e => {
    const read = e.kind === 'orm_read_candidate' || e.kind === 'database_sql_candidate' || e.kind === 'raw_builder' && /^(SELECT|WITH)$/.test(e.operation);
    const key = e.file + '|' + functionName(e.function), w = witnesses.get(key);
    const status = !read ? 'not_classified_as_read_execution' : w?.freshLines.has(e.line) ? 'current_source_callsite_witness_not_all_branches' : w?.freshLines.size ? 'current_source_same_function_only' : w ? 'historical_function_witness_only' : 'no_function_witness_requires_resolution';
    return {id:e.id, file:e.file, line:e.line, function:e.function, api:e.api, kind:e.kind, operation:e.operation, execution_role:executionRole(e), source_hint:e.source_hint, status, witness_key:w ? key : undefined};
  });
  const counts = Object.fromEntries([...new Set(sites.map(e => e.status))].sort().map(s => [s, sites.filter(e=>e.status===s).length]));
  const role_counts=Object.fromEntries([...new Set(sites.map(e=>e.execution_role))].sort().map(r=>[r,sites.filter(e=>e.execution_role===r).length]));
  const reads=sites.filter(e=>e.execution_role==='read_execution_candidate');
  const read_execution_counts=Object.fromEntries([...new Set(reads.map(e=>e.status))].sort().map(s=>[s,reads.filter(e=>e.status===s).length]));
  const payload = {warning:'Execution/API call-site triage, not unique SQL, complete branch coverage, source-version binding or a pass rate. Original broad counts retained; execution_role excludes builders/preparations/diagnostics from read execution candidates without marking them reviewed. Old captured plans are function-level leads only. No database accessed; parser checks full artifact counts.', inventory:'inventory-execution-sites-20261127.json', counts, role_counts, read_execution_counts, artifacts, functions, sites};
  const output = path.join(reports, 'coverage-ledger-20261127.json');
  fs.writeFileSync(output, JSON.stringify(payload, null, 2) + '\n', {mode:0o600});
  return {counts,role_counts,read_execution_counts, functions:functions.length, artifacts:artifacts.length, output};
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
  console.log(JSON.stringify({ledger:await buildLedger(repo),static_gaps:resolveStaticGaps(repo)}));
}
