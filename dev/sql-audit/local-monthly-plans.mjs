// One bounded synthetic-only diagnostic. No DSN, socket or arbitrary container
// input is accepted; never uses the production EXPLAIN runner.
import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const reports = path.join(repo, 'docs/sql-audit-20261126');
const container = 'monitor-monthly-sql-audit-20261127';
const start = Date.now();
const records = [];
const report = {
  warning: 'Actual MySQL 8.4 plans on 1500 synthetic rows, not production plans, executed business queries, branch coverage or latency benchmarks. No triggers or application workers run.',
  rows: 1500, records,
};
function sql(q) {
  if (Date.now() - start > 60000) throw new Error('whole diagnostic budget exhausted');
  const r = spawnSync('docker', ['exec', '-i', container, 'mysql', '-uroot', '--batch', '--raw', '--skip-column-names'], {
    input: q, encoding: 'utf8', timeout: 5000, maxBuffer: 512 * 1024,
  });
  if (r.error || r.status !== 0) throw new Error((r.stderr || r.error?.message || 'mysql failed').slice(0, 1500));
  return r.stdout.trim();
}
const text = s => `CONVERT(0x${Buffer.from(s).toString('hex')} USING utf8mb4)`;
const value = v => typeof v === 'number' && Number.isSafeInteger(v) ? String(v) : typeof v === 'string' ? text(v) : (() => {throw new Error('unsupported synthetic parameter');})();
try {
  report.version = sql('SELECT VERSION();');
  sql(`CREATE DATABASE monthly_sql_audit;
USE monthly_sql_audit;
CREATE TABLE users(id BIGINT PRIMARY KEY, created_at BIGINT, quota BIGINT, used_quota BIGINT);
CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT,created_at BIGINT,type INT,quota BIGINT,token_id BIGINT,token_name VARCHAR(64),content TEXT,other TEXT,
KEY idx_user_created_type(user_id,created_at,type), KEY idx_created_at_type(created_at,type));`);
  const rows = Array.from({length: 1500}, (_, n) => {
    const i = n + 1;
    return `(${i},${201 + i % 200},${1790784000 + Math.floor(i / 10)},${i % 7 + 1},${i % 5},${i % 3 + 1},'audit','synthetic',${text('{"billing_source":"wallet"}')})`;
  });
  sql('USE monthly_sql_audit; INSERT INTO users VALUES(201,1788192000,1000,200); INSERT INTO logs VALUES' + rows.join(',') + '; ANALYZE TABLE logs;');
  const {Cases} = JSON.parse(fs.readFileSync(path.join(reports, 'monthlysource-catalog-20261127.json'), 'utf8'));
  for (const c of Cases) {
    const row = {id: c.ID, owner: c.Owner, variant: c.Variant, status: 'not_explained'};
    records.push(row);
    const binds = c.Args.map((v, i) => `SET @p${i}=${value(v)};`).join('\n');
    const using = c.Args.length ? ' USING ' + c.Args.map((_, i) => `@p${i}`).join(',') : '';
    const q = `USE monthly_sql_audit; ${binds}
SET @q=${text('EXPLAIN FORMAT=JSON ' + c.SQL)};
PREPARE inspected FROM @q; EXECUTE inspected${using}; DEALLOCATE PREPARE inspected;`;
    row.plan = JSON.parse(sql(q));
    row.status = 'explained_isolated_mysql';
  }
  report.stopped = false;
} catch (e) {
  report.stopped = true;
  report.error = e.message;
  process.exitCode = 1;
} finally {
  report.elapsed_ms = Date.now() - start;
  fs.writeFileSync(path.join(reports, 'monthlysource-isolated-plans-20261127.json'), JSON.stringify(report, null, 2) + '\n', {mode: 0o600});
  console.log(JSON.stringify({cases: records.length, explained: records.filter(x => x.status === 'explained_isolated_mysql').length, stopped: report.stopped, error: report.error}));
}
