import { execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';

// Only the numbered local 8204 releases are covered by the retention policy.
// No container, volume, build cache or other repository tag is removed here.
const args = process.argv.slice(2);
let apply = false;
let output;
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--apply') apply = true;
  else if (args[i] === '--out' && args[i + 1] && !args[i + 1].startsWith('--')) output = args[++i];
  else throw new Error('Usage: node dev/cleanup-8204-images.mjs [--apply] [--out result.json]');
}

function docker(...args) {
  return execFileSync('docker', args, {
    encoding: 'utf8', windowsHide: true, timeout: 60_000,
    maxBuffer: 8 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'],
  }).trim();
}

function containers() {
  const ids = docker('ps', '-aq').split(/\s+/).filter(Boolean);
  if (!ids.length) return [];
  return JSON.parse(docker('inspect', ...ids)).map(c => ({
    id: c.Id, name: c.Name, image: c.Config.Image, image_id: c.Image,
    started_at: c.State.StartedAt, running: c.State.Running,
    health: c.State.Health?.Status ?? null,
  })).sort((a, b) => a.id.localeCompare(b.id));
}

function versions() {
  return docker('image', 'ls', 'newapi-monitor', '--no-trunc', '--format', '{{json .}}')
    .split('\n').filter(Boolean).map(line => JSON.parse(line))
    .filter(r => r.Repository === 'newapi-monitor' && /^v[1-9][0-9]*$/.test(r.Tag))
    .map(r => ({ tag: `newapi-monitor:${r.Tag}`, version: BigInt(r.Tag.slice(1)), id: r.ID }))
    .sort((a, b) => a.version > b.version ? -1 : a.version < b.version ? 1 : 0)
    .map(({ version, ...row }) => row);
}

function signature(rows) {
  return JSON.stringify(rows.map(({ health, ...row }) => row));
}

function assertUnchanged(before, after) {
  if (signature(before) !== signature(after)) throw new Error('Container identity or start state changed; cleanup stopped.');
  for (const c of before) {
    if (c.health === 'healthy' && after.find(a => a.id === c.id)?.health !== 'healthy') {
      throw new Error(`Container health changed: ${c.name}; cleanup stopped.`);
    }
  }
}

function references(image, rows) {
  return rows.filter(c => c.image_id === image.id || c.image === image.tag).map(c => c.name);
}

const report = { at: new Date().toISOString(), apply, keep_count: 4, removed: [], blocked: [] };
try {
  report.containers_before = containers();
  report.versions_before = versions();
  report.keep = report.versions_before.slice(0, 4);
  report.candidates = report.versions_before.slice(4);
  report.blocked = report.candidates.map(c => ({ ...c, containers: references(c, report.containers_before) }))
    .filter(c => c.containers.length);
  if (apply) {
    const monitor = report.containers_before.find(c => c.name === '/newapi-monitor-prod-readonly-monitor-1');
    if (!monitor?.running || monitor.health !== 'healthy') throw new Error('8204 Monitor must be running and healthy before cleanup.');
    for (const candidate of report.candidates) {
      if (report.blocked.some(c => c.tag === candidate.tag)) continue;
      const currentContainers = containers();
      assertUnchanged(report.containers_before, currentContainers);
      if (references(candidate, currentContainers).length) throw new Error(`Image is now referenced: ${candidate.tag}`);
      const currentVersions = versions();
      if (JSON.stringify(currentVersions.slice(0, 4)) !== JSON.stringify(report.keep)) {
        throw new Error('Latest four images changed; rerun the cleanup preview.');
      }
      if (currentVersions.find(c => c.tag === candidate.tag)?.id !== candidate.id) {
        throw new Error(`Image tag changed: ${candidate.tag}`);
      }
      // Intentionally omit --force so Docker also protects new references.
      const result = docker('image', 'rm', candidate.tag);
      report.removed.push({ ...candidate, result });
      if (versions().some(c => c.tag === candidate.tag)) throw new Error(`Tag still exists: ${candidate.tag}`);
    }
  }
  report.containers_after = containers();
  assertUnchanged(report.containers_before, report.containers_after);
  report.versions_after = versions();
  report.retention_met = report.versions_after.length <= 4;
  report.status = apply && !report.retention_met ? 'blocked' : apply ? 'complete' : 'preview';
  if (report.status === 'blocked') process.exitCode = 2;
} catch (error) {
  report.status = 'failed';
  report.error = error.message;
  process.exitCode = 1;
} finally {
  const json = JSON.stringify(report, null, 2) + '\n';
  if (output) writeFileSync(output, json);
  console.log(JSON.stringify({
    status: report.status, apply, keep: report.keep, candidates: report.candidates,
    removed: report.removed, blocked: report.blocked, error: report.error,
    retention_met: report.retention_met, evidence: output,
  }, null, 2));
}
