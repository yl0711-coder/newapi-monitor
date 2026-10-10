import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const workflow = readFileSync(new URL('../../.github/workflows/ci.yml', import.meta.url), 'utf8').replace(/\r\n/g, '\n');
const job = name => workflow.split(`\n  ${name}:\n`)[1]?.split(/^  [\w-]+:/m)[0] ?? '';

test('all release artifacts wait for secret, test and image gates', () => {
  for (const name of ['build-push', 'build-nginx-ops-tools']) {
    assert.match(job(name), /needs: \[test, race-monitor, secrets, image-security\]/);
    assert.match(job(name), /if: github.event_name != 'pull_request'/);
  }
  assert.match(job('secrets'), /fetch-depth: 0/);
  assert.match(job('secrets'), /--redact=100.*--ignore-gitleaks-allow.*--log-opts="--all"/);
  assert.match(job('secrets'), /check-secret-policy\.mjs/);
  assert.match(job('test'), /check-dependency-policy\.mjs/);
  assert.match(job('test'), /test_ecs_cutover_preflight\.py/);
  assert.ok(job('test').includes("-p 'test_ecs_task*.py'"));
  assert.ok(job('test').includes("-p 'test_ecs_acceptance*.py'"));
});

test('image scanner blocks unfixed high/critical findings before artifact upload; PR builds are scanned', () => {
  const scan = job('image-security');
  assert.match(scan, /load: true\s+push: false/);
  assert.match(scan, /severity: HIGH,CRITICAL\s+ignore-unfixed: false\s+exit-code: '1'/);
  assert.match(scan, /severity: UNKNOWN,LOW,MEDIUM/);
  assert.ok(scan.indexOf('Block high and critical') < scan.indexOf('actions/upload-artifact'));
  assert.ok(scan.indexOf("if: github.event_name != 'pull_request'") > scan.indexOf('Report remaining'));
  assert.doesNotMatch(scan, /packages: write|continue-on-error|docker push/);
  assert.equal((scan.match(/uses: aquasecurity\/trivy-action@[a-f0-9]{40}/g) ?? []).length, 2);
  for (const image of ['newapi-monitor', 'newapi-monitor-hostagent', 'newapi-monitor-nginxcollector',
    'newapi-monitor-ecslogagent', 'newapi-monitor-nginxcollector-ecs']) {
    assert.ok(scan.includes(`image: ${image}\n`));
  }
});

test('publisher loads the scanned artifact and cannot rebuild or push unrelated tags', () => {
  const publish = job('build-push');
  assert.match(publish, /actions\/download-artifact/);
  assert.match(publish, /docker load -i/);
  assert.match(publish, /docker tag "monitor-security:\$IMAGE_NAME" "\$tag"/);
  assert.match(publish, /docker push "\$tag"/);
  assert.doesNotMatch(publish, /docker\/build-push-action|docker build|--all-tags/);
});

test('all production images use the same patched Go build version as CI', () => {
  const patchedVersion = '1.26.9';
  const read = file => readFileSync(new URL(`../../${file}`, import.meta.url), 'utf8');
  assert.equal(read('go.mod').match(/^go (\S+)$/m)?.[1], patchedVersion);
  const ciVersions = [...workflow.matchAll(/go-version:\s*'([^']+)'/g)];
  assert.ok(ciVersions.length > 0, 'CI must retain explicit toolchain pins');
  for (const [, version] of ciVersions) assert.equal(version, patchedVersion);
  for (const file of ['Dockerfile', 'cmd/hostagent/Dockerfile', 'cmd/nginxcollector/Dockerfile',
    'cmd/ecslogagent/Dockerfile', 'cmd/nginxcollector/Dockerfile.ecs-isolated', 'cmd/ecssynthetic/Dockerfile']) {
    const dockerfile = read(file);
    const builders = [...dockerfile.matchAll(/^(?:FROM |ARG BUILDER_IMAGE=)golang:([^\s]+)/gm)];
    assert.ok(builders.length > 0, `${file}: pinned default builder required`);
    for (const [, image] of builders) assert.equal(image, `${patchedVersion}-alpine3.23`, file);
    if (file !== 'cmd/ecssynthetic/Dockerfile') assert.match(dockerfile, /FROM .* AS runtime/);
  }
  assert.match(job('image-security'), /labels: \$\{\{ steps.meta.outputs.labels \}\}/);
  assert.match(job('image-security'), /pull: true\s+no-cache-filters: runtime/);
});

test('Docker context allowlist includes observability sources and the embedded schema', () => {
  const rules = readFileSync(new URL('../../.dockerignore', import.meta.url), 'utf8')
    .split(/\r?\n/);
  for (const rule of [
    '!internal/observability/',
    '!internal/observability/*.go',
    '!internal/observability/schema/',
    '!internal/observability/schema/observability.v1.schema.json',
  ]) {
    assert.ok(rules.includes(rule), 'missing production context rule: ' + rule);
  }
});

test('Docker context audit checks observability assets and excluded fixtures', () => {
  const audit = job('test').split('- name: Verify Docker build context isolation')[1] ?? '';
  for (const file of [
    'internal/observability/contract.go',
    'internal/observability/identifiers.go',
    'internal/observability/metrics.go',
    'internal/observability/schema.go',
    'internal/observability/schema/observability.v1.schema.json',
  ]) {
    assert.ok(audit.includes('test -e /context/' + file), 'missing context inclusion check: ' + file);
  }
  for (const file of [
    'internal/observability/ci-probe_test.go',
    'internal/observability/ci-local-config.json',
    'internal/observability/schema/ci-local-config.json',
  ]) {
    assert.ok(audit.includes('touch ' + file), 'missing excluded context fixture: ' + file);
    assert.ok(audit.includes('test ! -e /context/' + file), 'missing context exclusion check: ' + file);
  }
});
