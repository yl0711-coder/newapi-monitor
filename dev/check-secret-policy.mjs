import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

// Integration test of the real scanner/config, not a second implementation of its
// matching rules. Constructed high-entropy strings have no account or privileges.
const scanner = process.argv[2];
assert.ok(scanner, 'Usage: node dev/check-secret-policy.mjs /path/to/gitleaks');
const fixture = mkdtempSync(join(tmpdir(), 'monitor-secret-policy-'));
const config = fileURLToPath(new URL('../.gitleaks.toml', import.meta.url));
try {
  mkdirSync(join(fixture, 'monitor'));
  const testPath = join(fixture, 'monitor/settings_test.go');
  const dummy = ['0123456789abcdef', '0123456789abcdef'].join('');
  const scan = expected => {
    const result = spawnSync(scanner, ['dir', '--config', config, '--redact=100', '--no-banner', '--ignore-gitleaks-allow', fixture], { encoding: 'utf8' });
    // Never echo scanner output: even a broken redaction flag must not leak fixtures.
    assert.ifError(result.error);
    assert.equal(result.status, expected, 'Secret policy positive/negative control failed');
  };
  writeFileSync(testPath, `ChannelCostHMACKey: "${dummy}"\n`);
  scan(0);
  // Fixed entropy avoids a probabilistic scanner-test failure from random bytes.
  const synthetic = '0123456789abcdef'.repeat(3);
  writeFileSync(testPath, `api_key = "${synthetic}" // gitleaks:allow\n`);
  scan(1); // A different secret in an allowlisted test file remains blocked.
  writeFileSync(testPath, `ChannelCostHMACKey: "${dummy}"\n`);
  writeFileSync(join(fixture, 'monitor/settings.go'), `ChannelCostHMACKey: "${dummy}"\n`);
  scan(1); // The exempt test value is NOT exempt in production source.
  console.log('Secret policy controls passed: fixture allowed; novel secret and production key blocked');
} finally {
  rmSync(fixture, { recursive: true, force: true });
}
