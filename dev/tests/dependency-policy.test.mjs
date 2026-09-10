import assert from 'node:assert/strict';
import test from 'node:test';
import { checkDependencies, checkDependencyListing } from '../check-dependency-policy.mjs';

test('OpenPGP gate allows used crypto packages but rejects obsolete package and children', () => {
  assert.equal(checkDependencyListing('fmt\ngolang.org/x/crypto/bcrypt\ngolang.org/x/crypto/openpgp-other\n'), 3);
  for (const suffix of ['', '/packet', '/armor', '/errors']) {
    assert.throws(() => checkDependencyListing(`fmt\ngolang.org/x/crypto/openpgp${suffix}\n`), /forbidden/);
  }
  assert.throws(() => checkDependencyListing(' \n'), /Empty/);
});

test('OpenPGP gate fails closed when Go is missing or inventory is partial', () => {
  for (const result of [{ error: new Error('ENOENT') }, { status: 1, stdout: 'fmt' }, { status: null, stdout: 'fmt' }]) {
    assert.throws(() => checkDependencies(() => result), /go list failed/);
  }
  assert.equal(checkDependencies((command, args, options) => {
    assert.equal(command, 'go');
    assert.ok(args.includes('-deps') && args.includes('-test') && args.includes('-mod=readonly'));
    assert.equal(options.env.GOOS, 'linux');
    assert.equal(options.env.GOARCH, 'amd64');
    return { status: 0, stdout: 'fmt\n' };
  }), 1);
});
