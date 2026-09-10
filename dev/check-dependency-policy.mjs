import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const forbidden = 'golang.org/x/crypto/openpgp';

export function checkDependencyListing(listing) {
  const packages = listing.trim().split(/\r?\n/).filter(Boolean);
  if (!packages.length) throw new Error('Empty dependency inventory; security check did not run');
  const denied = packages.filter(name => name === forbidden || name.startsWith(`${forbidden}/`));
  if (denied.length) throw new Error(`Unmaintained OpenPGP dependency forbidden (GO-2026-5932): ${denied.join(', ')}`);
  return packages.length;
}

export function checkDependencies(run = spawnSync) {
  // Include transitive and test dependencies. Match the supported production target,
  // not the developer laptop's OS. Never inspect partial output from a failed go list.
  const result = run('go', ['list', '-mod=readonly', '-deps', '-test', '-f', '{{.ImportPath}}', './...'], {
    encoding: 'utf8', maxBuffer: 16 * 1024 * 1024,
    env: { ...process.env, GOOS: 'linux', GOARCH: 'amd64', CGO_ENABLED: '0' },
  });
  if (result.error || result.status !== 0) throw new Error('go list failed; dependency policy cannot be verified');
  return checkDependencyListing(result.stdout);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    console.log(`Dependency policy passed: ${checkDependencies()} Linux production/test packages`);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
