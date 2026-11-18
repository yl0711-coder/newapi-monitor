import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync, readdirSync} from 'node:fs';
import vm from 'node:vm';

// Every page defines its own HTML escape helper. Two copies had drifted:
// alerts.js left single quotes unescaped (unsafe inside a single-quoted
// attribute) and page.html rendered null as "null". Each definition must
// behave exactly like the canonical one.
const dir = new URL('../../monitor/', import.meta.url);
const files = readdirSync(dir).filter(f => /\.(js|html)$/.test(f) && !/\.min\.|production/.test(f));
const probes = [null, undefined, 0, 12.5, '', 'plain', `<a href="x" title='y'>&</a>`, `'`, `"`];
const canonical = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));

test('every esc helper escapes like the canonical implementation', () => {
  let found = 0;
  for (const f of files) {
    const src = readFileSync(new URL(f, dir), 'utf8');
    // Every helper is an arrow function ending in "...}[c]))", possibly over
    // several lines.
    for (const m of src.matchAll(/(?:const|let|var)\s+esc\s*=\s*([\s\S]*?\}\[\w+\]\)\))/g)) {
      found++;
      const fn = vm.runInNewContext(`(${m[1]})`, {String});
      for (const p of probes) {
        assert.equal(fn(p), canonical(p), `${f}: esc(${JSON.stringify(p)})`);
      }
    }
  }
  assert.ok(found >= 15, `expected to find the esc helpers, found ${found}`);
});
