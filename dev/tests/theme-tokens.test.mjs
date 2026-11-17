import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {join, dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import vm from 'node:vm';
import {convertCss, rawLiterals, deriveLight, CSS_FILES} from '../theme-tokens.mjs';
globalThis.__palette = (() => { const s = {}; vm.runInNewContext(readFileSync(new URL('../../monitor/theme_palette.js', import.meta.url), 'utf8'), {globalThis: s, window: s}); return s.MonitorThemePalette; })();

const dev = join(dirname(fileURLToPath(import.meta.url)), '..');
const monitor = join(dev, '..', 'monitor');

test('stylesheets carry no hard-coded colors and the token file is current', () => {
  const r = spawnSync(process.execPath, [join(dev, 'theme-tokens.mjs'), '--check'], {encoding: 'utf8'});
  assert.equal(r.status, 0, r.stderr || r.stdout);
  for (const f of CSS_FILES) assert.equal(rawLiterals(readFileSync(join(monitor, f), 'utf8')), 0, f);
});

test('conversion keeps the dark color as the token and honours /*fixed*/', () => {
  const used = new Set();
  const out = convertCss('a{color:#FBBF24;background:rgba(239, 68, 68, .15);border:1px solid #fff}b{color:#0f172a/*fixed*/}', used);
  assert.equal(out, 'a{color:rgb(var(--cr-fbbf24));background:rgb(var(--cra-ef4444) / calc(.15 * var(--cr-af)));border:1px solid rgb(var(--crb-ffffff))}b{color:#0f172a/*fixed*/}');
  assert.deepEqual([...used].sort(), ['a:ef4444', 'b:ffffff', 'fbbf24', 'ffffff']);
  // Output of the earlier format is upgraded, and conversion is idempotent.
  assert.equal(convertCss('x{background:rgb(var(--cr-6366f1) / .18)}', new Set()), 'x{background:rgb(var(--cra-6366f1) / calc(.18 * var(--cr-af)))}');
  assert.equal(convertCss(out, new Set()), out);
  // Only real border/outline properties take the border token.
  assert.equal(convertCss('x{--border:#2a2d3e;border-radius:4px;outline-color:#6366f1}', new Set()),
    'x{--border:rgb(var(--cr-2a2d3e));border-radius:4px;outline-color:rgb(var(--crb-6366f1))}');
});

test('light derivation turns dark surfaces light and light text dark', () => {
  const lum = hex => { const [r, g, b] = [0, 2, 4].map(i => parseInt(hex.slice(i, i + 2), 16) / 255); return 0.2126 * r + 0.7152 * g + 0.0722 * b; };
  assert.equal(deriveLight('0f1117'), 'f5f5f5');
  assert.equal(deriveLight('1a1d27'), 'ffffff');
  for (const surface of ['151b26', '171d29', '202433', '2a2d3e']) assert.ok(lum(deriveLight(surface)) > 0.75, surface);
  const contrast = hex => {
    const lin = [0, 2, 4].map(i => parseInt(hex.slice(i, i + 2), 16) / 255).map(v => v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4);
    return 1.05 / (0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2] + 0.05);
  };
  // Text and status colors stay readable on white (>= 4:1); grey text,
  // including the dimmest, keeps >= 4.5:1 and secondary text >= 5:1.
  for (const grey of ['475569', '64748b', '8b93a7']) assert.ok(contrast(deriveLight(grey)) >= 4.5, grey);
  for (const grey of ['94a3b8', '9ca8bb', '94a0b4']) assert.ok(contrast(deriveLight(grey)) >= 5, grey);
  for (const text of ['e2e8f0', 'cbd5e1', '94a3b8', 'fbbf24', 'f59e0b', 'fca5a5', '34d399', '10b981', 'c7d2fe', 'fde68a', '60a5fa']) {
    assert.ok(contrast(deriveLight(text)) >= 4, `${text} -> ${deriveLight(text)}`);
  }
  // Slate greys stay grey: light slate text must not turn into blue text,
  // and dark slate borders must become light hairlines.
  const chromaOf = hex => { const c = [0, 2, 4].map(i => parseInt(hex.slice(i, i + 2), 16)); return Math.max(...c) - Math.min(...c); };
  for (const slate of ['e8edf7', 'dfe5ef', '9ca8bb']) assert.ok(chromaOf(deriveLight(slate)) < 20, `${slate} -> ${deriveLight(slate)}`);
  for (const border of ['30384a', '354056', '343e51']) assert.ok(lum(deriveLight(border)) > 0.7, `${border} -> ${deriveLight(border)}`);
  for (const card of ['171b26', '161c28', '181e2a']) assert.equal(deriveLight(card), 'ffffff', card);
  // The indigo accent becomes new-api's sky blue: bright as a fill, darker
  // but the same hue as text, a light tint as a border.
  const P = globalThis.__palette;
  assert.equal(P.deriveLightFill('171b26'), 'ffffff');
  const hueOf = hex => { const [r, g, b] = [0, 2, 4].map(i => parseInt(hex.slice(i, i + 2), 16)); const mx = Math.max(r, g, b), mn = Math.min(r, g, b); return mx === b ? 60 * ((r - g) / (mx - mn)) + 240 : NaN; };
  assert.ok(contrast(P.deriveLightFill('6366f1')) >= 4.5 && Math.abs(hueOf(P.deriveLightFill('6366f1')) - 205) < 4);
  for (const accent of ['6366f1', '786dff', 'dedbff']) assert.ok(Math.abs(hueOf(deriveLight(accent)) - 205) < 4, `${accent} -> ${deriveLight(accent)}`);
  assert.equal(deriveLight('000000'), '000000');
  assert.equal(deriveLight('ffffff'), 'ffffff');
});

test('chart colors are mapped in the light theme only', () => {
  const ctx = {window: {}, document: {documentElement: {getAttribute: () => 'light', setAttribute() {}}, readyState: 'loading', addEventListener() {}}, localStorage: {getItem: () => null, setItem() {}}, CustomEvent: function () {}};
  ctx.window.matchMedia = () => ({matches: false, addEventListener() {}});
  ctx.window.addEventListener = () => {};
  ctx.globalThis = ctx.window;
  vm.createContext(ctx);
  vm.runInContext(readFileSync(join(monitor, 'theme_palette.js'), 'utf8'), ctx);
  ctx.MonitorThemePalette = ctx.window.MonitorThemePalette;
  vm.runInContext(readFileSync(join(monitor, 'theme.js'), 'utf8').replace(/\bwindow\.matchMedia\b/g, 'window.matchMedia'), Object.assign(ctx, {matchMedia: ctx.window.matchMedia}));
  // No saved choice: the admin UI stays dark until a user switches.
  assert.equal(ctx.window.MonitorTheme.get(), 'dark');
  const map = ctx.window.MonitorTheme.mapColor;
  assert.equal(map('#94a3b8'), '#' + deriveLight('94a3b8'));
  assert.equal(map('rgba(239,68,68,.2)').startsWith('rgba('), true);
  assert.equal(map('rgba(239,68,68,.2)').endsWith(',.2)'), true);
  assert.equal(map('not-a-color'), 'not-a-color');
  // Inline styles built at runtime are mapped by property role; tokens and
  // /*fixed*/ colors are left alone.
  const P = ctx.window.MonitorThemePalette, L = ctx.window.MonitorTheme.lightStyle;
  assert.equal(L('color:#94a3b8'), 'color:#' + P.deriveLight('94a3b8'));
  assert.equal(L('background:#10b981;border:1px solid #6366f1'), `background:#${P.deriveLightFill('10b981')};border:1px solid #${P.deriveLightBorder('6366f1')}`);
  assert.equal(L('color:rgb(var(--cr-e2e8f0))'), 'color:rgb(var(--cr-e2e8f0))');
  assert.equal(L('color:#0f172a/*fixed*/'), 'color:#0f172a/*fixed*/');
});
