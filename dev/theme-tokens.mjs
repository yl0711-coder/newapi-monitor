#!/usr/bin/env node
// Theme color tokens for the Monitor admin UI (light / dark / follow system).
//
// The admin pages were written for a dark background with color literals
// spread across many stylesheets. This tool replaces each CSS color literal
// with a token reference and generates monitor/theme_tokens.css:
//
//   #fbbf24                 -> rgb(var(--cr-fbbf24))
//   rgba(239,68,68,.15)     -> rgb(var(--cr-ef4444) / .15)
//
// The token name is the original dark color, so the dark theme renders exactly
// as before. The light value is derived by deriveLight() and can be pinned in
// LIGHT_OVERRIDES. Only CSS contexts are rewritten: .css files, <style> blocks
// and style="..." attributes (also inside JS template strings). Chart colors in
// JS are mapped at runtime by theme.js using the same table.
//
// usage: node dev/theme-tokens.mjs           rewrite sources + regenerate tokens
//        node dev/theme-tokens.mjs --check   fail if anything is unconverted/stale
import {readFileSync, writeFileSync} from 'node:fs';
import {join, dirname} from 'node:path';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..', 'monitor');

// Admin UI only. The customer usage portal (portal*.html) keeps its own look.
export const CSS_FILES = ['stability.css', 'capacity.css', 'group_governance.css', 'usage_logs.css', 'finance.css',
  'model_statistics.css', 'model_frt.css', 'token_anomaly.css', 'incident.css'];
export const HTML_FILES = ['page.html', 'login.html', 'alert.html'];
// JS files that build markup with inline style="..." attributes.
export const JS_FILES = ['usage_logs.js', 'alerts.js', 'capacity.js', 'channel_data_status.js', 'channel_management.js',
  'channel_pricing_observations.js', 'customer_health.js', 'ecs_log_sources.js', 'finance.js', 'group_governance.js',
  'incident.js', 'infra_assets.js', 'logchain.js', 'model_frt.js', 'model_report_dates.js', 'model_statistics.js',
  'quantile_display.js', 'range_picker.js', 'stability.js', 'token_anomaly.js', 'view_refresh.js'];
export const TOKENS_FILE = 'theme_tokens.css';

// The light derivation lives in monitor/theme_palette.js, shared with the
// browser (theme.js maps chart colors with the same function).
const palette = (() => {
  const sandbox = {};
  vm.runInNewContext(readFileSync(join(root, 'theme_palette.js'), 'utf8'), {globalThis: sandbox, window: sandbox});
  return sandbox.MonitorThemePalette;
})();
export const LIGHT_OVERRIDES = palette.overrides;
export const deriveLight = palette.deriveLight;

const HEX = /#([0-9a-fA-F]{6}|[0-9a-fA-F]{3})(?![0-9a-fA-F\w])/g;
const RGB = /rgba?\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*(?:,\s*([0-9.]+%?)\s*)?\)/g;

export function normHex(h) {
  h = h.toLowerCase();
  return h.length === 3 ? h.split('').map(c => c + c).join('') : h;
}
const toHex = (r, g, b) => palette.toHex(r, g, b);
const fromHex = h => palette.fromHex(h);

// Transparent uses get their own token (--cra-*) and an alpha scaled by
// --cr-af, because tints and overlays need different light values than
// solid text of the same dark color (see deriveLightAlpha).
const ref = (hex, alpha) => alpha === undefined ? `rgb(var(--cr-${hex}))` : `rgb(var(--cra-${hex}) / calc(${alpha} * var(--cr-af)))`;
// Earlier output of this tool, upgraded in place.
const LEGACY_ALPHA = /rgb\(var\(--cr-([0-9a-f]{6})\) \/ ([0-9.]+%?)\)/g;

// A literal followed by /*fixed*/ keeps the same color in both themes (for
// elements designed as light chips on the dark UI, e.g. date chips).
const FIXED = '/*fixed*/';
const isFixed = (text, end) => text.startsWith(FIXED, end);

// Convert color literals in a CSS fragment.
export function convertCss(css, used) {
  css = css.replace(LEGACY_ALPHA, (m, hex, a) => ref(hex, a));
  css = css.replace(RGB, (m, r, g, b, a, off, all) => {
    if (isFixed(all, off + m.length)) return m;
    const hex = toHex(+r, +g, +b);
    used.add(a === undefined ? hex : 'a:' + hex);
    return ref(hex, a);
  });
  css = css.replace(HEX, (m, h, off, all) => {
    if (isFixed(all, off + m.length)) return m;
    const hex = normHex(h);
    used.add(hex);
    return ref(hex);
  });
  // Solid borders and outlines get a border-role token (--crb-*): a colored
  // border that was bright on dark becomes a soft tint line on white
  // instead of a dark outline.
  css = css.replace(BORDER_DECL, (m, lead, prop, value) =>
    lead + prop + ':' + value.replace(/var\(--cr-([0-9a-f]{6})\)/g, (r, hex) => { used.add('b:' + hex); return `var(--crb-${hex})`; }));
  // Solid background fills get a fill-role token (--crg-*): a colored fill
  // stays a bright accent (white text on it) instead of the darker tone used
  // for colored text.
  return css.replace(BG_DECL, (m, lead, prop, value) =>
    lead + prop + ':' + value.replace(/var\(--cr-([0-9a-f]{6})\)/g, (r, hex) => { used.add('g:' + hex); return `var(--crg-${hex})`; }));
}
const BG_DECL = /(^|[{;\s"'])(background(?:-color|-image)?)\s*:([^;}"']*)/g;
const BORDER_DECL = /(^|[{;\s"'])((?:border|outline)(?:-[a-z]+)*)\s*:([^;}"']*)/g;

// Raw literals that are neither converted nor marked fixed.
export function rawLiterals(text) {
  let n = 0;
  for (const re of [HEX, RGB]) for (const m of text.matchAll(re)) if (!isFixed(text, m.index + m[0].length)) n++;
  return n;
}

// Convert inside <style> blocks and style="..." / style='...' attributes only.
export function convertMarkup(text, used) {
  text = text.replace(/(<style[^>]*>)([\s\S]*?)(<\/style>)/g, (m, a, body, z) => a + convertCss(body, used) + z);
  return text.replace(/(\bstyle=)(["'])([^"'`]*?)\2/g, (m, a, q, body) => a + q + convertCss(body, used) + q);
}

export function tokensCss(used) {
  const all = [...used].sort();
  const solid = all.filter(k => !/^[abg]:/.test(k));
  const alpha = all.filter(k => k.startsWith('a:')).map(k => k.slice(2));
  const border = all.filter(k => k.startsWith('b:')).map(k => k.slice(2));
  const fill = all.filter(k => k.startsWith('g:')).map(k => k.slice(2));
  const decl = (prefix, hexes, fn) => hexes.map(h => `--${prefix}-${h}:${fromHex(fn(h)).join(' ')}`);
  const dark = [...decl('cr', solid, h => h), ...decl('cra', alpha, h => h), ...decl('crb', border, h => h), ...decl('crg', fill, h => h), '--cr-af:1'].join(';');
  const light = [...decl('cr', solid, deriveLight), ...decl('cra', alpha, palette.deriveLightAlpha),
    ...decl('crb', border, palette.deriveLightBorder),
    ...decl('crg', fill, palette.deriveLightFill),
    `--cr-af:${palette.lightAlphaScale}`].join(';');
  return `/* Generated by dev/theme-tokens.mjs — do not edit by hand. Token = original dark color. */\n` +
    `:root{color-scheme:dark;${dark}}\n` +
    `:root[data-theme="light"]{color-scheme:light;${light}}\n`;
}

// Light value table for runtime use (chart colors in JS): dark hex -> light hex.
export function lightMap(used) {
  const map = {};
  for (const h of [...used].sort()) map[h] = deriveLight(h);
  return map;
}

function scanUsed(text, used) {
  for (const m of text.matchAll(/--cr([abg]?)-([0-9a-f]{6})/g)) used.add(m[1] ? m[1] + ':' + m[2] : m[2]);
}

function run(check) {
  const used = new Set();
  const changed = [];
  const apply = (file, fn) => {
    const path = join(root, file);
    const before = readFileSync(path, 'utf8');
    const after = fn(before, used);
    scanUsed(after, used);
    if (after !== before) {
      changed.push(file);
      if (!check) writeFileSync(path, after);
    }
  };
  for (const f of CSS_FILES) apply(f, convertCss);
  for (const f of [...HTML_FILES, ...JS_FILES]) apply(f, convertMarkup);
  // Stylesheets must hold no raw color literal at all.
  const raw = [];
  for (const f of CSS_FILES) {
    const text = readFileSync(join(root, f), 'utf8');
    const n = rawLiterals(text);
    if (n && !check) continue;
    if (n) raw.push(`${f}: ${n} raw color literal(s)`);
  }
  if (raw.length) changed.push(...raw);
  const tokens = tokensCss(used);
  const tokensPath = join(root, TOKENS_FILE);
  let current = '';
  try { current = readFileSync(tokensPath, 'utf8'); } catch {}
  if (tokens !== current) {
    changed.push(TOKENS_FILE);
    if (!check) writeFileSync(tokensPath, tokens);
  }
  if (check && changed.length) {
    console.error('theme tokens are stale or colors are hard-coded; run: node dev/theme-tokens.mjs\n  ' + changed.join('\n  '));
    process.exitCode = 1;
    return;
  }
  console.log(`${check ? 'checked' : 'converted'} ${changed.length} file(s), ${used.size} tokens${changed.length ? ': ' + changed.join(', ') : ''}`);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  run(process.argv.includes('--check'));
}
