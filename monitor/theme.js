// Monitor theme: light / dark / follow system. The choice is a per-browser
// preference (localStorage), not an account setting, so each computer keeps
// its own. The <head> boot snippet applies it before first paint; this file
// adds the settings panel, follows OS changes and re-colors charts.
(function () {
  'use strict';
  var KEY = 'monitor-theme';
  var PREFS = ['system', 'light', 'dark'];
  var media = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
  var palette = window.MonitorThemePalette;

  function readPref() {
    try { var v = localStorage.getItem(KEY); return PREFS.indexOf(v) >= 0 ? v : 'dark'; } catch (e) { return 'dark'; }
  }
  function writePref(v) { try { localStorage.setItem(KEY, v); } catch (e) {} }
  function resolve(pref) { return pref === 'system' ? (media && media.matches ? 'dark' : 'light') : pref; }
  function current() { return document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark'; }

  // ---- chart colors -------------------------------------------------------
  // ECharts draws on canvas and cannot read CSS variables. Chart options keep
  // their original (dark-theme) literals; in the light theme every literal is
  // mapped through the same derivation as the stylesheet tokens.
  var HEX = /^#([0-9a-fA-F]{6}|[0-9a-fA-F]{3})$/;
  var RGB = /^rgba?\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*(?:,\s*([0-9.]+)\s*)?\)$/;
  function mapColor(s) {
    if (typeof s !== 'string' || !palette) return s;
    var m = HEX.exec(s.trim());
    if (m) {
      var h = m[1].toLowerCase();
      if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
      // Chart series are fills and lines: keep them bright (greys such as
      // axis labels still follow the text mapping).
      return '#' + palette.deriveLightFill(h);
    }
    m = RGB.exec(s.trim());
    if (m) {
      // Transparent chart colors (grid lines, area fills) use the tint mapping,
      // so e.g. faint white grid lines become faint dark ones.
      var hex = palette.toHex(+m[1], +m[2], +m[3]);
      var c = palette.fromHex(m[4] === undefined ? palette.deriveLightFill(hex) : palette.deriveLightAlpha(hex));
      return m[4] === undefined ? 'rgb(' + c.join(',') + ')' : 'rgba(' + c.join(',') + ',' + m[4] + ')';
    }
    return s;
  }
  function mapDeep(v, seen) {
    if (typeof v === 'string') return mapColor(v);
    if (!v || typeof v !== 'object' || typeof v === 'function') return v;
    if (seen.indexOf(v) >= 0) return v;
    seen.push(v);
    if (Array.isArray(v)) return v.map(function (x) { return mapDeep(x, seen); });
    // Leave data payloads, DOM nodes and typed arrays untouched.
    if (v.nodeType || ArrayBuffer.isView(v)) return v;
    var out = {};
    for (var k in v) {
      if (!Object.prototype.hasOwnProperty.call(v, k)) continue;
      out[k] = (k === 'data' && Array.isArray(v[k]) && typeof v[k][0] !== 'object') ? v[k] : mapDeep(v[k], seen);
    }
    return out;
  }
  var charts = [];
  function hookECharts() {
    var ec = window.echarts;
    if (!ec || ec.__monitorThemeHooked) return;
    ec.__monitorThemeHooked = true;
    var init = ec.init;
    ec.init = function () {
      var inst = init.apply(this, arguments);
      var setOption = inst.setOption;
      inst.__monitorRaw = [];
      inst.setOption = function (opt, a, b) {
        var notMerge = a === true || (a && typeof a === 'object' && a.notMerge);
        if (notMerge) inst.__monitorRaw = [];
        inst.__monitorRaw.push([opt, a, b]);
        if (inst.__monitorRaw.length > 30) inst.__monitorRaw.splice(0, inst.__monitorRaw.length - 30);
        var applied = current() === 'light' ? mapDeep(opt, []) : opt;
        return setOption.call(inst, applied, a, b);
      };
      inst.__monitorSetOption = setOption;
      charts.push(inst);
      return inst;
    };
  }
  function recolorCharts() {
    charts = charts.filter(function (inst) { return inst && !(inst.isDisposed && inst.isDisposed()); });
    charts.forEach(function (inst) {
      var calls = inst.__monitorRaw || [];
      if (!calls.length) return;
      try {
        inst.clear();
        calls.forEach(function (c) {
          var applied = current() === 'light' ? mapDeep(c[0], []) : c[0];
          inst.__monitorSetOption.call(inst, applied, c[1], c[2]);
        });
      } catch (e) {}
    });
  }

  // ---- inline style colors ------------------------------------------------
  // Some markup is built at runtime with colors held in JS variables, e.g.
  // style="color:${col}". Stylesheets are tokenized at build time; these are
  // mapped here with the same rules (by property role) while the light theme
  // is active, and restored when switching back to dark.
  var INLINE = /#[0-9a-fA-F]{6}(?![0-9a-zA-Z])|#[0-9a-fA-F]{3}(?![0-9a-zA-Z])|rgba?\(\s*\d{1,3}\s*,\s*\d{1,3}\s*,\s*\d{1,3}\s*(?:,\s*[0-9.]+\s*)?\)/g;
  function roleOf(prop) {
    prop = prop.trim().toLowerCase();
    if (prop.indexOf('border') === 0 || prop.indexOf('outline') === 0) return 'border';
    if (prop.indexOf('background') === 0) return 'fill';
    return 'text';
  }
  function mapInline(value, role) {
    return value.replace(INLINE, function (m, off, all) {
      if (all.indexOf('/*fixed*/', off + m.length) === off + m.length) return m;
      var hex, alpha;
      var h = HEX.exec(m), r = RGB.exec(m);
      if (h) { hex = h[1].toLowerCase(); if (hex.length === 3) hex = hex[0] + hex[0] + hex[1] + hex[1] + hex[2] + hex[2]; }
      else if (r) { hex = palette.toHex(+r[1], +r[2], +r[3]); alpha = r[4]; }
      else return m;
      if (alpha !== undefined && +alpha < 1) {
        var a = palette.fromHex(palette.deriveLightAlpha(hex));
        return 'rgba(' + a.join(',') + ',' + (+alpha * palette.lightAlphaScale).toFixed(3) + ')';
      }
      var fn = role === 'border' ? palette.deriveLightBorder : role === 'fill' ? palette.deriveLightFill : palette.deriveLight;
      return '#' + fn(hex);
    });
  }
  function lightStyle(css) {
    return css.replace(/(^|;)(\s*[a-zA-Z-]+\s*):([^;]*)/g, function (m, sep, prop, value) {
      return sep + prop + ':' + mapInline(value, roleOf(prop));
    });
  }
  var DARK_STYLE = '__monitorDarkStyle', LIGHT_STYLE = '__monitorLightStyle';
  function fixInline(el) {
    var s = el.getAttribute && el.getAttribute('style');
    if (!s) return;
    if (current() === 'light') {
      if (s === el[LIGHT_STYLE]) return;
      INLINE.lastIndex = 0;
      if (!INLINE.test(s)) return;
      var l = lightStyle(s);
      if (l === s) return;
      el[DARK_STYLE] = s; el[LIGHT_STYLE] = l;
      el.setAttribute('style', l);
    } else if (el[DARK_STYLE] && s === el[LIGHT_STYLE]) {
      el.setAttribute('style', el[DARK_STYLE]);
      el[LIGHT_STYLE] = null;
    }
  }
  function fixInlineTree(root) {
    if (!root || root.nodeType !== 1) return;
    fixInline(root);
    var list = root.querySelectorAll('[style]');
    for (var i = 0; i < list.length; i++) fixInline(list[i]);
  }
  var observer = null;
  function watchInline() {
    if (!palette || observer || !window.MutationObserver || !document.body) return;
    observer = new MutationObserver(function (records) {
      for (var i = 0; i < records.length; i++) {
        var rec = records[i];
        if (rec.type === 'attributes') fixInline(rec.target);
        else for (var j = 0; j < rec.addedNodes.length; j++) fixInlineTree(rec.addedNodes[j]);
      }
    });
    observer.observe(document.body, {childList: true, subtree: true, attributes: true, attributeFilter: ['style']});
  }

  function apply(pref) {
    var theme = resolve(pref);
    var root = document.documentElement;
    var changed = root.getAttribute('data-theme') !== theme;
    root.setAttribute('data-theme', theme);
    root.setAttribute('data-theme-pref', pref);
    // Semi UI and other widgets key off body[theme-mode=dark].
    if (document.body) {
      if (theme === 'dark') document.body.setAttribute('theme-mode', 'dark');
      else document.body.removeAttribute('theme-mode');
    }
    if (changed) {
      recolorCharts();
      fixInlineTree(document.body);
      try { window.dispatchEvent(new CustomEvent('monitor-theme-change', {detail: {theme: theme, pref: pref}})); } catch (e) {}
    }
    syncPanel(pref);
  }
  if (media) {
    var onSystem = function () { if (readPref() === 'system') apply('system'); };
    if (media.addEventListener) media.addEventListener('change', onSystem); else if (media.addListener) media.addListener(onSystem);
  }
  // Another tab changed the preference.
  window.addEventListener('storage', function (e) { if (e.key === KEY) apply(readPref()); });

  // ---- settings panel -----------------------------------------------------
  var OPTIONS = [
    {pref: 'system', label: '系统'},
    {pref: 'light', label: '浅色'},
    {pref: 'dark', label: '深色'}
  ];
  function preview(kind) {
    return '<span class="mt-prev mt-prev-' + kind + '" aria-hidden="true">' +
      '<span class="mt-prev-side"><i class="mt-dot"></i><i class="mt-line"></i><i class="mt-line short"></i><i class="mt-line"></i></span>' +
      '<span class="mt-prev-main"><span class="mt-bars"><i></i><i></i><i></i><i></i></span><i class="mt-pie"></i><i class="mt-card"></i></span>' +
      '</span>';
  }
  function panelHtml() {
    return '<div class="mt-backdrop" data-mt-close></div>' +
      '<section class="mt-panel" role="dialog" aria-modal="true" aria-labelledby="mtTitle">' +
      '<header class="mt-head"><div><h2 id="mtTitle">主题设置</h2><p>调整外观以适应您的偏好。</p></div>' +
      '<button type="button" class="mt-close" data-mt-close aria-label="关闭">' +
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6 6 18"/></svg></button></header>' +
      '<div class="mt-body"><h3>主题</h3><div class="mt-options" role="radiogroup" aria-label="主题">' +
      OPTIONS.map(function (o) {
        return '<button type="button" class="mt-option" role="radio" data-mt-pref="' + o.pref + '" aria-checked="false">' +
          preview(o.pref) + '<span class="mt-check" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="m5 12 5 5 9-10"/></svg></span>' +
          '<span class="mt-label">' + o.label + '</span></button>';
      }).join('') +
      '</div><p class="mt-note">仅保存在当前浏览器，不同电脑可以各自选择。</p></div></section>';
  }
  var panel = null, lastFocus = null;
  function syncPanel(pref) {
    if (!panel) return;
    panel.querySelectorAll('[data-mt-pref]').forEach(function (b) {
      var on = b.getAttribute('data-mt-pref') === pref;
      b.classList.toggle('active', on);
      b.setAttribute('aria-checked', on ? 'true' : 'false');
    });
  }
  function openPanel() {
    if (!panel) {
      panel = document.createElement('div');
      panel.className = 'mt-root';
      panel.hidden = true;
      panel.innerHTML = panelHtml();
      document.body.appendChild(panel);
      panel.addEventListener('click', function (e) {
        var opt = e.target.closest('[data-mt-pref]');
        if (opt) { var p = opt.getAttribute('data-mt-pref'); writePref(p); apply(p); return; }
        if (e.target.closest('[data-mt-close]')) closePanel();
      });
      panel.addEventListener('keydown', function (e) { if (e.key === 'Escape') closePanel(); });
    }
    lastFocus = document.activeElement;
    syncPanel(readPref());
    panel.hidden = false;
    var active = panel.querySelector('.mt-option.active') || panel.querySelector('.mt-option');
    if (active) active.focus();
  }
  function closePanel() {
    if (!panel || panel.hidden) return;
    panel.hidden = true;
    if (lastFocus && lastFocus.focus) lastFocus.focus();
  }

  function boot() {
    hookECharts();
    apply(readPref());
    fixInlineTree(document.body);
    watchInline();
    var btn = document.getElementById('monitorThemeButton');
    if (btn) btn.addEventListener('click', openPanel);
  }
  // Hook ECharts before any page script creates a chart.
  hookECharts();
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();

  window.MonitorTheme = {get: readPref, set: function (p) { if (PREFS.indexOf(p) >= 0) { writePref(p); apply(p); } }, open: openPanel, mapColor: mapColor, lightStyle: lightStyle};
})();
