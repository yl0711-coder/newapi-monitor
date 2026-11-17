// Light-theme color derivation shared by dev/theme-tokens.mjs (stylesheet
// tokens) and theme.js (chart colors at runtime), so both always agree.
// Input and output are 6-digit lowercase hex without '#'.
//
// The light theme follows the new-api console (shadcn style) as deployed:
// white surfaces, neutral hairline borders (#e8e8e8), near-black text,
// neutral grey secondary text (#606060) and a sky-blue accent (#3ea4ec).
// Greys carry no hue; the indigo accent family is re-hued to sky blue.
(function (root) {
  // Pinned light values for the structural palette. Everything else is
  // derived by deriveLight().
  var OVERRIDES = {
    '0f1117': 'f5f5f5', // page background: off-white so white cards stand out (Primer canvas-subtle idea)
    '11141d': 'fcfcfc', // sidebar
    '1a1d27': 'ffffff', // cards / surfaces
    '202433': 'f5f5f5', // raised controls (new-api muted)
    '2a2d3e': 'e8e8e8', // borders (new-api border)
    '363b52': 'dedede',
    '3a3f56': 'd4d4d4',
    'e2e8f0': '1f1f1f', // primary text (~16:1, like Primer fg #1f2328)
    'f8fafc': '141414',
    'cbd5e1': '3d3d3d',
    '94a3b8': '595959', // muted text (~7:1, Primer muted is ~6:1)
    '64748b': '6b6b6b', // placeholder / dim text, still >= 5:1
    'ffffff': 'ffffff', // text on colored fills stays white
    '000000': '000000'
  };


  function fromHex(h) { return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)]; }
  function toHex(r, g, b) {
    return [r, g, b].map(function (v) { v = Math.max(0, Math.min(255, Math.round(v))); return (v < 16 ? '0' : '') + v.toString(16); }).join('');
  }
  function rgbToHsl(c) {
    var r = c[0] / 255, g = c[1] / 255, b = c[2] / 255;
    var max = Math.max(r, g, b), min = Math.min(r, g, b), l = (max + min) / 2;
    if (max === min) return [0, 0, l];
    var d = max - min, s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
    var h = max === r ? (g - b) / d + (g < b ? 6 : 0) : max === g ? (b - r) / d + 2 : (r - g) / d + 4;
    return [h / 6, s, l];
  }
  function hslToRgb(h, s, l) {
    if (s === 0) return [l * 255, l * 255, l * 255];
    var q = l < 0.5 ? l * (1 + s) : l + s - l * s, p = 2 * l - q;
    function f(t) { t = (t + 1) % 1; return t < 1 / 6 ? p + (q - p) * 6 * t : t < 1 / 2 ? q : t < 2 / 3 ? p + (q - p) * (2 / 3 - t) * 6 : p; }
    return [f(h + 1 / 3) * 255, f(h) * 255, f(h - 1 / 3) * 255];
  }
  // Chroma (max - min channel, 0..1) separates greys from colors more
  // reliably than HSL saturation, which explodes for very light or very dark
  // slate tones (e.g. #e8edf7 has 50% saturation but is plainly a grey).
  function chroma(c) { return (Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2])) / 255; }
  function isNeutral(c, l) { var k = chroma(c); return k < (l > 0.75 ? 0.1 : 0.16); }

  // The dark UI's indigo/violet-blue accent family becomes new-api's sky
  // blue; other hues keep their identity.
  var ACCENT_HUE = 205 / 360;
  function hue(h) { return h > 0.62 && h < 0.74 ? ACCENT_HUE : h; }
  function satCap(h) { return Math.abs(h - ACCENT_HUE) < 0.01 ? 0.8 : 0.72; }

  var DARK_BG = [15, 17, 23];
  function relLum(c) {
    var lin = c.map(function (v) { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); });
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2];
  }
  function contrast(a, b) { var x = relLum(a), y = relLum(b); return (Math.max(x, y) + 0.05) / (Math.min(x, y) + 0.05); }
  // Neutral grey with the given contrast ratio against white.
  function greyForContrast(ratio) {
    var y = 1.05 / ratio - 0.05, v = y <= 0.03928 / 12.92 ? y * 12.92 : 1.055 * Math.pow(y, 1 / 2.4) - 0.055;
    return toHex(v * 255, v * 255, v * 255);
  }

  // Neutral grey of lightness l (new-api greys carry no hue).
  function grey(l) { return hslToRgb(0, 0, l); }

  // Map a color designed for a dark background to its light-background role.
  function deriveLight(hex) {
    if (OVERRIDES[hex]) return OVERRIDES[hex];
    var rgb = fromHex(hex), hsl = rgbToHsl(rgb), h = hue(hsl[0]), s = hsl[1], l = hsl[2], c;
    if (l <= 0.04) return hex; // near-black: shadows and scrims stay dark
    if (isNeutral(rgb, l)) {
      // Dark surfaces become white (recessed panels a touch greyer), borders
      // become hairlines, light text becomes near-black.
      // Surfaces and borders are mapped by lightness; anything lighter
      // (text, icons) keeps at least its dark-theme contrast, with a floor
      // so secondary text stays crisp on white: >= 5:1 for dim text,
      // >= 4.6:1 even for the dimmest greys.
      var nl;
      if (l < 0.09) nl = 0.965;
      else if (l < 0.17) nl = Math.min(1, 0.965 + (l - 0.09) * 1.2);
      else if (l < 0.34) nl = 0.915 - (l - 0.17) * 0.3;
      else {
        var cd = contrast(rgb, DARK_BG);
        var target = l >= 0.78 ? 15 : Math.max(l >= 0.5 ? 5.2 : 4.6, Math.min(cd, 12));
        return greyForContrast(target);
      }
      c = grey(Math.max(0.11, nl));
      return toHex(c[0], c[1], c[2]);
    }
    var ns = Math.min(s, satCap(h)), nl2;
    if (l < 0.26) {
      // Deep tinted panels (e.g. a dark red banner) become soft tints.
      c = hslToRgb(h, Math.min(s, 0.6), 0.955);
      return toHex(c[0], c[1], c[2]);
    }
    if (l > 0.66) nl2 = 0.42;          // pastel text meant to glow on dark
    else if (l > 0.47) nl2 = Math.min(l - 0.08, 0.48);
    else nl2 = l;
    c = hslToRgb(h, ns, nl2);
    // Colored text and status marks must stay crisp on white (>= 5:1, small
    // text is common here); yellow and green are the usual offenders.
    while (nl2 > 0.18 && contrastWithWhite(c) < 5) {
      nl2 -= 0.02;
      c = hslToRgb(h, ns, nl2);
    }
    return toHex(c[0], c[1], c[2]);
  }

  // Light value for a color used with transparency (tints, overlays,
  // shadows). Tints keep a mid-lightness hue so they read clean rather than
  // muddy; white overlays (hover highlights on dark) become dark overlays.
  function deriveLightAlpha(hex) {
    var rgb = fromHex(hex), hsl = rgbToHsl(rgb), l = hsl[2];
    if (l <= 0.04) return hex;
    if (isNeutral(rgb, l)) return l > 0.6 ? '1c1f23' : deriveLight(hex);
    var h = hue(hsl[0]), c = hslToRgb(h, Math.min(hsl[1], satCap(h) + 0.1), 0.52);
    return toHex(c[0], c[1], c[2]);
  }

  // Light value for a solid border or outline: greys follow deriveLight,
  // colored borders become a light tint line of the same hue.
  function deriveLightBorder(hex) {
    var rgb = fromHex(hex), hsl = rgbToHsl(rgb), l = hsl[2];
    if (l <= 0.04 || isNeutral(rgb, l)) return deriveLight(hex);
    var h = hue(hsl[0]), c = hslToRgb(h, Math.min(hsl[1], satCap(h) + 0.1), 0.74);
    return toHex(c[0], c[1], c[2]);
  }

  // Light value for a solid background fill: greys and deep tinted panels
  // follow deriveLight; colored fills (primary buttons, active segments,
  // status dots, chart series) keep a bright mid tone like new-api's
  // primary #3ea4ec, carrying white text.
  function deriveLightFill(hex) {
    var rgb = fromHex(hex), hsl = rgbToHsl(rgb), l = hsl[2];
    if (l <= 0.04 || l < 0.26 || isNeutral(rgb, l)) return deriveLight(hex);
    var h = hue(hsl[0]);
    // White text sits on these fills (buttons are small text), so keep at
    // least 4.5:1 against white.
    var fs = Math.min(hsl[1], h === ACCENT_HUE ? 0.8 : 0.78), fl = h === ACCENT_HUE ? 0.55 : Math.max(0.42, Math.min(l, 0.55));
    var c = hslToRgb(h, fs, fl);
    while (fl > 0.25 && contrastWithWhite(c) < 4.5) { fl -= 0.02; c = hslToRgb(h, fs, fl); }
    return toHex(c[0], c[1], c[2]);
  }

  function contrastWithWhite(c) {
    var lin = c.map(function (v) { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); });
    var lum = 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2];
    return 1.05 / (lum + 0.05);
  }

  // Transparent colors sit on white in the light theme and read much
  // stronger than on the dark background, so their alpha is scaled down.
  var LIGHT_ALPHA_SCALE = 0.65;

  root.MonitorThemePalette = {overrides: OVERRIDES, deriveLight: deriveLight, deriveLightAlpha: deriveLightAlpha, deriveLightBorder: deriveLightBorder, deriveLightFill: deriveLightFill,
    lightAlphaScale: LIGHT_ALPHA_SCALE, fromHex: fromHex, toHex: toHex};
})(typeof window !== 'undefined' ? window : globalThis);
