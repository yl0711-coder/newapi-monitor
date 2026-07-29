// range_picker.js: Semi DatePicker 风格的零依赖日期范围选择器。
// monitor 页面是嵌入式静态 HTML，不能为了一个控件引入 React/Semi 整套运行时；
// 这里保留其关键交互：双月、起止时间、清除、快捷范围和范围高亮。
(function () {
  if (window.SemiRangePicker) return;

  const css = `
  /* 以下数值对齐 @douyinfe/semi-ui 2.69.1 DatePicker：type=dateTimeRange + size=small。 */
  .srp-wrap{position:relative;display:inline-block;min-width:430px;font-family:"Inter",-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
  .srp-input{width:100%;height:24px;border:0;border-radius:4px;background:#f4f5f5;color:#1c1f23;padding:2px 12px;font-size:13px;line-height:20px;cursor:pointer;outline:0;font-variant-numeric:tabular-nums}
  .srp-input:hover{background:#ebedf0}.srp-input:focus,.srp-wrap.open .srp-input{background:#f4f5f5;box-shadow:inset 0 0 0 1px #0064fa}
  .srp-clear{display:none!important}
  .srp-pop{display:none;position:absolute;z-index:80;left:0;top:calc(100% + 4px);width:568px;max-width:calc(100vw - 24px);background:#fff;color:#1c1f23;border:1px solid #e7e9eb;border-radius:6px;box-shadow:0 8px 20px rgba(28,31,35,.14);overflow:hidden}
  .srp-wrap.open .srp-pop{display:block}.srp-months{display:flex;padding:0}.srp-month{width:252px;box-sizing:content-box;padding:0 16px 16px;min-width:0}
  .srp-mhead{height:32px;display:flex;align-items:center;justify-content:space-between;padding:12px 0;margin:0}.srp-title{font-size:16px;line-height:22px;font-weight:600}.srp-navs{display:flex}.srp-nav{width:32px;height:32px;border:0;background:transparent;border-radius:4px;color:#73777d;font-size:26px;line-height:25px;cursor:pointer}.srp-nav:hover{background:#f4f5f5;color:#1c1f23}
  .srp-week,.srp-grid{display:grid;grid-template-columns:repeat(7,36px);text-align:center}.srp-week{height:36px;border-bottom:1px solid #e7e9eb;color:#72777d;font-size:12px;line-height:36px;font-weight:600}.srp-grid{padding-top:0;row-gap:0}.srp-day{width:36px;height:36px;border:0;border-radius:0;background:transparent;color:#1c1f23;font-size:14px;cursor:pointer}.srp-day:hover{background:#f4f5f5}.srp-day.muted{visibility:hidden}.srp-day.disabled{color:#c9cdd4;cursor:not-allowed}.srp-day.in-range{background:#e8f3ff;border-radius:0}.srp-day.start,.srp-day.end{background:#0064fa;color:#fff;border-radius:4px}.srp-day.today:not(.start):not(.end){color:#0064fa;background:#f4f5f5;font-weight:600}
  .srp-bottom{border-top:1px solid #e7e9eb;padding:12px 16px 0}.srp-values{display:grid;grid-template-columns:1fr 20px 1fr;align-items:center;gap:4px;font-size:13px}.srp-value{height:24px;display:flex;align-items:center;gap:5px;padding:0 8px;border-radius:4px;background:#f4f5f5;font-variant-numeric:tabular-nums;font-weight:400}.srp-value .srp-icon{color:#73777d;font-size:14px}.srp-time{color:#73777d;font-weight:400}.srp-tilde{color:#73777d;font-size:14px;text-align:center}.srp-presets{display:grid;grid-template-columns:repeat(5,minmax(96.2px,99.2px));gap:8px;padding:8px 4px 8px;margin:0}.srp-preset{max-width:99.2px;border:0;border-radius:4px;background:#f4f5f5;color:#1c1f23;padding:4px 8px;font-size:13px;line-height:20px;font-weight:400;cursor:pointer}.srp-preset:hover,.srp-preset.active{background:#e8f3ff;color:#0064fa}.srp-error{min-height:0;margin:0;color:#d14343;font-size:12px}
  @media(max-width:600px){.srp-wrap{min-width:100%}.srp-pop{left:auto;right:0;width:min(284px,calc(100vw - 24px))}.srp-months{display:block}.srp-month:nth-child(2){display:none}.srp-bottom{padding:8px}.srp-presets{grid-template-columns:repeat(2,1fr)}.srp-preset{max-width:none}.srp-time{display:none}}
  `;
  const style = document.createElement('style'); style.textContent = css; document.head.appendChild(style);

  const pad = n => String(n).padStart(2, '0');
  const ymd = d => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  const hms = d => `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  const cloneDay = d => new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const cloneDate = d => new Date(d.getTime());
  const parseValue = (v, isEnd) => {
    if (v instanceof Date) return cloneDate(v);
    if (!v) return null;
    const m = String(v).trim().match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/);
    if (!m) return null;
    return new Date(+m[1], +m[2] - 1, +m[3], m[4] == null ? (isEnd ? 23 : 0) : +m[4], m[5] == null ? (isEnd ? 59 : 0) : +m[5], m[6] == null ? (isEnd ? 59 : 0) : +m[6]);
  };
  const parseDay = v => { const d = parseValue(v); return d && cloneDay(d); };
  const sameDay = (a, b) => a && b && ymd(a) === ymd(b);
  const between = (d, a, b) => a && b && d > cloneDay(a) && d < cloneDay(b);
  const chineseMonth = d => `${d.getFullYear()}年 ${d.getMonth() + 1}月`;
  const addMonths = (d, n) => new Date(d.getFullYear(), d.getMonth() + n, 1);
  const addDays = (d, n) => { const x = cloneDay(d); x.setDate(x.getDate() + n); return x; };

  function SemiRangePicker(input, options) {
    this.input = input;
    this.options = Object.assign({ maxDays: 90, presets: [], onChange: null, onClear: null, showClear: false }, options || {});
    this.maxDate = parseDay(this.options.maxDate) || cloneDay(new Date());
    this.start = null; this.end = null; this.error = '';
    this.view = new Date(this.maxDate.getFullYear(), this.maxDate.getMonth(), 1);
    this.wrap = document.createElement('div'); this.wrap.className = 'srp-wrap';
    input.parentNode.insertBefore(this.wrap, input); this.wrap.appendChild(input);
    input.classList.add('srp-input'); input.readOnly = true;
    this.clear = null;
    if (this.options.showClear) { this.clear = document.createElement('button'); this.clear.type = 'button'; this.clear.className = 'srp-clear'; this.clear.title = '清除日期范围'; this.clear.textContent = '×'; this.wrap.appendChild(this.clear); }
    this.pop = document.createElement('div'); this.pop.className = 'srp-pop'; this.wrap.appendChild(this.pop);
    input.addEventListener('click', e => { e.stopPropagation(); this.toggle(); });
    if (this.clear) this.clear.addEventListener('click', e => { e.stopPropagation(); this.clearRange(); });
    document.addEventListener('click', e => { if (!this.wrap.contains(e.target)) this.close(); });
    this.setDate(this.options.value || [], false);
  }

  SemiRangePicker.prototype.selectedDates = function () { return [this.start, this.end].filter(Boolean); };
  SemiRangePicker.prototype.setDate = function (values, trigger, meta) {
    const list = Array.isArray(values) ? values : [values];
    this.start = parseValue(list[0], false); this.end = parseValue(list[1], true);
    if (this.start && this.end && this.start > this.end) [this.start, this.end] = [this.end, this.start];
    if (this.start) this.view = new Date(this.start.getFullYear(), this.start.getMonth(), 1);
    this.error = ''; this.paint();
    if (trigger && this.start && this.end && this.options.onChange) this.options.onChange(this.selectedDates(), meta || {});
  };
  SemiRangePicker.prototype.clearRange = function () { this.start = this.end = null; this.error = ''; this.paint(); if (this.options.onClear) this.options.onClear(); };
  SemiRangePicker.prototype.open = function () { this.wrap.classList.add('open'); this.render(); };
  SemiRangePicker.prototype.close = function () { this.wrap.classList.remove('open'); };
  SemiRangePicker.prototype.toggle = function () { this.wrap.classList.contains('open') ? this.close() : this.open(); };
  SemiRangePicker.prototype.paint = function () {
    this.wrap.classList.toggle('has-value', !!(this.start || this.end));
    this.input.value = this.start && this.end ? `${ymd(this.start)} ${hms(this.start)}  ~  ${ymd(this.end)} ${hms(this.end)}` : (this.start ? `${ymd(this.start)} ${hms(this.start)}  ~` : '');
    if (this.wrap.classList.contains('open')) this.render();
  };
  SemiRangePicker.prototype.choose = function (day) {
    if (day > this.maxDate) return;
    if (!this.start || this.end) { this.start = cloneDay(day); this.end = null; this.error = ''; this.render(); return; }
    let a = cloneDay(this.start), b = cloneDay(day); if (b < a) [a, b] = [b, a];
    if (Math.floor((b - a) / 86400000) + 1 > this.options.maxDays) { this.error = `时间范围最长 ${this.options.maxDays} 天`; this.render(); return; }
    this.start = a; this.end = b; this.end.setHours(23, 59, 59, 0); this.error = ''; this.paint(); this.close();
    if (this.options.onChange) this.options.onChange(this.selectedDates(), {});
  };
  SemiRangePicker.prototype.monthHTML = function (month, side) {
    const first = new Date(month.getFullYear(), month.getMonth(), 1), days = new Date(month.getFullYear(), month.getMonth() + 1, 0).getDate();
    let cells = ''; for (let i = 0; i < first.getDay(); i++) cells += '<span class="srp-day muted"></span>';
    for (let n = 1; n <= days; n++) {
      const d = new Date(month.getFullYear(), month.getMonth(), n), disabled = d > this.maxDate;
      const cls = ['srp-day']; if (disabled) cls.push('disabled'); if (sameDay(d, this.start)) cls.push('start'); else if (sameDay(d, this.end)) cls.push('end'); else if (between(d, this.start, this.end)) cls.push('in-range'); if (sameDay(d, cloneDay(new Date()))) cls.push('today');
      cells += `<button type="button" class="${cls.join(' ')}" data-day="${ymd(d)}"${disabled ? ' disabled' : ''}>${n}</button>`;
    }
    const left = side === 'left' ? '<div class="srp-navs"><button type="button" class="srp-nav" data-nav="-12">«</button><button type="button" class="srp-nav" data-nav="-1">‹</button></div>' : '<span></span>';
    const right = side === 'right' ? '<div class="srp-navs"><button type="button" class="srp-nav" data-nav="1">›</button><button type="button" class="srp-nav" data-nav="12">»</button></div>' : '<span></span>';
    return `<div class="srp-month"><div class="srp-mhead">${left}<div class="srp-title">${chineseMonth(month)}</div>${right}</div><div class="srp-week"><span>日</span><span>一</span><span>二</span><span>三</span><span>四</span><span>五</span><span>六</span></div><div class="srp-grid">${cells}</div></div>`;
  };
  SemiRangePicker.prototype.render = function () {
    const second = addMonths(this.view, 1), active = this.options.presets.find(p => this.start && this.end && sameDay(this.start, parseDay(p.start())) && sameDay(this.end, parseDay(p.end())));
    this.pop.innerHTML = `<div class="srp-months">${this.monthHTML(this.view, 'left')}${this.monthHTML(second, 'right')}</div><div class="srp-bottom"><div class="srp-values"><div class="srp-value"><span class="srp-icon">▣</span><span>${this.start ? ymd(this.start) : '开始日期'}</span><span class="srp-time">${this.start ? hms(this.start) : '00:00:00'}</span></div><span class="srp-tilde">~</span><div class="srp-value"><span class="srp-icon">▣</span><span>${this.end ? ymd(this.end) : '结束日期'}</span><span class="srp-time">${this.end ? hms(this.end) : '23:59:59'}</span></div></div><div class="srp-presets">${this.options.presets.map((p, i) => `<button type="button" class="srp-preset${active === p ? ' active' : ''}" data-preset="${i}">${p.text}</button>`).join('')}</div><div class="srp-error">${this.error || ''}</div></div>`;
    this.pop.querySelectorAll('[data-nav]').forEach(b => b.onclick = () => { this.view = addMonths(this.view, +b.dataset.nav); this.render(); });
    this.pop.querySelectorAll('[data-day]').forEach(b => b.onclick = () => this.choose(parseDay(b.dataset.day)));
    this.pop.querySelectorAll('[data-preset]').forEach(b => b.onclick = () => { const p = this.options.presets[+b.dataset.preset]; this.setDate([p.start(), p.end()], true, { preset: p.key || '' }); this.close(); });
  };
  window.SemiRangePicker = SemiRangePicker;
  window.SemiRangePickerDays = { addDays, cloneDay, parseDay, ymd };
})();
