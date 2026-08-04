// range_picker.js: 在 Monitor 的零构建页面中挂载 NewAPI 使用的真实 Semi DatePicker。
// React 18.2.0、Semi UI 2.72.2 及其原始 CSS 由 Go 服务以内嵌静态资源提供。
(function () {
  'use strict';

  if (window.SemiRangePicker) return;
  if (!window.React || !window.ReactDOM || !window.SemiUI || !window.SemiUI.DatePicker) {
    throw new Error('SemiRangePicker requires React, ReactDOM and SemiUI.DatePicker');
  }

  const pad = value => String(value).padStart(2, '0');
  const dateText = date => `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
  const cloneDate = date => new Date(date.getTime());
  const cloneDay = date => new Date(date.getFullYear(), date.getMonth(), date.getDate());
  const ymd = date => `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
  const parseValue = value => {
    if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : cloneDay(value);
    if (typeof value === 'number') {
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? null : cloneDay(parsed);
    }
    if (!value) return null;
    const match = String(value).trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) return null;
    const parsed = new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
    if (parsed.getFullYear() !== Number(match[1]) || parsed.getMonth() !== Number(match[2]) - 1 || parsed.getDate() !== Number(match[3])) return null;
    return parsed;
  };

  function SemiRangePicker(input, options) {
    if (!input || !input.parentNode) throw new Error('SemiRangePicker requires a mounted input element');
    this.input = input;
    this.options = Object.assign({ maxDays: 90, presets: [], onChange: null, onClear: null, showClear: true, theme: 'light' }, options || {});
    this.value = [];
    const parsedMaxDate = this.options.maxDate ? parseValue(this.options.maxDate) : null;
    this.maxDate = parsedMaxDate ? cloneDay(parsedMaxDate) : null;
    this.pickerRef = window.React.createRef();

    this.mount = document.createElement('div');
    this.mount.className = `srp-wrap${this.options.theme === 'dark' ? ' semi-always-dark' : ''}`;
    input.parentNode.insertBefore(this.mount, input);
    input.hidden = true;
    input.tabIndex = -1;
    input.setAttribute('aria-hidden', 'true');

    this.root = window.ReactDOM.createRoot(this.mount);
    this.setDate(this.options.value || [], false);
  }

  SemiRangePicker.prototype.selectedDates = function () {
    return this.value.map(cloneDate);
  };

  SemiRangePicker.prototype.syncSourceInput = function () {
    this.input.value = this.value.length === 2 ? `${dateText(this.value[0])} ~ ${dateText(this.value[1])}` : '';
  };

  SemiRangePicker.prototype.resolvePresetKey = function (dates) {
    if (!Array.isArray(dates) || dates.length !== 2) return '';
    for (const preset of this.options.presets || []) {
      const start = parseValue(typeof preset.start === 'function' ? preset.start() : preset.start);
      const end = parseValue(typeof preset.end === 'function' ? preset.end() : preset.end);
      if (start && end && start.getTime() === dates[0].getTime() && end.getTime() === dates[1].getTime()) return preset.key || '';
    }
    return '';
  };

  SemiRangePicker.prototype.rangeError = function (dates) {
    if (!Array.isArray(dates) || dates.length !== 2) return '';
    if (this.maxDate && (cloneDay(dates[0]) > this.maxDate || cloneDay(dates[1]) > this.maxDate)) return '不能选择未来日期';
    const maxDays = Number(this.options.maxDays) || 0;
    if (maxDays > 0) {
      const days = Math.floor((cloneDay(dates[1]) - cloneDay(dates[0])) / 86400000) + 1;
      if (days > maxDays) return `时间范围最长 ${maxDays} 天`;
    }
    return '';
  };

  SemiRangePicker.prototype.handleChange = function (nextValue) {
    const dates = (Array.isArray(nextValue) ? nextValue : [nextValue]).map(parseValue).filter(Boolean);
    if (dates.length === 1) {
      // Semi 在第一次点击起始日时会先回传单个日期。保留这个受控值，
      // 让用户继续选择结束日；不能重置为空，否则范围选择会被第一步清空。
      this.value = [cloneDate(dates[0])];
      this.syncSourceInput();
      this.render();
      return;
    }
    if (dates.length === 0) {
      this.value = [];
      this.syncSourceInput();
      this.render();
      return;
    }
    const error = this.rangeError(dates);
    if (error) {
      if (window.SemiUI.Toast && typeof window.SemiUI.Toast.warning === 'function') window.SemiUI.Toast.warning(error);
      this.render();
      return;
    }
    this.value = dates.map(cloneDate);
    this.syncSourceInput();
    this.render();
    if (typeof this.options.onChange === 'function') {
      this.options.onChange(this.selectedDates(), { preset: this.resolvePresetKey(this.value) });
    }
  };

  SemiRangePicker.prototype.handleClear = function () {
    this.value = [];
    this.syncSourceInput();
    this.render();
    if (typeof this.options.onClear === 'function') this.options.onClear();
  };

  SemiRangePicker.prototype.render = function () {
    const presets = (this.options.presets || []).map(preset => ({
      key: preset.key || '',
      text: preset.text,
      start: preset.start,
      end: preset.end,
    }));
    const disabledDate = this.maxDate ? date => cloneDay(date) > this.maxDate : undefined;
    this.root.render(window.React.createElement(window.SemiUI.DatePicker, {
      ref: this.pickerRef,
      value: this.value,
      type: 'dateRange',
      format: 'yyyy-MM-dd',
      placeholder: ['开始日期', '结束日期'],
      showClear: this.options.showClear !== false,
      pure: true,
      size: 'small',
      style: { width: '100%' },
      presets,
      disabledDate,
      dropdownClassName: this.options.theme === 'dark' ? 'semi-always-dark' : '',
      onChange: nextValue => this.handleChange(nextValue),
      onClear: () => this.handleClear(),
    }));
  };

  SemiRangePicker.prototype.setDate = function (values, trigger, meta) {
    const list = Array.isArray(values) ? values : [values];
    const start = parseValue(list[0]);
    const end = parseValue(list[1]);
    this.value = start && end ? [start, end] : [];
    this.syncSourceInput();
    this.render();
    if (trigger && this.value.length === 2 && typeof this.options.onChange === 'function') {
      this.options.onChange(this.selectedDates(), meta || {});
    }
  };

  SemiRangePicker.prototype.open = function () {
    if (this.pickerRef.current && typeof this.pickerRef.current.open === 'function') this.pickerRef.current.open();
  };

  SemiRangePicker.prototype.close = function () {
    if (this.pickerRef.current && typeof this.pickerRef.current.close === 'function') this.pickerRef.current.close();
  };

  SemiRangePicker.prototype.destroy = function () {
    this.root.unmount();
    this.mount.remove();
    this.input.hidden = false;
    this.input.removeAttribute('aria-hidden');
    this.input.removeAttribute('tabindex');
  };

  window.SemiRangePicker = SemiRangePicker;
  window.SemiRangePickerDays = { cloneDay, parseDay: parseValue, ymd };
})();
