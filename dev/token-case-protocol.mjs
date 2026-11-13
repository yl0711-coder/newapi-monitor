// Offline case verification only. Returns usage metadata, never response text.
const counters = ['input_tokens', 'output_tokens', 'cache_read_input_tokens', 'cache_creation_input_tokens'];
const ttlCounters = ['ephemeral_5m_input_tokens', 'ephemeral_1h_input_tokens'];
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
function updateUsage(previous, usage, errors) {
  const result = structuredClone(previous);
  if (!object(usage)) { errors.push('usage_not_object'); return result; }
  const copy = (source, target, keys) => {
    for (const key of keys) if (Object.hasOwn(source, key)) {
      if (Number.isSafeInteger(source[key]) && source[key] >= 0) target[key] = source[key];
      else errors.push('invalid_usage_' + key);
    }
  };
  copy(usage, result, counters);
  if (Object.hasOwn(usage, 'cache_creation')) {
    if (!object(usage.cache_creation)) errors.push('invalid_cache_creation');
    else { result.cache_creation ??= {}; copy(usage.cache_creation, result.cache_creation, ttlCounters); }
  }
  return result;
}
export function totalAnthropicInput(usage) {
  const values = ['input_tokens', 'cache_read_input_tokens', 'cache_creation_input_tokens'].map(k => usage?.[k]);
  if (!values.every(v => Number.isSafeInteger(v) && v >= 0)) return null;
  const total = values.reduce((a, b) => a + b, 0);
  return Number.isSafeInteger(total) ? total : null;
}
export function parseAnthropicJSON(raw) {
  const errors = [];
  let usage = {};
  try {
    const body = JSON.parse(raw);
    if (body.type === 'error' || body.error) errors.push('upstream_error');
    usage = updateUsage({}, body.usage, errors);
  } catch { errors.push('invalid_json'); }
  return {complete: errors.length === 0, final: errors.length ? null : usage, errors};
}
export function parseAnthropicSSE(raw) {
  const errors = [], counts = {message_start: 0, message_delta: 0, message_stop: 0, ping: 0, error: 0};
  if (Buffer.byteLength(raw) > 16 * 1024 * 1024) return {complete: false, start: null, final: null, counts, errors: ['size_limit']};
  const frames = raw.replace(/\r\n/g, '\n').split('\n\n');
  const tail = frames.pop();
  if (tail?.trim()) errors.push('unfinished_event');
  let start = null, current = {}, stopped = false;
  for (const frame of frames) {
    let event = '', data = [];
    for (const line of frame.split('\n')) {
      if (line.startsWith('event:')) event = line.slice(6).trim();
      if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''));
    }
    if (!data.length) continue;
    let body;
    try { body = JSON.parse(data.join('\n')); }
    catch { errors.push('invalid_json'); continue; }
    if (!object(body)) { errors.push('invalid_event'); continue; }
    const kind = body.type || event;
    if (event && body.type && event !== body.type) errors.push('event_type_conflict');
    if (Object.hasOwn(counts, kind)) counts[kind]++;
    if (kind === 'error') { errors.push('upstream_error'); continue; }
    if (kind === 'message_start') {
      if (counts.message_start !== 1 || stopped) errors.push('invalid_message_start_order');
      current = updateUsage({}, body.message?.usage, errors);
      start = structuredClone(current);
    } else if (kind === 'message_delta') {
      if (!start || stopped) errors.push('invalid_message_delta_order');
      if (Object.hasOwn(body, 'usage')) current = updateUsage(current, body.usage, errors);
    } else if (kind === 'message_stop') {
      if (!start || stopped) errors.push('invalid_message_stop_order');
      stopped = true;
    }
  }
  if (counts.message_start !== 1 || counts.message_stop !== 1) errors.push('incomplete_stream');
  return {complete: errors.length === 0, start, final: errors.length ? null : current, counts, errors};
}
