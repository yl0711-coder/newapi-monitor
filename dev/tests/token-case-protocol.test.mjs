import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {parseAnthropicSSE, parseAnthropicJSON, totalAnthropicInput} from '../token-case-protocol.mjs';
const event = body => 'event: ' + body.type + '\ndata: ' + JSON.stringify(body) + '\n\n';
const start = event({type:'message_start', message:{usage:{input_tokens:11694, cache_read_input_tokens:0, cache_creation_input_tokens:0, cache_creation:{ephemeral_5m_input_tokens:0, ephemeral_1h_input_tokens:0}}}});
const delta = event({type:'message_delta', usage:{input_tokens:171512, output_tokens:42, cache_creation:{ephemeral_5m_input_tokens:0}}});
const stop = event({type:'message_stop'});
test('cumulative usage replaces fields and preserves omitted fields and TTL children', () => {
  const r = parseAnthropicSSE(start + event({type:'message_delta',usage:{output_tokens:20}}) + delta + stop);
  assert.equal(r.complete, true);
  assert.equal(r.start.input_tokens, 11694);
  assert.equal(totalAnthropicInput(r.final), 171512);
  assert.equal(r.final.output_tokens, 42);
  assert.equal(r.final.cache_creation.ephemeral_1h_input_tokens, 0);
  assert.equal(r.counts.message_delta, 2);
});
test('SSE supports CRLF, multiline data, comments, ping and unknown extension fields', () => {
  const multiline = 'event: message_delta\ndata: {"type":"message_delta",\ndata: "usage":{"input_tokens":22703,"kiro_actual_input_tokens":1000000},"thinking":"private"}\n\n';
  const r = parseAnthropicSSE((start + ': keepalive\n\n' + event({type:'ping'}) + multiline + stop).replaceAll('\n','\r\n'));
  assert.equal(r.complete, true);
  assert.equal(totalAnthropicInput(r.final), 22703);
  assert.equal(r.counts.ping, 1);
  assert.doesNotMatch(JSON.stringify(r), /private|kiro|thinking/);
});
test('missing cache fields stay unknown instead of becoming zero', () => {
  const r = parseAnthropicJSON('{"usage":{"input_tokens":123}}');
  assert.equal(totalAnthropicInput(r.final), null);
  assert.equal(Object.hasOwn(r.final,'cache_read_input_tokens'), false);
});
for (const [label, raw] of [
  ['incomplete stream', start+delta],
  ['unfinished trailing event', start+delta+stop+'data: {'],
  ['invalid JSON', start+'data: nope\n\n'+stop],
  ['upstream error', start+event({type:'error',error:{message:'private'}})+stop],
  ['invalid counter', start+event({type:'message_delta',usage:{input_tokens:null}})+stop],
  ['usage after stop', start+stop+delta],
]) test(label+' cannot produce valid final usage', () => {
  const r = parseAnthropicSSE(raw);
  assert.equal(r.complete,false); assert.equal(r.final,null); assert.ok(r.errors.length);
  assert.doesNotMatch(JSON.stringify(r),/private/);
});
test('private original JSON and SSE reproduce archived usage without response text', {skip:!process.env.TOKEN_CASE_ARCHIVE_ROOT}, () => {
  const root = path.join(process.env.TOKEN_CASE_ARCHIVE_ROOT,'模拟测试');
  const a = parseAnthropicJSON(fs.readFileSync(path.join(root,'4sapi-test-A-response-2026-10-10.json'),'utf8'));
  const b = parseAnthropicSSE(fs.readFileSync(path.join(root,'4sapi-test-B-response-2026-10-10.sse.txt'),'utf8'));
  const a6 = parseAnthropicSSE(fs.readFileSync(path.join(root,'Claude-Fable-5-Token复测-163-2-响应-2026-10-12.sse.txt'),'utf8'));
  assert.equal(a.complete,true); assert.equal(totalAnthropicInput(a.final),168707);
  assert.equal(b.complete,true); assert.equal(b.start.input_tokens,11694); assert.equal(totalAnthropicInput(b.final),171512);
  assert.deepEqual([b.counts.message_start,b.counts.message_delta,b.counts.message_stop],[1,1,1]);
  assert.equal(a6.complete,true); assert.equal(totalAnthropicInput(a6.final),22703);
});
