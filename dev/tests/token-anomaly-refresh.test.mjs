import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {access, at, detectionHealth, health, notifyHealth, report, item} from '../token-anomaly-fixture.mjs';

const source = readFileSync(new URL('../../monitor/token_anomaly.js', import.meta.url), 'utf8');

class Element {
  constructor(id) {
    this.id = id;
    this.value = '';
    this.hidden = false;
    this.disabled = false;
    this.checked = false;
    this.open = false;
    this.innerHTML = '';
    this.textContent = '';
    this.attrs = {};
    this.listeners = new Map();
  }
  addEventListener(name, fn) {this.listeners.set(name, fn)}
  setAttribute(name, value) {this.attrs[name] = value}
  getAttribute(name) {return this.attrs[name] ?? null}
  focus() {this.focused = true}
  showModal() {this.open = true}
  close() {this.open = false}
}

function fixture() {
	let clock=Date.now();
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, new Element(id));
    return elements.get(id);
  };
  const events = {window: new Map(), document: new Map()};
  const requests = [];
  const timers = new Map();
  let timerID = 0;
  const addEvent = (scope, name, fn) => events[scope].set(name, fn);
  const document = {
    hidden: false,
    activeElement: null,
    getElementById: element,
    querySelector: selector => element('query:' + selector),
    querySelectorAll: () => [],
    addEventListener: (name, fn) => addEvent('document', name, fn),
  };
  const window = {
    scrollX: 0,
    scrollY: 0,
    addEventListener: (name, fn) => addEvent('window', name, fn),
    scrollTo(x,y) {window.scrollX=x;window.scrollY=y},
    monitorNavigationContext: () => ({}),
  };
  const context = vm.createContext({
	Date: class extends Date { static now(){return clock} },
    window,
    document,
    URL,
    URLSearchParams,
    AbortController,
    crypto: {randomUUID: () => 'refresh-test'},
    setTimeout(fn, delay) {
      const id = ++timerID;
      timers.set(id, {fn, delay});
      return id;
    },
    clearTimeout: id => timers.delete(id),
    fetch: (url, options) => new Promise((resolve, reject) => requests.push({url, options, resolve, reject})),
  });
  const end = source.lastIndexOf('})();');
  vm.runInContext(
    source.slice(0, end) + '\nglobalThis.testing={state,load,tokenAnomalyActivate:window.tokenAnomalyActivate,tokenAnomalyDeactivate:window.tokenAnomalyDeactivate};\n' + source.slice(end),
    context,
  );
  const response = (value, status = 200) => ({ok: status === 200, status, json: async () => value});
  const settle = (offset, value = report()) => {
    const values = [value, health, detectionHealth, notifyHealth, access];
    for (const [i, body] of values.entries()) requests[offset + i].resolve(response(body));
  };
  return {
	advance: seconds=>{clock+=seconds*1000},
    ...context.testing,
    document,
    window,
    element,
    requests,
    timers,
    events,
    settle,
    async tick(delay) {
      const pending = [...timers.entries()].filter(([, timer]) => timer.delay === delay);
      for (const [id, timer] of pending) {
        timers.delete(id);
        timer.fn();
      }
      await new Promise(setImmediate);
    },
    async emit(scope, name) {
      events[scope].get(name)?.();
      await new Promise(setImmediate);
    },
  };
}

test('following latest moves the range while pagination pins it and fixed history stays fixed', async () => {
	const h=fixture();h.tokenAnomalyActivate();h.settle(0);await new Promise(setImmediate);
	const first=JSON.parse(h.requests[0].options.body);
	assert.equal(first.to-first.from,900);assert.equal(first.newest_first,true);
	h.state.query.model='kept-model';h.state.query.model_set=true;
	h.window.scrollY=420;
	h.advance(65);await h.tick(30000);
	const second=JSON.parse(h.requests[5].options.body);
	assert.equal(second.to-first.to,65);assert.equal(second.from-first.from,65);
	assert.equal(second.model,'kept-model');h.settle(5);await new Promise(setImmediate);
	assert.equal(h.window.scrollY,420);
	h.advance(45);const paging=h.load(true);h.settle(10);await paging;
	assert.equal(JSON.parse(h.requests[10].options.body).to,second.to,'pagination changed its range');
	h.state.query.follow_seconds=0;h.element('taTimeMode').value='0';
	const fixed={from:h.state.query.from,to:h.state.query.to};h.advance(120);
	const refresh=h.load(false,true);h.settle(15);await refresh;
	const last=JSON.parse(h.requests[15].options.body);
	assert.equal(last.from,fixed.from);assert.equal(last.to,fixed.to);assert.equal(last.newest_first,false);
});

test('one-hour following mode and a newer request fence a late prior response', async () => {
	const h=fixture();h.tokenAnomalyActivate();h.settle(0);await new Promise(setImmediate);
	h.state.query.follow_seconds=3600;h.element('taTimeMode').value='3600';h.advance(31);
	const old=h.load(false,true);h.advance(31);const newer=h.load(false,true);
	h.settle(10);await newer;
	const selected={...h.state.query};h.settle(5);await old;
	assert.equal(h.state.query.to,selected.to);assert.equal(selected.to-selected.from,3600);
	assert.equal(h.requests[5].options.signal.aborted,true);
});

test('Token anomaly auto refresh is local, visible-only, and state-preserving', async () => {
  const h = fixture();
  h.tokenAnomalyActivate();
  h.settle(0);
  await new Promise(setImmediate);
  assert.equal(h.requests.length, 5);
  assert.ok([...h.timers.values()].some(timer => timer.delay === 30000));

  h.state.query.model = 'fixture-model';
  h.state.query.model_set = true;
  h.element('taModel').value = 'fixture-model';
  await h.tick(30000);
  assert.equal(h.requests.length, 10);
  assert.ok(h.requests.slice(5).every(request => request.url.startsWith('/anomalies/token/')));
  h.settle(5);
  await new Promise(setImmediate);
  assert.equal(h.state.query.model, 'fixture-model');
  assert.equal(h.element('taModel').value, 'fixture-model');

  h.document.hidden = true;
  await h.emit('document', 'visibilitychange');
  await h.tick(30000);
  assert.equal(h.requests.length, 10, 'hidden page kept its refresh timer');

  h.document.hidden = false;
  await h.emit('document', 'visibilitychange');
  assert.equal(h.requests.length, 15, 'visible page did not resume its local refresh');
  h.settle(10);
  await new Promise(setImmediate);
  h.tokenAnomalyDeactivate();
  await h.tick(30000);
  assert.equal(h.requests.length, 15, 'deactivated page kept refreshing');
});

test('slow auto refresh keeps the latest focus, scroll and user-closed detail', async () => {
  const h=fixture();h.tokenAnomalyActivate();const initial=report();initial.has_more=false;h.settle(0,initial);await new Promise(setImmediate);
  const range=JSON.stringify(h.state.query);h.element('taDetail').open=true;
  const p=h.load(false,true);
  // Interaction after the request began wins over the old view snapshot.
  h.window.scrollY=480;h.document.activeElement=h.element('taModel');h.element('taDetail').close();
  h.settle(5,initial);await p;
  assert.equal(h.window.scrollY,480);assert.equal(h.element('taModel').focused,true);
  assert.equal(h.element('taDetail').open,false);assert.equal(JSON.stringify(h.state.query),range);
});

test('auto refresh restores a list control by request identity after reordering', async () => {
  const h=fixture();h.tokenAnomalyActivate();const initial=report();initial.has_more=false;h.settle(0,initial);await new Promise(setImmediate);
  const focused=new Element('');focused.attrs['data-ta-detail']='0';h.document.activeElement=focused;
  const p=h.load(false,true),next=report();next.items=[item(9),initial.items[0]];next.has_more=false;
  h.settle(5,next);await p;
  assert.equal(h.element('query:[data-ta-detail="1"]').focused,true);
  assert.equal(h.element('query:[data-ta-detail="0"]').focused,undefined);
});

test('late failed refresh page cannot replace a newer report or failure state', async () => {
  const h=fixture();h.tokenAnomalyActivate();h.settle(0);await new Promise(setImmediate);h.state.loadedPages=2;
  const stale=h.load(false,true);h.settle(5);await new Promise(setImmediate);
  assert.equal(h.requests.length,11);
  const fresh=h.load(false,true),next=report();next.has_more=false;next.items[0].display.customer_name='latest';h.settle(11,next);await fresh;
  assert.equal(h.requests[10].options.signal.aborted,true);h.requests[10].reject(Error('late failed page'));await stale;
  assert.equal(h.state.reportReadFailed,false);assert.match(h.element('taList').innerHTML,/latest/);assert.equal(h.element('taError').hidden,true);
});

test('unavailable local refresh preserves the visible snapshot and shows unknown coverage', async () => {
  const h=fixture();h.tokenAnomalyActivate();const initial=report();initial.has_more=false;h.settle(0,initial);await new Promise(setImmediate);
  const html=h.element('taList').innerHTML,p=h.load(false,true);
  h.settle(5,{...report(),available:false,items:[]});await p;
  assert.equal(h.element('taList').innerHTML,html);assert.match(h.element('taCoverage').textContent,/覆盖未知/);assert.equal(h.element('taError').hidden,false);
});
