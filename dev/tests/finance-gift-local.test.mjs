import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {webcrypto} from 'node:crypto';

const id='a'.repeat(64), digest='b'.repeat(64);
const flush=async()=>{for(let i=0;i<10;i++)await new Promise(resolve=>setImmediate(resolve));};
function fixture({progressFailure=false,progressCode='',authStatus='awaiting_execution',serverCooldown=0,previewStatus=200}={}){
  const elements=new Map(), calls=[],timers=new Map(),listeners=new Map();let timerID=0;
  let now=0;
  const el=key=>{if(!elements.has(key))elements.set(key,{value:'',textContent:'',disabled:false});return elements.get(key);};
  let control={instance:'process-1',revision:0,status:'idle',task_id:'',request_id:''};
  const context=vm.createContext({AbortController,crypto:webcrypto,Uint8Array,Date:{now:()=>now},confirm:()=>true,
    document:{hidden:false,getElementById:el,addEventListener:(name,fn)=>listeners.set(name,fn)},
    window:{addEventListener:(name,fn)=>listeners.set(name,fn)},
    sessionStorage:{getItem:()=>'',setItem(){}},
    setTimeout:(fn,delay)=>{const key=++timerID;timers.set(key,{fn,delay});return key;},clearTimeout:key=>timers.delete(key),
    fetch:async(path,options)=>{
      const body=options.body?JSON.parse(options.body):null;calls.push({path,body,options});
      let result={},status=200;const headers=new Map();
      if(path.endsWith('/local/control'))result={control:{...control},confirmation:digest,execution_enabled:true,preview_retry_after_seconds:serverCooldown};
      else if(path.endsWith('/preview')){status=previewStatus;headers.set(status===429?'Retry-After':'X-Finance-Gift-Cooldown-Seconds','10');result={preview:{blocked_targets:0,ready_targets:3,entries:[]},error:'核验冷却中'};}
      else if(path.endsWith('/authorizations')){headers.set('X-Finance-Gift-Cooldown-Seconds','10');result={task_id:id};}
      else if(path.endsWith('/start')){control={...control,task_id:id,status:'running',request_id:body.request_id,revision:control.revision+1};result={control};}
      else if(path.endsWith('/stop')){control={...control,status:'stopping',revision:control.revision+1};result={control};}
      else if(path.endsWith('/revoke'))authStatus='revoked';
      else if(path.endsWith('/progress')){
        if(progressFailure){status=503;result={error:'提交记录暂不可核验',error_code:progressCode};}
        else result={progress:{committed_targets:1,authorized_targets:2,rows_updated:3,status:'partial_commits_recorded'}};
      }else if(path.endsWith('/'+id))result={status:authStatus,authorization:{targets:[{user_id:116,hour_ts:123}]}};
      else throw Error('unexpected route '+path);
      return {ok:status<400,status,headers,json:async()=>result};
    },
  });
  const html=readFileSync(new URL('../../monitor/finance_gift_local.html',import.meta.url),'utf8');
  vm.runInContext(html.match(/<script>([\s\S]*?)<\/script>/)[1],context);
  return {el,calls,timers,listeners,context,advance:async(ms)=>{now+=ms;const due=[...timers.entries()].filter(([,t])=>t.delay===1000);for(const [key,t] of due){timers.delete(key);t.fn();}await flush();}};
}

test('page load is read-only; preview and approval never start execution',async()=>{
  const f=fixture();await flush();
  assert.deepEqual(f.calls.map(c=>c.path),['/finance/gift-handoff/local/control']);
  assert.equal(f.el('start').disabled,true);
  await f.el('preview').onclick();assert.equal(f.el('authorize').disabled,true);
  assert.match(f.el('cooldown').textContent,/10 秒/);
  await f.el('authorize').onclick();assert.equal(f.calls.some(c=>c.path.endsWith('/authorizations')),false);
  await f.advance(9000);assert.match(f.el('cooldown').textContent,/1 秒/);
  await f.advance(1000);assert.equal(f.el('authorize').disabled,false);
  await f.el('authorize').onclick();
  assert.equal(f.el('task').value,id);
  assert.equal(f.el('start').disabled,false);
  assert.equal(f.calls.some(c=>c.path.endsWith('/start')),false);
  assert.equal(f.calls.find(c=>c.path.endsWith('/authorizations')).body.max_hours,2);
  assert.match(f.el('progress').textContent,/1\/2.*3 条/);
});

test('server cooldown survives page load; expiration never automatically authorizes',async()=>{
  const f=fixture({serverCooldown:4});await flush();
  assert.equal(f.el('preview').disabled,true);assert.match(f.el('cooldown').textContent,/4 秒/);
  await f.el('preview').onclick();assert.equal(f.calls.length,1);
  await f.advance(4000);assert.equal(f.el('preview').disabled,false);
  assert.equal(f.calls.length,1);assert.equal(f.el('authorize').disabled,true);
});

test('rate limited and failed evidence requests retain server cooldown without automatic retry',async()=>{
  for(const previewStatus of [429,503]){
    const f=fixture({previewStatus});await flush();await f.el('preview').onclick();
    assert.equal(f.el('preview').disabled,true);assert.match(f.el('cooldown').textContent,/10 秒/);
    assert.equal(f.el('authorize').disabled,true);
    await f.advance(10000);assert.equal(f.el('preview').disabled,false);
    assert.equal(f.calls.filter(c=>c.path.endsWith('/preview')).length,1);
  }
});

test('missing ledger is explained as unknown, not zero or proof of never executed',async()=>{
  const f=fixture({progressFailure:true,progressCode:'ledger_missing'});await flush();
  f.el('task').value=id;await f.el('refresh').onclick();
  assert.match(f.el('progress').textContent,/提交台账尚未建立/);
  assert.match(f.el('progress').textContent,/进度未知（不是 0）/);
  assert.match(f.el('progress').textContent,/若此前已执行/);
  const other=fixture({progressFailure:true,progressCode:'progress_unavailable'});await flush();
  other.el('task').value=id;await other.el('refresh').onclick();
  assert.match(other.el('progress').textContent,/读取失败/);
  assert.doesNotMatch(other.el('progress').textContent,/首次本机执行/);
});

test('explicit start is single-flight; stop identifies the same attempt; no auto-resume',async()=>{
  const f=fixture();await flush();f.el('task').value=id;await f.el('refresh').onclick();
  const first=f.el('start').onclick();
  assert.equal(f.el('task').disabled,true);
  const second=f.el('start').onclick();await Promise.all([first,second]);
  const starts=f.calls.filter(c=>c.path.endsWith('/start'));
  assert.equal(starts.length,1);assert.equal(starts[0].body.instance,'process-1');
  assert.equal(f.el('start').disabled,true);assert.equal(f.el('stop').disabled,false);
  assert.equal([...f.timers.values()].filter(t=>t.delay===5000).length,1);
  await f.el('stop').onclick();
  assert.equal(f.calls.find(c=>c.path.endsWith('/stop')).body.request_id,starts[0].body.request_id);
  assert.match(f.el('runtime').textContent,/正在停止/);
  f.listeners.get('pagehide')();assert.equal(f.timers.size,0);
  assert.equal(f.calls.filter(c=>c.path.endsWith('/start')).length,1);
});

test('progress failure is not zero, revoked authorization prevents start, input edit invalidates approval',async()=>{
  const f=fixture({progressFailure:true,authStatus:'revoked'});await flush();f.el('task').value=id;await f.el('refresh').onclick();
  assert.match(f.el('progress').textContent,/暂不可核验.*不是 0/);
  assert.equal(f.el('start').disabled,true);assert.equal(f.el('revoke').disabled,true);
  const other=fixture();await flush();other.el('task').value=id;await other.el('refresh').onclick();
  assert.equal(other.el('start').disabled,false);
  other.el('task').value='c'.repeat(64);other.el('task').oninput();assert.equal(other.el('start').disabled,true);
});
