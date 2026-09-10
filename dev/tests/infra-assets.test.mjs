import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

const source=readFileSync(new URL('../../monitor/infra_assets.js',import.meta.url),'utf8');
function ui(fetch=()=>{throw Error('unexpected network')}, timers={setTimeout,clearTimeout}) {
  let html='', pageButtons=[], actionButtons=[];
  const alerts=[];
  const root={textContent:'',
    get innerHTML(){return html;},
    set innerHTML(value){
      html=value;
      pageButtons=[...value.matchAll(/<button[^>]*data-page="([^"]+)" data-direction="([^"]+)"([^>]*)>/g)].map(m=>({
        dataset:{page:m[1],direction:m[2],next:m[3].match(/data-next="([^"]*)"/)?.[1]||''},
        disabled:m[3].includes('disabled'),addEventListener(_event,listener){this.click=listener;},
      }));
      actionButtons=[...value.matchAll(/<button[^>]*data-asset="([^"]+)" data-action="([^"]+)" data-revision="([^"]+)"([^>]*)>/g)].map(m=>({
        dataset:{asset:m[1],action:m[2],revision:m[3]},
        disabled:m[4].includes('disabled'),addEventListener(_event,listener){this.click=listener;},
      }));
    },
    querySelectorAll(selector){return selector==='button[data-page]'?pageButtons:selector==='button[data-action]'?actionButtons:[];},
    insertAdjacentHTML(_position,value){html=value+html;},
  };
  const context=vm.createContext({window:{confirm:()=>true,alert:message=>alerts.push(message)},document:{getElementById:()=>root},fetch,IS_ROOT:true,Date,location:{},AbortController,...timers});
  vm.runInContext(source,context);
  return {api:context.window.MonitorInfraAssets,root,context,alerts,
    actionButton:action=>actionButtons.find(b=>b.dataset.action===action),
    button:(page,direction)=>pageButtons.find(b=>b.dataset.page===page&&b.dataset.direction===direction)};
}
const asset=(overrides={})=>({id:'a'.repeat(64),resource:'worker',identity:'arn:task:1',kind:'ecs_task',parent:'ecs/cluster/service',platform:'ECS/Fargate',state:'active',revision:1,status:'cloud_running',...overrides});

test('resource manager groups ECS tasks under services and distinguishes cloud state from logs',()=>{
  const {api,root}=ui();
  api.render({active:[asset(),asset({id:'b'.repeat(64),identity:'arn:task:2',resource:'worker2'})],archived:[]},true);
  assert.equal((root.innerHTML.match(/data-group="ecs\/cluster\/service"/g)||[]).length,1);
  assert.match(root.innerHTML,/2 个任务记录/);
  assert.match(root.innerHTML,/不代表日志采集成功/);
  assert.match(root.innerHTML,/下线归档/);
});

test('archive buttons follow backend freshness gates; archived rows are not silently restored',()=>{
  const {api,root}=ui();
  api.render({active:[],archived:[asset({state:'archived',can_restore:false,can_remove:true})]},true);
  assert.match(root.innerHTML,/data-action="restore"[^>]*disabled/);
  assert.doesNotMatch(root.innerHTML,/data-action="remove"[^>]*disabled/);
  api.render({active:[],archived:[asset({state:'archived',can_restore:true,can_remove:false})]},true);
  assert.match(root.innerHTML,/检测到新的实时更新，可恢复/);
  assert.match(root.innerHTML,/下线归档（1）/);
  assert.doesNotMatch(root.innerHTML,/data-action="restore"[^>]*disabled/);
  assert.match(root.innerHTML,/data-action="remove"[^>]*disabled/);
});

test('read-only users have no action controls; names are escaped; null lists do not crash',()=>{
  const {api,root}=ui();
  api.render({active:[asset({resource:'<img src=x onerror=alert(1)>',identity:'"><script>x</script>'})],archived:null},false);
  assert.doesNotMatch(root.innerHTML,/<img|<script|data-action=/);
  assert.match(root.innerHTML,/&lt;img/);
  api.render({active:null,archived:null},true);
  assert.match(root.innerHTML,/暂无资源/);
});

test('late inventory response cannot overwrite a newer resource list',async()=>{
  let finish;
  let calls=0;
  const {api,root}=ui(async()=>{
    if (++calls===1) return {ok:true,json:()=>new Promise(resolve=>{finish=resolve})};
    return {ok:true,json:async()=>({active:[asset({resource:'new-worker'})],archived:[]})};
  });
  const first=api.load();await Promise.resolve();await api.load();
  finish({active:[asset({resource:'old-worker'})],archived:[]});await first;
  assert.match(root.innerHTML,/new-worker/);assert.doesNotMatch(root.innerHTML,/old-worker/);
});

test('inventory failure is not rendered as an empty healthy list',async()=>{
  const {api,root}=ui(async()=>({ok:false,json:async()=>({error:'AWS 目录不可用'})}));
  await api.load();assert.match(root.textContent,/不能据此判断实例已下线/);
});

test('sampling uncertainty and archive warning do not claim complete log health',()=>{
  const {api,root}=ui();
  api.render({active:[asset({status:'sample_unconfirmed'})],archived:[]});
  assert.match(root.innerHTML,/采样时间无效或已过期/);
  assert.match(root.innerHTML,/日志链路告警独立管理/);
  assert.doesNotMatch(root.innerHTML,/归档后不再发离线告警/);
});

test('active and archived cursors have independent navigation controls',()=>{
  const {api,root}=ui();
  api.render({active:[],archived:[asset({state:'archived'})],archived_next:'b'.repeat(64)});
  assert.match(root.innerHTML,/data-page="active" data-direction="next"[^>]*disabled/);
  assert.doesNotMatch(root.innerHTML,/data-page="archived" data-direction="next"[^>]*disabled/);
  assert.match(root.innerHTML,/归档资源 · 第 1 页/);
});

test('double Next and timer refresh cannot repeat a cursor while navigation is pending',async()=>{
  const calls=[];let complete;
  const {api,root,button}=ui(url=>{
    calls.push(url);
    return new Promise(resolve=>{complete=data=>resolve({ok:true,json:async()=>data});});
  });
  api.render({active:[],archived:[],archived_next:'b'.repeat(64)});
  const next=button('archived','next');
  const navigation=next.click();
  assert.equal(next.disabled,true);
  next.click();await api.load();
  assert.equal(calls.length,1);
  assert.match(root.innerHTML,/归档资源 · 第 1 页/);
  complete({active:[],archived:[asset({resource:'second-page',state:'archived'})]});
  await navigation;
  assert.match(root.innerHTML,/归档资源 · 第 2 页/);
  assert.doesNotMatch(root.innerHTML,/第 3 页/);
  assert.match(root.innerHTML,/当前资源 · 第 1 页/);
  const back=button('archived','prev').click();
  assert.match(calls[1],/archived_after=$/);
  complete({active:[],archived:[],archived_next:'b'.repeat(64)});
  await back;
  assert.match(root.innerHTML,/归档资源 · 第 1 页/);
});

test('failed navigation preserves the committed page and allows a correct retry',async()=>{
  for(const failure of ['http','network','malformed']){
    const calls=[];let attempt=0;
    const {api,root,button}=ui(async url=>{
      calls.push(url);
      if(++attempt===1){
        if(failure==='network')throw Error('network unavailable');
        return {ok:failure==='malformed',json:async()=>failure==='malformed'?null:{error:'<bad gateway>'}};
      }
      return {ok:true,json:async()=>({active:[],archived:[asset({resource:'second-page',state:'archived'})]})};
    });
    api.render({active:[],archived:[asset({resource:'first-page',state:'archived'})],archived_next:'c'.repeat(64)});
    await button('archived','next').click();
    assert.match(root.innerHTML,/翻页失败，仍显示原页/);
    assert.match(root.innerHTML,/归档资源 · 第 1 页/);
    assert.match(root.innerHTML,/first-page/);
    assert.doesNotMatch(root.innerHTML,/<bad gateway>/);
    assert.equal(button('archived','next').disabled,false);
    await button('archived','next').click();
    assert.equal(calls[0],calls[1]);
    assert.match(root.innerHTML,/归档资源 · 第 2 页/);
    assert.doesNotMatch(root.innerHTML,/翻页失败/);
  }
});

test('navigation timeout releases the lock and preserves the committed cursor',async()=>{
  let expire,cleared=false;
  const {api,root,button}=ui((_url,{signal})=>new Promise((_resolve,reject)=>{
    signal.addEventListener('abort',()=>reject(Error('request timeout')),{once:true});
  }),{setTimeout(fn,ms){assert.equal(ms,15000);expire=fn;return 1;},clearTimeout(){cleared=true;}});
  api.render({active:[],archived:[],archived_next:'d'.repeat(64)});
  const request=button('archived','next').click();expire();await request;
  assert.equal(cleared,true);
  assert.match(root.innerHTML,/归档资源 · 第 1 页/);
  assert.match(root.innerHTML,/翻页失败/);
  assert.equal(button('archived','next').disabled,false);
});

function manualDeadlines() {
  let serial=0;
  const pending=new Map();
  return {
    timers:{setTimeout(fn,ms){assert.equal(ms,15000);pending.set(++serial,fn);return serial;},clearTimeout(id){pending.delete(id);}},
    expire(){assert.equal(pending.size,1);[...pending.values()][0]();},
    assertCleared(){assert.equal(pending.size,0);},
  };
}

test('all resource writes time out without retries and reconcile before another manual action',async()=>{
  for(const operation of ['archive','restore','remove']) {
    for(const phase of ['headers','body']) {
      const deadlines=manualDeadlines();let posts=0,reads=0;
      const row=asset({state:operation==='archive'?'active':'archived',can_restore:true,can_remove:true});
      const data={active:row.state==='active'?[row]:[],archived:row.state==='archived'?[row]:[]};
      const view=ui((url,{signal})=>{
        if(url!=='/infra/assets/action'){reads++;return Promise.resolve({ok:true,json:async()=>data});}
        posts++;
        const stalled=new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>reject(Error('timeout')),{once:true}));
        return phase==='headers'?stalled:Promise.resolve({ok:true,json:()=>stalled});
      },deadlines.timers);
      view.api.render(data);
      const button=view.actionButton(operation);
      const request=button.click();button.click();
      await Promise.resolve();
      assert.equal(posts,1);assert.equal(button.disabled,true);
      deadlines.expire();await request;
      assert.equal(posts,1);assert.equal(reads,1);
      assert.match(view.alerts[0],/结果暂未确认.*勿重复提交/);
      assert.equal(view.actionButton(operation).disabled,false);
      deadlines.assertCleared();
    }
  }
});

test('lost ACK reconciles a committed archive without a second POST',async()=>{
  let posts=0;
  const view=ui(async url=>{
    if(url==='/infra/assets/action'){posts++;throw Error('connection lost');}
    return {ok:true,json:async()=>({active:[],archived:[asset({state:'archived',revision:2,can_remove:true})]})};
  });
  view.api.render({active:[asset()],archived:[]});
  await view.actionButton('archive').click();
  assert.equal(posts,1);
  assert.equal(view.actionButton('archive'),undefined);
  assert.equal(view.actionButton('remove').dataset.revision,'2');
  assert.match(view.alerts[0],/结果暂未确认/);
});

test('successful write releases controls even if the independent dashboard refresh never finishes',async()=>{
  let refreshes=0;
  const view=ui(async url=>({ok:true,json:async()=>url==='/infra/assets/action'?{ok:true}:{active:[],archived:[asset({state:'archived',revision:2,can_remove:true})]}}));
  view.context.loadInfra=()=>{refreshes++;return new Promise(()=>{});};
  view.api.render({active:[asset()],archived:[]});
  await view.actionButton('archive').click();await Promise.resolve();
  assert.equal(refreshes,1);
  assert.equal(view.actionButton('remove').disabled,false);
  assert.deepEqual(view.alerts,[]);
});

test('failed post-commit dashboard refresh is not reported as a failed write',async()=>{
  const view=ui(async url=>({ok:true,json:async()=>url==='/infra/assets/action'?{ok:true}:{active:[],archived:[]}}));
  view.context.loadInfra=async()=>{throw Error('dashboard unavailable');};
  view.api.render({active:[asset()],archived:[]});
  await view.actionButton('archive').click();
  for(let i=0;i<5;i++)await Promise.resolve();
  assert.equal(view.alerts.length,1);
  assert.match(view.alerts[0],/操作已完成.*刷新失败/);
});
