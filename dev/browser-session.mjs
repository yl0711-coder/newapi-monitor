// Real Chrome/CDP with a task-owned D-drive profile. No third-party runtime.
import fs from 'node:fs';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
export async function browserSession({browser,scratch,origin}){
 const root=path.resolve(scratch);fs.mkdirSync(root,{recursive:true});
 if(process.platform==='win32'&&!/^D:/i.test(root))throw Error('Browser scratch must be on D:');
 const profile=fs.mkdtempSync(path.join(root,'profile-')),temp=fs.mkdtempSync(path.join(root,'temp-'));
 const proc=spawn(browser,['--headless=new','--remote-debugging-pipe','--no-first-run','--no-default-browser-check','--disable-background-networking','--disable-component-update','--disable-sync','--disable-default-apps','--disable-client-side-phishing-detection','--metrics-recording-only','--lang=zh-CN','--window-size=1280,950','--user-data-dir='+profile,'about:blank'],{windowsHide:true,stdio:['ignore','pipe','pipe','pipe','pipe'],env:{...process.env,TEMP:temp,TMP:temp}});
 let id=0,buffer='',session;const pending=new Map(),exceptions=[],external=[];proc.stderr.on('data',()=>{});
 function call(method,params={},sid=session){return new Promise((resolve,reject)=>{const seq=++id,timer=setTimeout(()=>{pending.delete(seq);reject(Error('CDP timeout: '+method))},15000);pending.set(seq,{resolve,reject,timer});proc.stdio[3].write(JSON.stringify({id:seq,method,params,...(sid?{sessionId:sid}:{})})+'\0')})}
 proc.stdio[4].on('data',chunk=>{buffer+=chunk.toString();for(let at;(at=buffer.indexOf('\0'))>=0;){const raw=buffer.slice(0,at);buffer=buffer.slice(at+1);if(!raw)continue;const msg=JSON.parse(raw),p=pending.get(msg.id);if(p){pending.delete(msg.id);clearTimeout(p.timer);msg.error?p.reject(Error(JSON.stringify(msg.error))):p.resolve(msg.result)}else if(msg.method==='Runtime.exceptionThrown')exceptions.push(msg.params.exceptionDetails);else if(msg.method==='Fetch.requestPaused'){const allowed=msg.params.request.url.startsWith(origin+'/');if(!allowed)external.push(msg.params.request.url);call(allowed?'Fetch.continueRequest':'Fetch.failRequest',{requestId:msg.params.requestId,...(allowed?{}:{errorReason:'BlockedByClient'})},msg.sessionId).catch(()=>{})}}});
 async function evaluate(expression){const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value}
 const pause=ms=>new Promise(r=>setTimeout(r,ms));
 async function waitFor(expression,ms=8000){const end=Date.now()+ms;do{try{if(await evaluate(expression))return}catch(e){if(!/Inspected target navigated|Execution context was destroyed|Cannot find context with specified id/.test(e.message))throw e}await pause(80)}while(Date.now()<end);throw Error('waitFor: '+expression)}
 async function click(selector){const pos=await evaluate(`(()=>{const e=document.querySelector(${JSON.stringify(selector)});if(!e)throw Error('missing click target');e.scrollIntoView({block:'center',inline:'nearest'});const r=e.getBoundingClientRect(),x=r.x+r.width/2,y=r.y+r.height/2;return {x,y,ok:!e.disabled&&!!r.width&&!!r.height&&(e===document.elementFromPoint(x,y)||e.contains(document.elementFromPoint(x,y)))};})()`);if(!pos.ok)throw Error('Not clickable: '+selector);for(const type of ['mousePressed','mouseReleased'])await call('Input.dispatchMouseEvent',{type,x:pos.x,y:pos.y,button:'left',clickCount:1})}
 async function key(key,modifiers=0){const windowsVirtualKeyCode=({Tab:9,Enter:13,Escape:27,Home:36,End:35,ArrowUp:38,ArrowDown:40,ArrowLeft:37,ArrowRight:39,Backspace:8})[key]||key.toUpperCase().charCodeAt(0);for(const type of ['keyDown','keyUp'])await call('Input.dispatchKeyEvent',{type,key,code:key,windowsVirtualKeyCode,modifiers})}
 async function input(selector,value){await click(selector);await key('a',2);await call('Input.insertText',{text:value});await key('Tab')}
 async function screenshot(file){const r=await call('Page.captureScreenshot',{format:'png'});fs.writeFileSync(file,Buffer.from(r.data,'base64'))}
 // Authenticate before loading the application's login page. Its own redirect
 // can destroy an in-page Runtime.evaluate while a login fetch is resolving.
 // Cookies still come from the real server; no page assets or responses change.
 async function authenticate({username=process.env.MONITOR_LOCAL_USER||'local',password=process.env.MONITOR_LOCAL_PASSWORD||'local'}={}){
  const response=await fetch(origin+'/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username,password}),signal:AbortSignal.timeout(10000)});
  if(response.status!==200)throw Error('Browser authentication HTTP '+response.status);
  const cookies=response.headers.getSetCookie().map(raw=>{const pair=raw.split(';')[0],at=pair.indexOf('=');if(at<=0)throw Error('Invalid authentication cookie');return{name:pair.slice(0,at),value:pair.slice(at+1),url:origin+'/',httpOnly:/;\s*httponly(?:;|$)/i.test(raw),secure:/;\s*secure(?:;|$)/i.test(raw)}});
  if(!cookies.length)throw Error('Authentication returned no session cookie');
  await call('Network.setCookies',{cookies});return response.status;
 }
 async function close(){for(const p of pending.values()){clearTimeout(p.timer);p.reject(Error('browser closing'))}pending.clear();const exited=once(proc,'exit');try{await call('Browser.close',{},null)}catch{proc.kill()}await Promise.race([exited,pause(5000)]);if(proc.exitCode===null)proc.kill();for(const dir of [profile,temp]){if(!path.resolve(dir).startsWith(root+path.sep))throw Error('Cleanup scope mismatch');fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:250})}}
 let version,target;
 try{
  version=await call('Browser.getVersion',{},null);target=await call('Target.createTarget',{url:'about:blank'},null);({sessionId:session}=await call('Target.attachToTarget',{targetId:target.targetId,flatten:true},null));
  await call('Page.enable');await call('Runtime.enable');await call('Fetch.enable',{patterns:[{urlPattern:'http*'}]});await call('Emulation.setDeviceMetricsOverride',{width:1280,height:950,deviceScaleFactor:1,mobile:false});await call('Emulation.setTimezoneOverride',{timezoneId:'America/New_York'});
 }catch(e){await close();throw e}
 return {call,evaluate,waitFor,click,key,input,screenshot,authenticate,pause,version,exceptions,external,targetId:target.targetId,close};
}
