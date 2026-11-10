// Isolated DOM regression for the actual shared refresh helper. No production
// APIs or data; model order changes are deliberate synthetic fixture mutations.
import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
import {createServer} from 'node:http';import {once} from 'node:events';
import {browserSession} from './browser-session.mjs';
const args=process.argv.slice(2),opt=k=>args[args.indexOf(k)+1];
assert.ok(args.includes('--out')&&args.includes('--scratch')&&args.includes('--browser'));
const out=path.resolve(opt('--out'));fs.mkdirSync(out,{recursive:true});
const html=`<!doctype html><html><head><style>body{margin:0}header{height:200px}table{width:100%;border-collapse:collapse}td{height:42px}button{font:16px sans-serif}details{padding:20px}footer{height:1000px}</style></head><body><header>Local synthetic reorder test</header><main id="root"></main><footer></footer><script src="/view-refresh.js"></script><script>
window.order=Array.from({length:70},(_,i)=>i);
window.render=items=>'<div id="msModels"><div><table><tbody>'+items.map(i=>'<tr><td><button data-ms-expand="model-'+i+'">model-'+i+'</button></td><td>'+i+'</td></tr>').join('')+'</tbody></table></div></div>';
root.innerHTML=render(order);
</script></body></html>`;
const server=createServer((req,res)=>{res.setHeader('Content-Type',req.url==='/view-refresh.js'?'application/javascript':'text/html;charset=utf-8');res.end(req.url==='/view-refresh.js'?fs.readFileSync('monitor/view_refresh.js'):html)});
server.listen(0,'127.0.0.1');await once(server,'listening');
const origin='http://127.0.0.1:'+server.address().port;
const b=await browserSession({browser:opt('--browser'),scratch:opt('--scratch'),origin}),{evaluate:ev}=b;const checks=[];let failure;
try{
 await b.call('Page.navigate',{url:origin+'/'});await b.waitFor('!!window.MonitorView');
 await ev("window.scrollTo(0,861);document.querySelector('[data-ms-expand=\"model-20\"]').focus({preventScroll:true})");
 const before=await ev("({top:document.querySelector('[data-ms-expand=\"model-20\"]').getBoundingClientRect().top,scroll:scrollY,focused:document.activeElement.getAttribute('data-ms-expand')})");
 await ev("MonitorView.preserve(root,()=>{order=[...order.slice(5),...order.slice(0,5)];root.innerHTML=render(order)})");
 const after=await ev("({top:document.querySelector('[data-ms-expand=\"model-20\"]').getBoundingClientRect().top,scroll:scrollY,focused:document.activeElement.getAttribute('data-ms-expand')})");
 checks.push({name:'Stable model button below positional table ancestors survives reordered results',before,after});
 assert.equal(after.focused,before.focused,'reordered model result lost keyboard focus');assert.ok(Math.abs(after.top-before.top)<1,'reordered model result lost visible-row position');
 checks[0].status='pass';
 await ev(`root.innerHTML='<div><table><tbody><tr data-ms-group-key="group-A"><td><button data-ms-group-key="group-A">group A</button></td></tr></tbody></table></div>';document.querySelector('button[data-ms-group-key]').focus();MonitorView.preserve(root,()=>root.innerHTML=root.innerHTML)`);
 assert.equal(await ev("document.activeElement.tagName"),'BUTTON','row and button share a key but are different elements');
 checks.push({name:'Equal group key on row and button preserves the button focus',status:'pass'});
 await ev(`root.innerHTML='<div data-ms-section-key="first"><div data-ms-channel-key="channel-7"><input value="first"></div></div><div data-ms-section-key="second"><div data-ms-channel-key="channel-7"><input value="second"></div></div>';const input=document.querySelector('[data-ms-section-key="second"] input');input.focus();input.value='draft';input.setSelectionRange(1,4);MonitorView.preserve(root,()=>root.innerHTML='<div data-ms-section-key="second"><div data-ms-channel-key="channel-7"><input value="server-second"></div></div><div data-ms-section-key="first"><div data-ms-channel-key="channel-7"><input value="server-first"></div></div>')`);
 const field=await ev("({section:document.activeElement.closest('[data-ms-section-key]')?.getAttribute('data-ms-section-key'),value:document.activeElement.value,selection:[document.activeElement.selectionStart,document.activeElement.selectionEnd]})");
 assert.deepEqual(field,{section:'second',value:'draft',selection:[1,4]});checks.push({name:'Repeated channel IDs remain scoped to their reordered parent section',status:'pass',field});
 await ev("order=Array.from({length:70},(_,i)=>i);root.innerHTML=render(order);window.scrollTo(0,861);document.querySelector('[data-ms-expand=\"model-20\"]').focus({preventScroll:true})");
 const focusedBefore=await ev("document.activeElement.getBoundingClientRect().top");
 await ev("MonitorView.preserve(root,()=>{order=order.filter(i=>i!==16).concat(16);root.innerHTML=render(order)})");
 const focusedAfter=await ev("({top:document.activeElement.getBoundingClientRect().top,model:document.activeElement.getAttribute('data-ms-expand')})");
 checks.push({name:'A different visible row changing rank cannot move the focused model off screen',before:focusedBefore,after:focusedAfter});
 assert.equal(focusedAfter.model,'model-20');assert.ok(Math.abs(focusedAfter.top-focusedBefore)<1,'another row became the scroll anchor and displaced the focused model');checks.at(-1).status='pass';
 await ev("order=Array.from({length:70},(_,i)=>i);root.innerHTML=render(order);window.scrollTo(0,861);document.querySelector('[data-ms-expand=\"model-20\"]').focus({preventScroll:true})");
 const nestedBefore=await ev("document.activeElement.getBoundingClientRect().top");
 // Layout changes may also move the browser's own scroll anchor. Exercise
 // that movement explicitly instead of relying on Chrome's heuristic.
 await ev("MonitorView.preserve(root,()=>{root.insertAdjacentHTML('afterbegin','<div id=refreshError style=height:60px>Refresh error</div>');window.scrollBy({top:70,behavior:'instant'});MonitorView.preserve(root,()=>refreshError.style.height='130px')})");
 const nestedAfter=await ev("({top:document.activeElement.getBoundingClientRect().top,model:document.activeElement.getAttribute('data-ms-expand')})");
 checks.push({name:'Nested status and table updates preserve one visible anchor',before:nestedBefore,after:nestedAfter});
 assert.equal(nestedAfter.model,'model-20');assert.ok(Math.abs(nestedAfter.top-nestedBefore)<1,'nested restoration displaced the focused row');checks.at(-1).status='pass';
}catch(e){failure=e;console.error(e.message)}finally{
 fs.writeFileSync(path.join(out,'result.json'),JSON.stringify({at:new Date().toISOString(),kind:'real Chrome, synthetic DOM, actual view_refresh.js; not final-image acceptance',checks,failure:failure?.message||null},null,2)+'\n');await b.close();await new Promise(resolve=>server.close(resolve));
}
if(failure)process.exitCode=1;else console.log('PASS '+checks.length+' refresh identity regressions');
