(function(){
'use strict';
// Capture when committing, since the user can scroll or edit during a slow fetch.
const keys=['data-frt-key','data-frt-model-key','data-ms-expand','data-ms-group-key','data-ms-section-key','data-ms-channel-key','data-lc-id','data-ch-row','data-al-row','data-incident-id'];
function identity(el){
  if(el.id)return ['id',el.id];
  for(const key of keys)if(el.hasAttribute(key))return [key,el.getAttribute(key)];
  return null;
}
function path(root,el){
  const parts=[];
  while(el&&el!==root){
    const key=identity(el);
    parts.unshift(key?{key,tag:el.tagName}:{tag:el.tagName,index:Array.from(el.parentElement?.children||[]).filter(n=>n.tagName===el.tagName).indexOf(el)});
    // A stable identity must not depend on positional ancestors: counts can
    // reorder a table while its buttons still represent the same models.
    // Repeated channel identities stay relative to their unique group section.
    if(key&&matches(root,key,el.tagName).length===1)return parts;
    el=el.parentElement;
  }
  return el===root?parts:null;
}
function matches(root,key,tag){return Array.from(root.querySelectorAll('['+key[0]+']')).filter(el=>el.tagName===tag&&el.getAttribute(key[0])===key[1])}
function find(root,parts){
  if(!parts)return null;
  let el=root;
  for(const part of parts){
    el=part.key?matches(el,part.key,part.tag)[0]:Array.from(el.children).filter(n=>n.tagName===part.tag)[part.index];
    if(!el)return null;
  }
  return el;
}
function preserve(root,update){
  if(!root||!root.getClientRects().length)return update();
  const x=window.scrollX,y=window.scrollY,focus=root.contains(document.activeElement)?document.activeElement:null;
  const focused=focus?{path:path(root,focus),start:focus.selectionStart,end:focus.selectionEnd,direction:focus.selectionDirection}:null;
  const fields=Array.from(root.querySelectorAll('input,textarea,select')).map(el=>({path:path(root,el),value:el.value,checked:el.checked}));
  const disclosures=Array.from(root.querySelectorAll('details')).map(el=>({path:path(root,el),open:el.open}));
  const scrollers=Array.from(root.querySelectorAll('*')).filter(el=>el.scrollLeft||el.scrollTop).map(el=>({path:path(root,el),left:el.scrollLeft,top:el.scrollTop}));
  const focusRect=focus?.getBoundingClientRect();
  const visibleFocus=focusRect?.height&&focusRect.bottom>0&&focusRect.top<window.innerHeight;
  // Keep the control the user is operating in view even if another visible
  // row independently changes rank. Off-screen filters must not pull the
  // viewport back to the page header.
  const anchor=visibleFocus?focus:Array.from(root.querySelectorAll(keys.map(k=>'['+k+']').join(','))).find(el=>{const r=el.getBoundingClientRect();return r.height&&r.top>=0&&r.top<window.innerHeight});
  const anchored=anchor?{path:path(root,anchor),top:anchor.getBoundingClientRect().top}:null;
  const minHeight=root.style.minHeight;
  root.style.minHeight=root.getBoundingClientRect().height+'px';
  try{return update()}
  finally{
    for(const field of fields){const el=find(root,field.path);if(el){el.value=field.value;if(typeof field.checked==='boolean')el.checked=field.checked}}
    for(const disclosure of disclosures){const el=find(root,disclosure.path);if(el)el.open=disclosure.open}
    for(const pos of scrollers){const el=find(root,pos.path);if(el){el.scrollLeft=pos.left;el.scrollTop=pos.top}}
    if(focused){const el=find(root,focused.path);if(el){el.focus({preventScroll:true});if(focused.start!=null&&el.setSelectionRange)try{el.setSelectionRange(focused.start,focused.end,focused.direction)}catch(_){}}}
    root.style.minHeight=minHeight;
    const next=anchored&&find(root,anchored.path);
    // Layout, focus restoration or a nested render can already have moved the
    // browser's scroll position. The new rect is relative to that current
    // position; adding the old y would apply the movement a second time.
    window.scrollTo({left:x,top:next?window.scrollY+next.getBoundingClientRect().top-anchored.top:y,behavior:'instant'});
  }
}
function freshness(report){
  if(report?.collection_failed)return {state:'failed',warning:true,text:'最近一次采集失败，正在等待恢复。'};
  const through=report?.actual_through_ts??report?.through_ts,lag=report?.lag_seconds,poll=report?.timing?.collection_poll_seconds;
  if(!(Number(through)>0)||!Number.isFinite(lag)||lag<0)return {state:'unknown',warning:true,text:'实际采集进度未知。'};
  if(lag===0)return {state:'current',warning:false,text:'已追平封口目标。'};
  if(!(Number.isFinite(poll)&&poll>0))return {state:'unknown',warning:true,text:'距封口目标 '+lag+' 秒；采集周期未知，无法判断是否正常。'};
  if(lag>poll)return {state:'backlog',warning:true,text:'采集积压 '+lag+' 秒，超出一个采集周期。'};
  return {state:'waiting',warning:false,text:'正常等待：距封口目标 '+lag+' 秒，处于一个采集周期内。'};
}
function timing(report){
  const t=report?.timing;if(!t)return '';
  return '封口等待 '+t.finalize_delay_seconds+' 秒，按 '+t.alignment_seconds+' 秒对齐；采集周期 '+t.collection_poll_seconds+' 秒。'+freshness(report).text;
}
window.MonitorView={preserve,timing,freshness};
})();
