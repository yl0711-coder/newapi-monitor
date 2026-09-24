(function(){
'use strict';

const state={inited:false,loading:false,window:'24h',generation:0,abort:null,report:null,expanded:new Set(),expandedGroups:new Set()};
const $=id=>document.getElementById(id);
const esc=value=>String(value==null?'':value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=value=>(Number(value)||0).toLocaleString('zh-CN');
const pct=(value,total)=>total>0?((value*100)/total).toFixed(1)+'%':'—';
const WINDOW_LABEL={'24h':'近24小时','3d':'近3天','7d':'近7天'};

window.modelStatisticsActivate=function(){
  if(!state.inited)init();
  load(state.window);
};
window.modelStatisticsDeactivate=function(){
  if(!state.inited)return;
  state.generation++;
  state.abort?.abort();
  state.abort=null;
  state.loading=false;
};

function init(){
  state.inited=true;
  document.querySelectorAll('[data-ms-window]').forEach(button=>button.addEventListener('click',()=>{
    const key=button.getAttribute('data-ms-window');
    if(WINDOW_LABEL[key])load(key);
  }));
  $('msModels')?.addEventListener('click',event=>{
    const groupButton=event.target.closest('[data-ms-group-key]');
    if(groupButton){
      const key=groupButton.getAttribute('data-ms-group-key');
      if(state.expandedGroups.has(key))state.expandedGroups.delete(key);else state.expandedGroups.add(key);
      if(state.report)render(state.report);
      return;
    }
    const button=event.target.closest('[data-ms-expand]');
    if(!button)return;
    const model=button.getAttribute('data-ms-expand');
    if(state.expanded.has(model))state.expanded.delete(model);else state.expanded.add(model);
    if(state.report)render(state.report);
  });
}

async function load(windowKey){
  if(!WINDOW_LABEL[windowKey])return;
  state.window=windowKey;
  state.abort?.abort();
  const controller=new AbortController();
  state.abort=controller;
  const generation=++state.generation;
  state.loading=true;
  setActiveWindow();
  setStatus('正在读取本地模型事实…','');
  try{
    const response=await fetch('/model-statistics/report?window='+encodeURIComponent(windowKey),{
      headers:{'Accept':'application/json'},cache:'no-store',signal:controller.signal
    });
    if(response.status===401){location.href='/login';return}
    const body=await response.text();
    let data={};try{data=body?JSON.parse(body):{}}catch(_){throw new Error('接口返回了无效 JSON')}
    if(!response.ok)throw new Error(data.error||'模型统计读取失败');
    if(generation!==state.generation)return;
    state.report=data;
    render(data);
    setStatus('已更新 '+new Date(Number(data.generated_at||0)*1000).toLocaleString('zh-CN',{hour12:false}),'ok');
  }catch(error){
    if(error?.name==='AbortError')return;
    if(generation!==state.generation)return;
    state.report=null;
    renderError(error?.message||'模型统计读取失败');
  }finally{
    if(generation===state.generation){state.loading=false;state.abort=null}
  }
}

function setActiveWindow(){
  document.querySelectorAll('[data-ms-window]').forEach(button=>button.classList.toggle('active',button.getAttribute('data-ms-window')===state.window));
}
function setStatus(text,kind){
  const target=$('msStatus');if(!target)return;
  target.textContent=text;target.className='ms-status '+(kind||'');
}
function renderError(message){
  const error=$('msError');if(error){error.hidden=false;error.textContent=message}
  const summary=$('msSummary');if(summary)summary.innerHTML='';
  const models=$('msModels');if(models)models.innerHTML='<div class="ms-empty">暂时无法读取模型统计。</div>';
}
function render(report){
  const error=$('msError');if(error){error.hidden=true;error.textContent=''}
  const models=Array.isArray(report.models)?report.models:[];
  const total=models.reduce((sum,item)=>sum+(Number(item.requests)||0),0);
  const unavailable=models.reduce((sum,item)=>sum+(Number(item.unavailable_channel_requests)||0),0);
  if($('msSummary'))$('msSummary').innerHTML=[
    summaryCard('统计模型',models.length,'个模型'),
    summaryCard('统计分组',Number(report.group_count)||0,'个分组'),
    summaryCard('模型请求总数',total,'次（已路由 + 无可用渠道）'),
    summaryCard('已进入渠道',total-unavailable,'次'),
    summaryCard('无可用渠道',unavailable,'次，表示客户有需求但当前没有可用渠道')
  ].join('');
  if($('msCoverage')){
    const complete=report.source?.facts_complete===true;
    const note=report.source?.note||'已路由请求与前置无可用渠道请求按互斥事实合并统计。';
    $('msCoverage').textContent=(complete?'数据覆盖已确认：':'数据覆盖未确认：')+note;
    $('msCoverage').classList.toggle('incomplete',!complete);
  }
  if(!$('msModels'))return;
  $('msModels').innerHTML=models.length?renderModels(models):'<div class="ms-empty">当前时间范围没有可用的模型请求记录。</div>';
}
function summaryCard(title,value,note){
  return '<div class="ms-summary-card"><small>'+esc(title)+'</small><b>'+num(value)+'</b><span>'+esc(note)+'</span></div>';
}
function renderModels(models){
  const total=models.reduce((sum,item)=>sum+(Number(item.requests)||0),0);
  let rows='';
  models.forEach(model=>{
    const name=model.model||'未标注模型';
    const open=state.expanded.has(name);
    const groups=Array.isArray(model.groups)?model.groups:[];
    const requests=Math.max(Number(model.requests)||0,0);
    const unavailable=Math.max(Number(model.unavailable_channel_requests)||0,0);
    const highUnavailable=requests>0&&unavailable/requests>0.4;
    rows+='<tr class="ms-model-row'+(open?' is-open':'')+(highUnavailable?' ms-model-row-high-unavailable':'')+'"'+(highUnavailable?' title="无可用渠道请求占比超过40%"':'')+'><td class="ms-model-name"><button type="button" class="ms-expand" data-ms-expand="'+esc(name)+'" aria-expanded="'+String(open)+'" aria-label="'+(open?'收起':'展开')+esc(name)+'">'+(open?'▾':'▸')+'</button><strong>'+esc(name)+'</strong></td><td>'+num(model.requests)+'</td><td>'+num(model.routed_requests)+'</td><td>'+num(model.unavailable_channel_requests)+'</td><td>'+pct(Number(model.requests)||0,total)+'</td></tr>';
    if(open){
      const groupRows=groups.map(group=>renderGroup(name,group,Number(model.requests)||0)).join('');
      rows+='<tr class="ms-detail-row"><td colspan="5"><div class="ms-detail"><div class="ms-detail-title">'+esc(name)+' 的分组明细（点击分组查看客户）</div><table><thead><tr><th>分组</th><th>请求次数</th><th>已进入渠道</th><th>无可用渠道</th><th>占该模型</th></tr></thead><tbody>'+groupRows+'</tbody></table></div></td></tr>';
    }
  });
  return '<div class="ms-model-table-wrap"><table><thead><tr><th>模型（点击展开分组）</th><th>请求次数</th><th>已进入渠道</th><th>无可用渠道</th><th>占全部模型请求</th></tr></thead><tbody>'+rows+'</tbody></table></div>';
}
function renderGroup(modelName,group,modelTotal){
  const groupName=group.group||'未标注分组';
  const key=JSON.stringify([modelName,groupName]);
  const open=state.expandedGroups.has(key);
  const customers=Array.isArray(group.customers)?group.customers:[];
  let html='<tr class="ms-group-row'+(open?' is-open':'')+'" data-ms-group-key="'+esc(key)+'"><td><button type="button" class="ms-expand ms-group-expand" data-ms-group-key="'+esc(key)+'" aria-expanded="'+String(open)+'" aria-label="'+(open?'收起':'展开')+esc(groupName)+'的客户明细">'+(open?'▾':'▸')+'</button><strong>'+esc(groupName)+'</strong></td><td>'+num(group.requests)+'</td><td>'+num(group.routed_requests)+'</td><td>'+num(group.unavailable_channel_requests)+'</td><td>'+pct(Number(group.requests)||0,modelTotal)+'</td></tr>';
  if(!open)return html;
  const customerRows=customers.map(customer=>{
    const customerID=Number(customer.customer_id)||0;
    return '<tr><td>'+(customerID>0?num(customerID):'无法识别')+'</td><td>'+esc(customer.customer_name||'未知客户')+'</td><td>'+num(customer.requests)+'</td><td>'+pct(Number(customer.requests)||0,Number(group.requests)||0)+'</td></tr>';
  }).join('');
  html+='<tr class="ms-customer-detail-row"><td colspan="5"><div class="ms-customer-detail"><div class="ms-detail-title">'+esc(groupName)+' 的客户明细</div><table><thead><tr><th>客户 ID</th><th>客户名</th><th>请求次数</th><th>占该分组</th></tr></thead><tbody>'+(customerRows||'<tr><td colspan="4" class="ms-customer-empty">暂无可识别的客户明细</td></tr>')+'</tbody></table></div></td></tr>';
  return html;
}
})();
