(function(){
'use strict';
const DAY=86400000,OFFSET=8*3600000;
const $=id=>document.getElementById(id);
const today=()=>day(Date.now());
function day(ms){return new Date(ms+OFFSET).toISOString().slice(0,10)}
function parseDay(value){
  if(!/^\d{4}-\d{2}-\d{2}$/.test(value))throw Error('日期格式应为 YYYY-MM-DD');
  const ms=Date.parse(value+'T00:00:00+08:00');
  if(!Number.isFinite(ms)||day(ms)!==value)throw Error('日期不存在，请重新选择');
  return ms;
}
function shift(value,days){return day(parseDay(value)+days*DAY)}
function calendar(start,end){
  const from=parseDay(start),to=parseDay(end)+DAY;
  if(to<=from)throw Error('结束日期不能早于开始日期');
  if(to-from>7*DAY)throw Error('单次查询最多 7 天');
  if(end>today())throw Error('不能选择未来日期');
  return {from:new Date(from).toISOString(),to:new Date(to).toISOString(),range:'calendar'};
}
function context(s){
  return s.from||s.range?{from:s.from,to:s.to,range:s.range||'exact'}:{window:s.window||'24h',...(s.to?{to:s.to}:{})};
}
function restore(s,c){
  if(c.from!==undefined){s.from=c.from;s.to=c.to||'';s.range=c.range||'exact';return}
  if(c.window!==undefined||c.to!==undefined){s.window=c.window||'24h';s.from='';s.to=c.to||'';s.range=''}
}
function apply(s,c){s.window=c.window||s.window||'24h';s.from=c.from||'';s.to=c.to||'';s.range=c.range||''}
function describe(r){
  if(!r||r.mode==='rolling')return '';
  const from=Date.parse(r.requested_from),to=Date.parse(r.requested_to);
  const selected=r.mode==='calendar'?'选定日期 '+day(from)+' 至 '+day(to-1)+'（含结束日）。':'固定起止核对。';
  return selected+(r.note||'');
}
let picker=null,calendarTarget=null;
function openCalendar(target,minimum,showError){
  const dialog=$('modelDateDialog');
  if(!window.flatpickr||!dialog?.showModal){showError('日期选择器不可用，请直接输入 YYYY-MM-DD。');return}
  calendarTarget=target;
  if(!picker){
    picker=window.flatpickr($('modelDateCalendarInput'),{inline:true,appendTo:$('modelDateCalendar'),locale:'zh',dateFormat:'Y-m-d',disableMobile:true});
    picker.monthsDropdownContainer?.addEventListener('keyup',event=>event.stopPropagation());
    $('modelDateConfirm').addEventListener('click',()=>{
      if(!picker.selectedDates[0]||!calendarTarget)return;
      calendarTarget.value=picker.formatDate(picker.selectedDates[0],'Y-m-d');
      calendarTarget.dispatchEvent(new Event('change',{bubbles:true}));
      dialog.close();calendarTarget.focus();
    });
    $('modelDateCancel').addEventListener('click',()=>dialog.close());
  }
  picker.set('minDate',minimum||null);picker.set('maxDate',today());
  let initial=target.value;
  try{parseDay(initial)}catch{initial=today()}
  if(minimum&&initial<minimum)initial=minimum;
  if(initial>today())initial=today();
  picker.setDate(initial,false,'Y-m-d');
  $('modelDateDialogTitle').textContent=target.id.endsWith('Start')?'选择开始日期':'选择结束日期（包含当天）';
  dialog.showModal();
  picker.calendarContainer.querySelector('.selected')?.focus();
}
function mount(prefix,getSelection,onChange){
  const root=$(prefix+'Dates');
  if(!root)return {sync(){}};
  const start=$(prefix+'DateStart'),end=$(prefix+'DateEnd'),note=$(prefix+'DateNote');
  let last='',minimum='',maxDays=7;
  const error=message=>{note.textContent=message;note.classList.add('md-error')};
  function submit(single=false){
    try{
      const lastDate=single?start.value:end.value;
      const selection=calendar(start.value,lastDate);
      if(minimum&&start.value<minimum)throw Error('所选日期超出本地分钟事实留存；最早可选 '+minimum);
      if(parseDay(lastDate)-parseDay(start.value)+DAY>maxDays*DAY)throw Error('当前留存下单次最多 '+maxDays+' 天');
      note.classList.remove('md-error');onChange(selection);
    }catch(e){error(e.message)}
  }
  root.addEventListener('submit',event=>{event.preventDefault();submit()});
  root.querySelectorAll('[data-md-action]').forEach(button=>button.addEventListener('click',()=>{
    const action=button.dataset.mdAction;
    if(action==='start'||action==='end'){openCalendar(action==='start'?start:end,minimum,error);return}
    if(action==='reset'){onChange({window:'24h'});return}
    if(action==='single'){submit(true);return}
    try{
      if(action==='today'||action==='yesterday'){start.value=end.value=shift(today(),action==='today'?0:-1)}
      else {const delta=action==='previous'?-1:1;start.value=shift(start.value,delta);end.value=shift(end.value,delta)}
      submit();
    }catch(e){error(e.message)}
  }));
  return {sync(selection=getSelection(),report){
    if(report?.range){
      const r=report.range,raw=Date.parse(r.retained_from);
      if(Number.isFinite(raw)){
        const candidate=day(raw);
        minimum=parseDay(candidate)<raw?shift(candidate,1):candidate;
      }
      if(Number.isInteger(r.max_days)&&r.max_days>0)maxDays=r.max_days;
    }
    const key=JSON.stringify(context(selection));
    if(last!==key){
      last=key;
      if(selection.from&&Number.isFinite(Date.parse(selection.from))&&Number.isFinite(Date.parse(selection.to))){
        start.value=day(Date.parse(selection.from));end.value=day(Date.parse(selection.to)-1);
      }else start.value=end.value=today();
    }
    for(const input of [start,end]){input.dataset.minDate=minimum;input.dataset.maxDate=today()}
    root.querySelector('[data-md-action="next"]').disabled=end.value>=today();
    root.querySelector('[data-md-action="previous"]').disabled=!!minimum&&start.value<=minimum;
    root.querySelectorAll('[data-md-action="today"],[data-md-action="yesterday"]').forEach(b=>{
      const d=shift(today(),b.dataset.mdAction==='today'?0:-1);
      b.classList.toggle('active',selection.range==='calendar'&&start.value===d&&end.value===d);
    });
    note.classList.remove('md-error');note.textContent='北京时间 · 结束日包含当天 · 单次最多 '+maxDays+' 天'+(minimum?' · 最早可选 '+minimum:'')+'；仅查询本地事实。';
  }};
}
window.ModelReportDates={day,today,parseDay,shift,calendar,context,restore,apply,describe,mount};
})();
