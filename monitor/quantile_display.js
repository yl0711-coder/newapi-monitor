// Legacy histograms cannot locate a percentile inside the open-ended bucket.
// Keep the observed maximum separate; never present tail interpolation as fact.
(function(){
'use strict';
const estimateFormatter=new Intl.NumberFormat('zh-CN',{maximumFractionDigits:1});
const boundFormatter=new Intl.NumberFormat('zh-CN',{maximumFractionDigits:3});
function format(estimate,bounds,scale,unit,tailEdge){
  if(estimate==null||estimate===''||!Number.isFinite(Number(estimate))||Number(estimate)<=0||!Number.isFinite(scale)||scale<=0)return '—';
  const value=Number(estimate);
  const fmt=n=>estimateFormatter.format(n);
  if(bounds){
    const lower=Number(bounds.lower),upper=Number(bounds.upper);
    if(bounds.valid!==true||bounds.lower==null||bounds.upper==null||!Number.isFinite(lower)||!Number.isFinite(upper)||lower<0||upper<=lower)return '—';
    if(bounds.open_tail===true){
      const bound=n=>boundFormatter.format(n/scale);
      return `(${bound(lower)}, ${bound(upper)}] ${unit}（分桶范围）`;
    }
  }else if(value>tailEdge){
    // Old cached/API payload: the last finite edge is still known, but the
    // maximum/containing bucket metadata is not. Do not invent an upper bound.
    return `>${fmt(tailEdge)} ${unit}（旧分桶）`;
  }
  return `≈${fmt(value)} ${unit}`;
}
window.MonitorQuantiles={format};
})();
