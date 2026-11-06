package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"
)

// The actual first authenticated GET -> actual embedded page script -> actual
// POST/save/download contract. VM DOM, not a browser rendering acceptance.
// CI already installs Node for the monthly suite; missing Node is an explicit
// failure, not a skipped/passing customer journey. No listener or online data.
func TestPortalMonthlyFirstClickActualServerScriptAndSavedBill(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Fatal("Node is required for the server/page contract acceptance", err)
	}
	testCapturedHistoryPublication(t, newCapturedHistoryFixture(t), true, false)
}

func TestPortalMonthlyRegisteredCalendarActualServerScriptAndSavedBill(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Fatal("Node is required for calendar/page acceptance", err)
	}
	testCapturedHistoryPublication(t, newCapturedHistoryFixture(t), true, true)
}

func capturedHistoryFirstButtonRequest(t *testing.T, response, calendar []byte) (string, string, string) {
	t.Helper()
	source, err := os.ReadFile("../../monitor/portal_monthly_generation_local.js")
	if err != nil {
		t.Fatal(err)
	}
	if len(calendar) > 0 {
		source = bytes.Replace(source, []byte("/*MONTHLY_CALENDAR_ENABLED*/ false"), []byte("/*MONTHLY_CALENDAR_ENABLED*/ true"), 1)
	}
	input, err := json.Marshal(struct {
		Source   string          `json:"source"`
		First    json.RawMessage `json:"first"`
		Calendar json.RawMessage `json:"calendar,omitempty"`
	}{string(source), response, calendar})
	if err != nil {
		t.Fatal(err)
	}
	// Feed the REAL API body, not a hand-written not_requested mock. Exercise
	// the production script's disabled state and click listener; return its
	// actual request to the authenticated handler in the parent test process.
	const script = `
const fs=require('node:fs'),vm=require('node:vm');
const {source,first,calendar}=JSON.parse(fs.readFileSync(0,'utf8'));
const nodes={},calls=[];
const node=id=>nodes[id]??={style:{display:'block'},value:'',disabled:false,textContent:'',events:{},before(){},addEventListener(k,fn){this.events[k]=fn;}};
node('monthlyMonth').value=first.month;
const document={hidden:false,createElement:()=>node('panel'),addEventListener(){}};
vm.runInNewContext(source,{document,window:{addEventListener(){}},$:node,
 lastClosedMonth:()=>first.month,AbortController,MutationObserver:class{observe(){}},
 setTimeout(){return 1;},clearTimeout(){},
 fetch(url,options){return new Promise(resolve=>calls.push({url,options,resolve}));}
});
(async()=>{
 if(calls.length!==1||calls[0].options.method!=='GET')throw Error('expected initial read');
 let index=0;
 if(calendar){
  if(calls[0].url!=='/api/monthly-statement/calendar')throw Error('missing registration read');
  calls[0].resolve({status:200,ok:true,json:async()=>calendar});
  for(let i=0;i<12;i++)await Promise.resolve();
  if(calls.length!==2)throw Error('missing initial generation status');
  index=1;
 }
 calls[index].resolve({status:200,ok:true,json:async()=>first});
 for(let i=0;i<12;i++)await Promise.resolve();
 if(node('monthlyGenerate').disabled)throw Error('first generation button disabled');
 node('monthlyGenerate').events.click();
 node('monthlyGenerate').events.click();
 if(calls.length!==index+2||calls[index+1].options.method!=='POST')throw Error('first click did not submit exactly once');
 const c=calls[index+1];process.stdout.write(JSON.stringify({method:c.options.method,path:c.url,body:c.options.body}));
})().catch(()=>{process.stderr.write('first-click contract failed');process.exitCode=1;});
`
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "-e", script)
	cmd.Stdin = bytes.NewReader(input)
	raw, err := cmd.Output()
	if err != nil {
		t.Fatal("real initial response could not drive the page's first click", err)
	}
	var request struct {
		Method string `json:"method"`
		Path   string `json:"path"`
		Body   string `json:"body"`
	}
	if json.Unmarshal(raw, &request) != nil || request.Method != "POST" || request.Path != "/api/monthly-statement/generation" || request.Body != `{"month":"2026-09"}` {
		t.Fatal("page submitted an unexpected request")
	}
	return request.Method, request.Path, request.Body
}
