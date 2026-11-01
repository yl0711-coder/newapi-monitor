import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

const workflow=readFileSync(new URL('../../.github/workflows/ci.yml',import.meta.url),'utf8');
const files=['preparation_test.go','automatic_preparation_files_test.go'].map(name=>
  readFileSync(new URL('../../internal/monthlydraft/'+name,import.meta.url),'utf8'));

function assertMonthlyCLIContract(yaml){
  const step=yaml.match(/^      - name: Test monthly statement preparation CLI with synthetic files\n([\s\S]*?)(?=^      (?:- |#)|^  [\w-]+:|$(?![\s\S]))/m)?.[1];
  assert.ok(step,'required monthly CLI step is missing');
  assert.match(step,/set -euo pipefail/);
  assert.match(step,/go build -trimpath -o "\$RUNNER_TEMP\/monthlybillstage" \.\/cmd\/monthlybillstage/);
  assert.match(step,/MONTHLY_STAGE_TEST_BINARY="\$RUNNER_TEMP\/monthlybillstage" \\\n\s+go test -race -count=1 -timeout 3m/);
  assert.doesNotMatch(step,/continue-on-error|\|\|\s*true|GITHUB_ENV|(?:^|\s)-short(?:\s|$)/);
  const selected=step.match(/-run '([^']+)' \.\/internal\/monthlydraft/)?.[1];
  assert.ok(selected,'actual monthly draft package must be tested');
  const pattern=new RegExp(selected);
  const required=[];
  for(const source of files){
    const functions=[...source.matchAll(/^func (Test\w+)\(t \*testing\.T\) \{/gm)];
    for(let i=0;i<functions.length;i++){
      const body=source.slice(functions[i].index,functions[i+1]?.index??source.length);
      if(body.includes('os.Getenv("MONTHLY_STAGE_TEST_BINARY")'))required.push(functions[i][1]);
    }
  }
  assert.equal(required.length,2,'review newly added conditional CLI tests before changing this contract');
  for(const name of required)assert.ok(pattern.test(name),'CLI test silently skipped by CI selection: '+name);
}

test('monthly CI builds the real command and enables both synthetic CLI round trips',()=>{
  assertMonthlyCLIContract(workflow);
});

test('monthly CI contract rejects accidental command-test skips and ignored failures',()=>{
  for(const change of [
    yaml=>yaml.replace('MONTHLY_STAGE_TEST_BINARY="$RUNNER_TEMP/monthlybillstage"','OTHER_VARIABLE="$RUNNER_TEMP/monthlybillstage"'),
    yaml=>yaml.replace('go build -trimpath -o "$RUNNER_TEMP/monthlybillstage" ./cmd/monthlybillstage','echo command-not-built'),
    yaml=>yaml.replace('^Test(PreparationCLI|AutomaticPreparationFilesCLIRoundTrip)$','^TestPreparationCLI$'),
    yaml=>yaml.replace("./internal/monthlydraft\n      #", "./internal/monthlydraft || true\n      #"),
  ])assert.throws(()=>assertMonthlyCLIContract(change(workflow)));
});
