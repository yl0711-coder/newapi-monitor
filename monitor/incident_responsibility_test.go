package monitor

import (
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/observability"
)

// 证据等级按 01.3 §87-90 的四档区分：既不自动升级，也不一律降级。
func TestEvidenceLevelFollowsContractFourTiers(t *testing.T) {
	cases := map[IncidentCorrelation]observability.EvidenceLevel{
		// exact 的两条合法路径（01.3 §87）。
		CorrelationDirectFact:                observability.EvidenceExact,
		CorrelationDeterministicReproduction: observability.EvidenceExact,
		// 多源高度相关是 correlated（§88）。
		CorrelationMultiSource: observability.EvidenceCorrelated,
		// 启发式与弱相关是 inferred（§89）。
		CorrelationLocalOnly:      observability.EvidenceInferred,
		CorrelationTimeWindowOnly: observability.EvidenceInferred,
		// 无观察点是 unavailable（§90）。
		CorrelationObservationMissing: observability.EvidenceUnavailable,
	}
	for c, want := range cases {
		if got := EvidenceLevelFor(c); got != want {
			t.Errorf("取得方式 %q → %q want %q", c, got, want)
		}
	}
}

// 多源关联不得自动升为 exact（关联 ≠ 直接事实）。
func TestMultiSourceCorrelationNotUpgradedToExact(t *testing.T) {
	if got := EvidenceLevelFor(CorrelationMultiSource); got == observability.EvidenceExact {
		t.Fatal("多源关联被升格为 exact")
	}
}

// 直接事实不得被一律降级——exact 必须有合法取得路径。
func TestDirectFactReachesExact(t *testing.T) {
	if got := EvidenceLevelFor(CorrelationDirectFact); got != observability.EvidenceExact {
		t.Fatalf("直接事实 → %q，exact 路径被堵死", got)
	}
	if got := EvidenceLevelFor(CorrelationDeterministicReproduction); got != observability.EvidenceExact {
		t.Fatalf("确定性复现 → %q want exact", got)
	}
}

// 未知取得方式不得放行成可用等级。
func TestUnknownCorrelationYieldsUnavailable(t *testing.T) {
	for _, c := range []IncidentCorrelation{"", "guessed", "high"} {
		if got := EvidenceLevelFor(c); got != observability.EvidenceUnavailable {
			t.Errorf("未知取得方式 %q → %q want unavailable", c, got)
		}
	}
}

// 证据等级为 inferred/unavailable 时一律无权归责，不论 fault_class 指向谁。
func TestResponsibilityCeilingByEvidenceLevel(t *testing.T) {
	blameFaults := []observability.FaultClass{
		observability.FaultUpstream5xx,
		observability.FaultTransportConnect,
		observability.FaultTransportTimeout,
		observability.FaultRateLimitCapacity,
		observability.FaultBillingAnomaly,
	}
	for _, fc := range blameFaults {
		for _, lvl := range []observability.EvidenceLevel{
			observability.EvidenceInferred, observability.EvidenceUnavailable,
		} {
			got := DecideResponsibility(ResponsibilityInput{
				FaultClass:                             fc,
				EvidenceLevel:                          lvl,
				UpstreamEvidenceLinked:                 true,
				UpstreamEvidenceIndicatesUpstreamCause: true,
			})
			if got != RespInsufficientEvidence {
				t.Errorf("fault=%q level=%q → %q want insufficient_evidence", fc, lvl, got)
			}
			if got.AssignsBlame() {
				t.Errorf("fault=%q level=%q 在证据不足时归责了", fc, lvl)
			}
		}
	}
}

// A｜429：默认不归责；只有可靠关联且证据指向上游限流才归 upstream。
func TestRateLimitDefaultsToInsufficientEvidence(t *testing.T) {
	// 仅我方日志。
	got := DecideResponsibility(ResponsibilityInput{
		FaultClass:    observability.FaultRateLimitCapacity,
		EvidenceLevel: observability.EvidenceCorrelated,
	})
	if got != RespInsufficientEvidence {
		t.Fatalf("仅我方 429 日志 → %q want insufficient_evidence", got)
	}
}

// 关联上了但证据并未指向上游成因时，仍不归上游。
func TestRateLimitLinkedButNotUpstreamCauseStaysInsufficient(t *testing.T) {
	got := DecideResponsibility(ResponsibilityInput{
		FaultClass:             observability.FaultRateLimitCapacity,
		EvidenceLevel:          observability.EvidenceCorrelated,
		UpstreamEvidenceLinked: true,
		// 关联到了上游这一跳，但那份证据没指向上游限流。
		UpstreamEvidenceIndicatesUpstreamCause: false,
	})
	if got == RespUpstream {
		t.Fatal("只是关联到上游这一跳就归责上游了")
	}
	if got != RespInsufficientEvidence {
		t.Fatalf("→ %q want insufficient_evidence", got)
	}
}

// 可靠关联 + 证据指向上游限流：才归 upstream。
func TestRateLimitUpstreamOnlyWithLinkedUpstreamCause(t *testing.T) {
	got := DecideResponsibility(ResponsibilityInput{
		FaultClass:                             observability.FaultRateLimitCapacity,
		EvidenceLevel:                          observability.EvidenceCorrelated,
		UpstreamEvidenceLinked:                 true,
		UpstreamEvidenceIndicatesUpstreamCause: true,
	})
	if got != RespUpstream {
		t.Fatalf("→ %q want upstream", got)
	}
}

// B｜空输出：仅有 token 字段与扣费记录时不足以定责。
func TestEmptyOutputWithOnlyTokenEvidenceStaysInsufficient(t *testing.T) {
	for _, lvl := range []observability.EvidenceLevel{
		observability.EvidenceExact, observability.EvidenceCorrelated,
	} {
		got := DecideResponsibility(ResponsibilityInput{
			FaultClass:    observability.FaultEmptyOrTruncatedOutput,
			EvidenceLevel: lvl,
			// 没有任何上游侧关联证据——只有 completion_tokens=0 与扣费。
		})
		if got == RespUpstream {
			t.Errorf("level=%q 仅凭 token 字段就归责上游", lvl)
		}
		if got != RespInsufficientEvidence {
			t.Errorf("level=%q → %q want insufficient_evidence", lvl, got)
		}
	}
}

// B｜空输出：证据补齐后不禁止定责。
func TestEmptyOutputBlamesUpstreamOnceCauseConfirmed(t *testing.T) {
	got := DecideResponsibility(ResponsibilityInput{
		FaultClass:                             observability.FaultEmptyOrTruncatedOutput,
		EvidenceLevel:                          observability.EvidenceExact,
		UpstreamEvidenceLinked:                 true,
		UpstreamEvidenceIndicatesUpstreamCause: true,
	})
	if got != RespUpstream {
		t.Fatalf("根因已证实时 → %q want upstream（证据补齐后不应仍禁止定责）", got)
	}
}

// B｜空输出：关联上但证据未指向上游成因时，仍不定责。
func TestEmptyOutputLinkedWithoutCauseStaysInsufficient(t *testing.T) {
	got := DecideResponsibility(ResponsibilityInput{
		FaultClass:                             observability.FaultEmptyOrTruncatedOutput,
		EvidenceLevel:                          observability.EvidenceExact,
		UpstreamEvidenceLinked:                 true,
		UpstreamEvidenceIndicatesUpstreamCause: false,
	})
	if got != RespInsufficientEvidence {
		t.Fatalf("→ %q want insufficient_evidence", got)
	}
}

// B｜空输出：仅 token 证据时不支持 DEGRADED 之上的处置。
func TestEmptyOutputTokenOnlySupportsNoQuarantine(t *testing.T) {
	resp := DecideResponsibility(ResponsibilityInput{
		FaultClass:    observability.FaultEmptyOrTruncatedOutput,
		EvidenceLevel: observability.EvidenceCorrelated,
	})
	if SupportsQuarantineSuggestion(observability.FaultEmptyOrTruncatedOutput, resp, observability.EvidenceCorrelated) {
		t.Fatal("仅 token 证据的空输出支持了隔离建议")
	}
}

// C｜计费异常：仅凭零用量日志不归 platform。
func TestBillingAnomalyLogAloneDoesNotBlamePlatform(t *testing.T) {
	got := DecideResponsibility(ResponsibilityInput{
		FaultClass: observability.FaultBillingAnomaly,
		// 原始事件可核验（有 NewAPI 原文），但责任结论不因此确定。
		EvidenceLevel: observability.EvidenceExact,
	})
	if got == RespPlatform {
		t.Fatal("仅凭零用量日志就把计费异常归责平台")
	}
	if got != RespInsufficientEvidence {
		t.Fatalf("→ %q want insufficient_evidence", got)
	}
}

// C｜计费异常：根因证实在平台链路后可归 platform。
func TestBillingAnomalyBlamesPlatformOnceCauseConfirmed(t *testing.T) {
	got := DecideResponsibility(ResponsibilityInput{
		FaultClass:             observability.FaultBillingAnomaly,
		EvidenceLevel:          observability.EvidenceExact,
		PlatformCauseConfirmed: true,
	})
	if got != RespPlatform {
		t.Fatalf("平台根因已证实时 → %q want platform", got)
	}
}

// C｜计费异常：根因证实在上游（如 usage 缺失）时归 upstream。
func TestBillingAnomalyBlamesUpstreamWhenUsageMissingConfirmed(t *testing.T) {
	got := DecideResponsibility(ResponsibilityInput{
		FaultClass:                             observability.FaultBillingAnomaly,
		EvidenceLevel:                          observability.EvidenceExact,
		UpstreamEvidenceLinked:                 true,
		UpstreamEvidenceIndicatesUpstreamCause: true,
	})
	if got != RespUpstream {
		t.Fatalf("上游 usage 缺失已证实时 → %q want upstream", got)
	}
}

// 计费异常即使责任已定位、证据为 exact，也不产生隔离建议；
// 但这不等于「与渠道无关」——同一场景下其他分类仍可隔离。
func TestBillingAnomalyNoQuarantineYetChannelStillRelevant(t *testing.T) {
	resp := DecideResponsibility(ResponsibilityInput{
		FaultClass:                             observability.FaultBillingAnomaly,
		EvidenceLevel:                          observability.EvidenceExact,
		UpstreamEvidenceLinked:                 true,
		UpstreamEvidenceIndicatesUpstreamCause: true,
	})
	if resp != RespUpstream {
		t.Fatalf("前置条件失败: %q", resp)
	}
	if SupportsQuarantineSuggestion(observability.FaultBillingAnomaly, resp, observability.EvidenceExact) {
		t.Fatal("计费异常产生了隔离建议")
	}
	// 同样的责任与证据，upstream_5xx 可以隔离——
	// 说明拒绝是针对该分类，不是「计费异常与渠道无关」这种一般结论。
	if !SupportsQuarantineSuggestion(observability.FaultUpstream5xx, resp, observability.EvidenceExact) {
		t.Fatal("同等条件下 upstream_5xx 也被拒，拒绝理由被过度泛化")
	}
}

// 采集缺口恒为证据不足，不得归责上游。
func TestTelemetryGapAlwaysInsufficientEvidence(t *testing.T) {
	for _, lvl := range []observability.EvidenceLevel{
		observability.EvidenceExact, observability.EvidenceCorrelated,
		observability.EvidenceInferred, observability.EvidenceUnavailable,
	} {
		got := DecideResponsibility(ResponsibilityInput{
			FaultClass:                             observability.FaultTelemetryGap,
			EvidenceLevel:                          lvl,
			UpstreamEvidenceLinked:                 true,
			UpstreamEvidenceIndicatesUpstreamCause: true,
		})
		if got != RespInsufficientEvidence {
			t.Errorf("telemetry_gap level=%q → %q want insufficient_evidence", lvl, got)
		}
		if got == RespUpstream {
			t.Errorf("采集缺口被归责上游（level=%q）", lvl)
		}
	}
}

// unknown 与 telemetry_gap 不可互换。
func TestFaultUnknownNeverCollapsesIntoTelemetryGap(t *testing.T) {
	unk := DecideResponsibility(ResponsibilityInput{
		FaultClass:    observability.FaultUnknown,
		EvidenceLevel: observability.EvidenceCorrelated,
	})
	gap := DecideResponsibility(ResponsibilityInput{
		FaultClass:    observability.FaultTelemetryGap,
		EvidenceLevel: observability.EvidenceCorrelated,
	})
	if unk == gap {
		t.Fatalf("unknown 与 telemetry_gap 的责任归属相同: %q", unk)
	}
	if unk != RespUnknown {
		t.Fatalf("unknown → %q want unknown", unk)
	}
	if gap != RespInsufficientEvidence {
		t.Fatalf("telemetry_gap → %q want insufficient_evidence", gap)
	}
	// 两者都不指向责任方，但原因不同。
	if unk.AssignsBlame() || gap.AssignsBlame() {
		t.Fatal("两者都不应指向具体责任方")
	}
	if unk.DisplayName() == gap.DisplayName() {
		t.Fatal("两者页面文案相同，用户无法区分")
	}
}

// D｜无观察点：缺少证据时不擅自输出，但不是永久禁止。
func TestFaultClassNeedingExternalEvidenceNotProducedWithoutIt(t *testing.T) {
	external := []observability.FaultClass{
		observability.FaultProtocolInvalid,
		observability.FaultResponsesIncomplete,
		observability.FaultThinkingSignatureInvalid,
		observability.FaultToolCallInvalid,
	}
	for _, fc := range external {
		if FaultClassObservabilityOf(fc) != FaultNeedsExternalEvidence {
			t.Errorf("%q 观察能力状态错误: %q", fc, FaultClassObservabilityOf(fc))
		}
		// 没有外部证据：不输出。
		if CanProduceFaultClass(fc, false, false) {
			t.Errorf("%q 在缺少外部证据时被输出", fc)
		}
		// 接入 Eval 的脱敏校验结果后：可以输出。这证明不是永久禁止。
		if !CanProduceFaultClass(fc, true, false) {
			t.Errorf("%q 在有外部证据时仍被禁止，阻碍后续联调", fc)
		}
	}
}

// E｜客户端两类：没有经授权的客户端遥测就不产出；Eval 不能代替。
func TestClientFaultsRequireClientTelemetryNotEval(t *testing.T) {
	clientFaults := []observability.FaultClass{
		observability.FaultClientRetryLoop,
		observability.FaultClientCompaction,
	}
	for _, fc := range clientFaults {
		if FaultClassObservabilityOf(fc) != FaultNeedsClientTelemetry {
			t.Errorf("%q 观察能力状态错误: %q", fc, FaultClassObservabilityOf(fc))
		}
		// 无任何证据：不产出。
		if CanProduceFaultClass(fc, false, false) {
			t.Errorf("%q 在无客户端遥测时被产出", fc)
		}
		// 仅接入 Eval（有外部证据但无客户端遥测）：仍不产出。
		if CanProduceFaultClass(fc, true, false) {
			t.Errorf("%q 仅凭 Eval 就被产出，Eval 不能代替客户端遥测", fc)
		}
		// 有经授权的客户端遥测：可产出。
		if !CanProduceFaultClass(fc, false, true) {
			t.Errorf("%q 在有客户端遥测时仍被禁止", fc)
		}
	}
}

// correlated 不支持隔离建议（01.3 §88：最多支持 DEGRADED）。
// 这是本轮 review 指出的缺陷：原实现只拦 inferred/unavailable，correlated 被放行。
func TestCorrelatedEvidenceDoesNotSupportQuarantine(t *testing.T) {
	// 429 + upstream + correlated：review 给出的具体反例。
	resp := DecideResponsibility(ResponsibilityInput{
		FaultClass:                             observability.FaultRateLimitCapacity,
		EvidenceLevel:                          observability.EvidenceCorrelated,
		UpstreamEvidenceLinked:                 true,
		UpstreamEvidenceIndicatesUpstreamCause: true,
	})
	if resp != RespUpstream {
		t.Fatalf("前置条件失败: %q", resp)
	}
	if SupportsQuarantineSuggestion(observability.FaultRateLimitCapacity, resp, observability.EvidenceCorrelated) {
		t.Fatal("correlated 证据支持了隔离建议，与 01.3「最多支持 DEGRADED」冲突")
	}
	// 同一场景升到 exact 才放行。
	if !SupportsQuarantineSuggestion(observability.FaultRateLimitCapacity, resp, observability.EvidenceExact) {
		t.Fatal("exact 证据下应可支持隔离建议")
	}
}

// 空值与非法证据等级不得放行隔离建议。
func TestInvalidEvidenceLevelDoesNotSupportQuarantine(t *testing.T) {
	for _, lvl := range []observability.EvidenceLevel{"", "high", "medium", "strong", "EXACT"} {
		if SupportsQuarantineSuggestion(observability.FaultUpstream5xx, RespUpstream, lvl) {
			t.Errorf("非法证据等级 %q 放行了隔离建议", lvl)
		}
	}
}

// 观察能力缺失的两类在实际判定里走不同分支：一个等 Eval，一个等客户端遥测。
func TestObservabilityGapsResolveDifferently(t *testing.T) {
	// 接入 Eval 后：协议类可产出，客户端类仍不可产出。
	if !CanProduceFaultClass(observability.FaultProtocolInvalid, true, false) {
		t.Fatal("有 Eval 证据时 protocol_invalid 仍不可产出")
	}
	if CanProduceFaultClass(observability.FaultClientRetryLoop, true, false) {
		t.Fatal("仅 Eval 就让 client_retry_loop 可产出")
	}
	// 接入客户端遥测后：客户端类可产出，协议类仍需 Eval。
	if !CanProduceFaultClass(observability.FaultClientRetryLoop, false, true) {
		t.Fatal("有客户端遥测时 client_retry_loop 仍不可产出")
	}
	if CanProduceFaultClass(observability.FaultProtocolInvalid, false, true) {
		t.Fatal("仅客户端遥测就让 protocol_invalid 可产出")
	}
}

// 有观察点的分类不受上述门槛影响，无需任何外部证据即可产出。
func TestObservableFaultsProducibleWithoutExternalEvidence(t *testing.T) {
	for _, fc := range []observability.FaultClass{
		observability.FaultUpstream5xx,
		observability.FaultTransportTimeout,
		observability.FaultRateLimitCapacity,
		observability.FaultBillingAnomaly,
	} {
		if !CanProduceFaultClass(fc, false, false) {
			t.Errorf("%q 有观察点却不可产出", fc)
		}
	}
}

// semantic_quality 不用于自动隔离（01.3 明示）。
func TestSemanticQualityNeverQuarantines(t *testing.T) {
	if SupportsQuarantineSuggestion(observability.FaultSemanticQuality, RespUpstream, observability.EvidenceExact) {
		t.Fatal("semantic_quality 产生了隔离建议")
	}
}

// 责任未指向具体一方时不支持任何处置结论。
func TestNoDispositionWithoutBlame(t *testing.T) {
	for _, r := range []IncidentResponsibility{RespUnknown, RespInsufficientEvidence} {
		if SupportsQuarantineSuggestion(observability.FaultUpstream5xx, r, observability.EvidenceExact) {
			t.Errorf("责任为 %q 时支持了隔离建议", r)
		}
	}
}

// 缺失或非法的 evidence_level 不得给出确定责任。
// 已复现的缺陷：EvidenceLevel 为 ""/"high"/未知串时，只要上游两个标记为 true，
// 429、空输出、计费异常都返回 upstream。
func TestDecideResponsibilityRejectsInvalidEvidenceLevel(t *testing.T) {
	bad := []observability.EvidenceLevel{
		"",          // 缺失
		"high",      // API-evaluator 的测试置信度，不是 evidence level
		"medium",    //
		"low",       //
		"strong",    // 臆造值
		"EXACT",     // 大小写不符
		" exact",    // 带空格
		"correlate", // 拼写缺字
	}
	faults := []observability.FaultClass{
		observability.FaultRateLimitCapacity,
		observability.FaultEmptyOrTruncatedOutput,
		observability.FaultBillingAnomaly,
		observability.FaultUpstream5xx,
	}
	for _, lvl := range bad {
		for _, fc := range faults {
			in := ResponsibilityInput{
				FaultClass:    fc,
				EvidenceLevel: lvl,
				// 上游两个标记都为 true——缺陷正是在这种输入下放行。
				UpstreamEvidenceLinked:                 true,
				UpstreamEvidenceIndicatesUpstreamCause: true,
				PlatformCauseConfirmed:                 true,
			}
			got := DecideResponsibility(in)
			if got == RespUpstream || got == RespPlatform {
				t.Errorf("evidence_level=%q fault=%q → %q（非法等级给出了确定责任）", lvl, fc, got)
			}
			if got.AssignsBlame() {
				t.Errorf("evidence_level=%q fault=%q → %q 指向了责任方", lvl, fc, got)
			}
			if got != RespInsufficientEvidence {
				t.Errorf("evidence_level=%q fault=%q → %q want insufficient_evidence", lvl, fc, got)
			}
		}
	}
}

// 非法证据等级应进入契约错误处理，而不是静默降级。
func TestDecideResponsibilityCheckedReturnsContractError(t *testing.T) {
	for _, lvl := range []observability.EvidenceLevel{"", "high", "bogus"} {
		resp, err := DecideResponsibilityChecked(ResponsibilityInput{
			FaultClass:                             observability.FaultRateLimitCapacity,
			EvidenceLevel:                          lvl,
			UpstreamEvidenceLinked:                 true,
			UpstreamEvidenceIndicatesUpstreamCause: true,
		})
		if err == nil {
			t.Errorf("evidence_level=%q 未返回契约错误", lvl)
		}
		if resp != RespInsufficientEvidence {
			t.Errorf("evidence_level=%q resp=%q want insufficient_evidence", lvl, resp)
		}
	}
}

// 非法 fault_class 同样不得给出确定责任。
func TestDecideResponsibilityRejectsInvalidFaultClass(t *testing.T) {
	for _, fc := range []observability.FaultClass{"", "upstream_5xx_maybe", "UPSTREAM_5XX"} {
		resp, err := DecideResponsibilityChecked(ResponsibilityInput{
			FaultClass:                             fc,
			EvidenceLevel:                          observability.EvidenceExact,
			UpstreamEvidenceLinked:                 true,
			UpstreamEvidenceIndicatesUpstreamCause: true,
		})
		if err == nil {
			t.Errorf("fault_class=%q 未返回契约错误", fc)
		}
		if resp.AssignsBlame() {
			t.Errorf("fault_class=%q → %q 指向了责任方", fc, resp)
		}
	}
}

// 合法的 exact/correlated 仍照常继续检查定责条件——校验不得把正常路径一并堵死。
func TestDecideResponsibilityStillWorksForValidLevels(t *testing.T) {
	for _, lvl := range []observability.EvidenceLevel{
		observability.EvidenceExact, observability.EvidenceCorrelated,
	} {
		resp, err := DecideResponsibilityChecked(ResponsibilityInput{
			FaultClass:                             observability.FaultRateLimitCapacity,
			EvidenceLevel:                          lvl,
			UpstreamEvidenceLinked:                 true,
			UpstreamEvidenceIndicatesUpstreamCause: true,
		})
		if err != nil {
			t.Errorf("evidence_level=%q 合法却报错: %v", lvl, err)
		}
		if resp != RespUpstream {
			t.Errorf("evidence_level=%q → %q want upstream", lvl, resp)
		}
	}
	// inferred/unavailable 合法但证据不足：不报错，不归责。
	for _, lvl := range []observability.EvidenceLevel{
		observability.EvidenceInferred, observability.EvidenceUnavailable,
	} {
		resp, err := DecideResponsibilityChecked(ResponsibilityInput{
			FaultClass:                             observability.FaultRateLimitCapacity,
			EvidenceLevel:                          lvl,
			UpstreamEvidenceLinked:                 true,
			UpstreamEvidenceIndicatesUpstreamCause: true,
		})
		if err != nil {
			t.Errorf("evidence_level=%q 合法却报错: %v", lvl, err)
		}
		if resp != RespInsufficientEvidence {
			t.Errorf("evidence_level=%q → %q want insufficient_evidence", lvl, resp)
		}
	}
}
