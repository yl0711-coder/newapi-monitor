package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"gorm.io/gorm"
)

// UsageGenerationStatus is an explicit CUSTOMER allowlist. It does not contain
// task/source IDs, evidence hashes, paths, financial amounts or raw errors.
// Generation is separate from the existing reviewed statement/download route;
// even draft_ready must never advertise a published or downloadable bill here.
type UsageGenerationStatus struct {
	Month   string `json:"month"`
	State   string `json:"state"`
	Message string `json:"message"`
	taskID  string // internal lookup only; never included in customer JSON
}

type UsageGenerationProblem struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// PublicUsageGenerationProblem MUST be used instead of serializing err.Error()
// at a future customer HTTP boundary. Internal errors remain server-side only.
func PublicUsageGenerationProblem(err error) UsageGenerationProblem {
	switch {
	case err == nil:
		return UsageGenerationProblem{}
	case errors.Is(err, ErrGenerationRulesChanged):
		return UsageGenerationProblem{"not_prepared", "该月账单需重新准备，请稍后查看或联系客服。"}
	case errors.Is(err, ErrGenerationChanged), errors.Is(err, ErrGenerationSelectionConflict), errors.Is(err, monthlytasks.ErrStale):
		return UsageGenerationProblem{"state_changed", "登录权限或账单状态已变化，请刷新后重试。"}
	case errors.Is(err, context.Canceled):
		return UsageGenerationProblem{"request_cancelled", "请求已取消。"}
	case errors.Is(err, ErrGenerationInput), errors.Is(err, monthlytasks.ErrInvalid):
		return UsageGenerationProblem{"invalid_request", "请求无效，请选择已结束的月份后重试。"}
	case errors.Is(err, ErrGenerationSelectionMissing), errors.Is(err, monthlytasks.ErrNotFound):
		return UsageGenerationProblem{"not_prepared", "该月账单暂未准备好，请稍后查看。"}
	case errors.Is(err, ErrGenerationBusy), errors.Is(err, ErrGenerationCapacity), errors.Is(err, monthlytasks.ErrBusy), errors.Is(err, monthlytasks.ErrCapacity), errors.Is(err, monthlytasks.ErrCooldown):
		return UsageGenerationProblem{"temporarily_busy", "账单服务暂时繁忙，请稍后重试。"}
	case errors.Is(err, ErrGenerationEvidence), errors.Is(err, ErrGenerationIntegrity), errors.Is(err, monthlytasks.ErrCorrupt), errors.Is(err, monthlytasks.ErrExhausted):
		return UsageGenerationProblem{"needs_review", "该月账单需要进一步核验，请联系客服。"}
	default:
		return UsageGenerationProblem{"temporarily_unavailable", "账单服务暂不可用，请稍后查看。"}
	}
}

func usageGenerationTaskStatus(month string, task monthlytasks.Task) (UsageGenerationStatus, error) {
	status := UsageGenerationStatus{Month: month, taskID: task.ID}
	switch task.State {
	case monthlytasks.Queued:
		status.State, status.Message = "queued", "生成申请已排队。"
	case monthlytasks.Running:
		status.State, status.Message = "processing", "账单生成任务处理中。"
	case monthlytasks.DraftReady:
		status.State, status.Message = "pending_verification", "账单数据已生成，待核验后可查看。"
	case monthlytasks.Invalidated:
		status.State, status.Message = "needs_preparation", "该月账单需重新准备，请稍后查看或联系客服。"
	case monthlytasks.Failed:
		status.State, status.Message = "needs_review", "该月账单需要进一步核验，请联系客服。"
		if task.Attempts < 3 && (task.FailureCode == "source_unavailable" || task.FailureCode == "internal_failure") {
			status.State, status.Message = "temporarily_unavailable", "账单暂时未能生成，请稍后查看。"
		}
	default:
		return UsageGenerationStatus{}, ErrGenerationIntegrity
	}
	return status, nil
}

// ReadUsageMonthStatus only reads. It never runs generation, reads large input
// payloads, retries failed work or writes task state. gid/version must come from
// a verified server-side session. The returned error is PRIVATE (map it above).
// Publication state and published bills continue to use existing reviewed APIs.
func (g *Generation) ReadUsageMonthStatus(parent context.Context, authorization *gorm.DB, gid, version int64, month string) (UsageGenerationStatus, error) {
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	start, err := time.ParseInLocation("2006-01", month, zone)
	if !validSelectionScope(gid, month) || err != nil || start.AddDate(0, 1, 0).After(time.Now()) {
		return UsageGenerationStatus{}, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	owner, _, err := LoadUsageGenerationOwner(ctx, authorization, gid, version)
	if err != nil {
		return UsageGenerationStatus{}, err
	}
	selected, err := ReadGenerationSelection(ctx, g.receiver, gid, month)
	if err != nil {
		return UsageGenerationStatus{}, err
	}
	status := UsageGenerationStatus{Month: month, State: "not_prepared", Message: "该月账单暂未准备好，请稍后查看。"}
	if selected.Head != "" {
		if selected.Request == nil {
			status.State, status.Message = "needs_preparation", "该月账单需重新准备，请稍后查看或联系客服。"
		} else {
			if selected.Request.Owner != owner {
				return UsageGenerationStatus{}, ErrGenerationChanged
			}
			if !currentGenerationRules(*selected.Request) {
				status.State, status.Message = "needs_preparation", "该月账单需重新准备，请稍后查看或联系客服。"
			} else {
				task, err := g.tasks.Find(ctx, *selected.Request)
				if errors.Is(err, monthlytasks.ErrNotFound) {
					status.State, status.Message = "not_requested", "该月尚未提交生成申请。"
				} else if err != nil {
					return UsageGenerationStatus{}, err
				} else {
					status, err = usageGenerationTaskStatus(month, task)
					if err != nil {
						return UsageGenerationStatus{}, err
					}
				}
			}
		}
	}
	// Recheck after reading metadata; do not return another scope/version's state.
	currentOwner, _, err := LoadUsageGenerationOwner(ctx, authorization, gid, version)
	if err != nil {
		return UsageGenerationStatus{}, err
	}
	after, err := ReadGenerationSelection(ctx, g.receiver, gid, month)
	if err != nil {
		return UsageGenerationStatus{}, err
	}
	if after.Head != selected.Head || currentOwner != owner {
		return UsageGenerationStatus{}, ErrGenerationChanged
	}
	if err := ctx.Err(); err != nil {
		return UsageGenerationStatus{}, err
	}
	return status, nil
}
