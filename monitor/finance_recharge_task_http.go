//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type financeRechargeTaskPricePoint struct {
	EffectiveAt  int64    `json:"effective_at"`
	WithinWindow bool     `json:"within_window"`
	Known        bool     `json:"known"`
	Paid         *float64 `json:"paid"`
	Credit       *float64 `json:"credit"`
}

func financeRechargeTaskPricePoints(plan financeRechargeTaskPlan) ([]financeRechargeTaskPricePoint, error) {
	_, _, _, request, rows, err := plan.execution()
	if err != nil {
		return nil, err
	}
	points := make([]financeRechargeTaskPricePoint, 0, len(rows))
	for _, row := range rows {
		var snapshot channelFinanceVersionSnapshot
		if err := json.Unmarshal([]byte(row.SnapshotJSON), &snapshot); err != nil {
			return nil, err
		}
		point := financeRechargeTaskPricePoint{EffectiveAt: row.EffectiveAt, WithinWindow: row.EffectiveAt >= request.Corrections[0].FromTs && row.EffectiveAt < request.Corrections[0].ToTs}
		if validChannelFinanceNumber(snapshot.UpstreamRechargePaid) && validChannelFinanceNumber(snapshot.UpstreamRechargeCredit) {
			point.Known = true
			point.Paid = &snapshot.UpstreamRechargePaid
			point.Credit = &snapshot.UpstreamRechargeCredit
		}
		points = append(points, point)
	}
	return points, nil
}

// HTTP boundary called only through the default-off maintenance integration.
// It uses existing session middleware and a fixed operator-configured task
// path, never a request-provided path/DB. Execution has a separate write gate.
func serveFinanceRechargeTask(c *gin.Context, db *gorm.DB, dir, action string) {
	c.Header("Cache-Control", "no-store")
	if c.GetInt("urole") < roleRoot || c.GetString("uname") == "" {
		c.JSON(http.StatusForbidden, gin.H{"error": "需要超级管理员身份"})
		return
	}
	if c.Request.Method != http.MethodPost || c.Request.URL.RawQuery != "" || db == nil || dir == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "任务请求无效"})
		return
	}
	var request struct {
		Confirmation string `json:"confirmation"`
	}
	if !giftAuthorizationJSON(c, &request) {
		return
	}
	if len(request.Confirmation) != 64 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "确认摘要无效"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()
	actor, now := c.GetString("uname"), time.Now().Unix()
	var result any
	var err error
	switch action {
	case "approve":
		err = approveFinanceRechargeTask(ctx, db, dir, request.Confirmation, actor, now)
		result = gin.H{"status": "approved", "execution_started": false}
	case "execute":
		result, err = runFinanceRechargeTask(ctx, db, dir, request.Confirmation, actor, now)
	case "stop":
		err = stopFinanceRechargeTask(ctx, db, dir, request.Confirmation, actor, now)
		result = gin.H{"status": "stopped"}
	case "preview", "status":
		var lockErr error
		lock, lockErr := giftLocalLock(dir)
		if lockErr != nil {
			err = lockErr
			break
		}
		defer lock.Close()
		var plan financeRechargeTaskPlan
		plan, err = loadFinanceRechargeTask(ctx, db, dir, request.Confirmation)
		if err != nil {
			break
		}
		if action == "preview" {
			if err = revalidateFinanceRechargeTask(ctx, db, plan); err != nil {
				break
			}
			// Only accounting scope and amounts, never raw historical snapshots or
			// arbitrary future configuration fields stored inside those snapshots.
			op, preview := "repair", FinanceRechargeRehearsalResult{}
			if plan.Repair != nil {
				preview = plan.Repair.Preview
			} else {
				op, preview = "recovery", plan.Recovery.Preview
			}
			_, _, expiry, scope, _, _ := plan.execution()
			correction := scope.Corrections[0]
			var prices []financeRechargeTaskPricePoint
			prices, err = financeRechargeTaskPricePoints(plan)
			if err != nil {
				break
			}
			result = gin.H{"mode": "review_only", "operation": op, "execution_started": false, "confirmation": request.Confirmation, "expires_at": expiry,
				"domain": correction.Domain, "from_ts": correction.FromTs, "to_ts": correction.ToTs, "amounts": preview.Domains, "price_versions": prices, "report_from": scope.FromDate, "report_to": scope.ToDate}
		} else {
			result, err = inspectFinanceRechargeTask(ctx, db, dir, request.Confirmation, plan)
		}
	default:
		c.JSON(http.StatusNotFound, gin.H{"error": "任务操作不存在"})
		return
	}
	if err != nil {
		// Do not leak filesystem paths, account credentials or database errors.
		c.JSON(http.StatusConflict, gin.H{"error": "任务校验未通过或状态暂不可读；请核对任务状态，勿重复创建任务"})
		return
	}
	c.JSON(http.StatusOK, result)
}
