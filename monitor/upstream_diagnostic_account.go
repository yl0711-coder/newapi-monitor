package monitor

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// A probe reads at most one usage page / one daily summary. It never publishes
// returned rows, advances a cursor or invokes sync* methods that rotate sessions.
func (m *Monitor) probeSavedUpstream(ctx context.Context, client *http.Client, row ChannelUpstreamAccount, report *upstreamDiagnosticReport) error {
	now := time.Now()
	to := now.Truncate(time.Hour).Unix()
	from := to - 60 // one closed minute for paginated interfaces
	dayTo := cstDayStart(now.Unix())
	dayFrom := dayTo - 24*3600
	pacer := newUpstreamUsageRequestPacer(2, 0)
	var balance func() error
	var usage func() error
	scope := "消费接口单页抽查"
	switch row.Provider {
	case upstreamProviderNewAPI:
		var cred newAPICredential
		if m.openUpstreamCredential(row, &cred) != nil || cred.AccessToken == "" || row.UserID <= 0 {
			return fmt.Errorf("凭据无效")
		}
		balance = func() error { _, _, err := syncNewAPIBalance(ctx, client, row, cred); return err }
		usage = func() error { _, err := fetchNewAPIUsagePage(ctx, client, row, cred, from, to, 1, pacer); return err }
	case upstreamProviderSub2API:
		var cred sub2APICredential
		if m.openUpstreamCredential(row, &cred) != nil {
			return fmt.Errorf("凭据无效")
		}
		if diagnosticSessionUnavailable(cred.AccessToken, cred.ExpiresAt, report) {
			return nil
		}
		balance = func() error { _, err := sub2APIProfile(ctx, client, row, cred); return err }
		usage = func() error {
			_, err := fetchSub2APIUsageWindow(ctx, client, row, cred, dayFrom, dayTo, pacer, row.UsageAdapter)
			return err
		}
		scope = "昨日消费汇总接口"
	case upstreamProviderTokenForce:
		var cred tokenForceCredential
		if m.openUpstreamCredential(row, &cred) != nil {
			return fmt.Errorf("凭据无效")
		}
		if diagnosticSessionUnavailable(cred.AccessToken, cred.ExpiresAt, report) {
			return nil
		}
		balance = func() error { _, err := tokenForceBalance(ctx, client, row, cred); return err }
		usage = func() error {
			_, err := fetchTokenForceUsagePage(ctx, client, row, cred, from, to, 0, pacer)
			return err
		}
		report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "金额口径", Status: "warning", Code: "currency_confirmation", Message: "接口检测不验证您填写的 CNY/USD 结算比例", Action: "按实际结算合同确认换算单位，不要默认认为人民币与美元 1:1。"})
	case upstreamProviderAICodeWith:
		var cred aiCodeWithCredential
		if m.openUpstreamCredential(row, &cred) != nil {
			return fmt.Errorf("凭据无效")
		}
		keys, err := aiCodeWithCredentialKeys(cred)
		if err != nil || len(keys) == 0 {
			return fmt.Errorf("凭据无效")
		}
		balance = func() error {
			_, _, err := syncAICodeWithBalance(ctx, client, row, aiCodeWithCredential{APIKey: keys[0]})
			return err
		}
		usage = func() error {
			if m.cfg.UpstreamAICodeWithRecordsEnabled {
				_, err := fetchAICodeWithRecordPage(ctx, client, row, keys[0], dayFrom, "", pacer)
				return err
			}
			_, err := fetchAICodeWithUsageWindow(ctx, client, row, keys[0], dayFrom, dayTo, pacer)
			return err
		}
		scope = "第一把 Key 的昨日消费接口"
		report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "检测范围", Status: "warning", Code: "first_key_only", Message: "为限制请求量，仅检测第一把 Key；其他 Key 未验证", Action: "逐 Key 状态请查看账户配置中的 Key 列表；单页成功不代表全量账单完整。"})
		mode := upstreamDiagnosticCheck{Name: "账单模式", Status: "ok", Code: "daily_mode", Message: "新同步轮次使用自然日账单，本次抽查昨日汇总", Action: "不将日合计强拆为小时；既有未完成轮次仍按原冻结模式续跑。"}
		if m.cfg.UpstreamAICodeWithRecordsEnabled {
			scope = "第一把 Key 的昨日记录首屏"
			mode.Code, mode.Message = "record_mode", "新同步轮次使用记录聚合，本次仅抽查昨日记录第一页"
			mode.Action = "小时账单须经分页补齐和日总额核对后发布；此检测不验证全日总额，也不写入账单。"
		}
		report.Checks = append(report.Checks, mode)
	default:
		return errDiagnosticShape
	}
	if err := balance(); err != nil {
		report.Checks = append(report.Checks, diagnosticFailure("余额与认证", err))
		report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: scope, Status: "skipped", Code: "balance_failed", Message: "余额或认证失败，未继续请求消费接口", Action: "先解决上一项，再重新检测。"})
		return nil
	}
	report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "余额与认证", Status: "ok", Code: "balance_readable", Message: "已成功读取并解析余额；未写入余额快照"})
	if err := usage(); err != nil {
		report.Checks = append(report.Checks, diagnosticFailure(scope, err))
		return nil
	}
	report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: scope, Status: "ok", Code: "usage_readable", Message: "接口及本次返回字段可解析，不代表全量账单已同步或完整", Action: "完整性与历史补齐进度请查看数据同步状态。"})
	return nil
}

func diagnosticSessionUnavailable(access string, expires int64, report *upstreamDiagnosticReport) bool {
	if access != "" && expires > time.Now().Add(30*time.Second).Unix() {
		return false
	}
	report.Checks = append(report.Checks, upstreamDiagnosticCheck{Name: "登录会话", Status: "warning", Code: "session_refresh_needed", Message: "当前 Access Token 已到期或即将到期，本次未尝试认证", Action: "先点“同步余额”让正常流程刷新并保存会话，再检测；若提示 invalid refresh token，请重新登录导入。"})
	return true
}
