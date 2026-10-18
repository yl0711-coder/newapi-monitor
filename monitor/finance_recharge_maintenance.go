//go:build unix

package monitor

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Operator-provisioned input only. No uploads, user-selected scope, filesystem
// paths or database DSNs are accepted by the HTTP boundary.
type financeRechargeMaintenanceProposal struct {
	Version         int                              `json:"version"`
	Repair          *FinanceRechargeRehearsalRequest `json:"repair,omitempty"`
	RecoverOriginal *financeRechargeGuardPlan        `json:"recover_original,omitempty"`
	OriginalSHA256  string                           `json:"original_sha256,omitempty"`
}

type financeRechargePreparedTask struct {
	ProposalSHA256 string `json:"proposal_sha256"`
	TaskID         string `json:"task_id"`
}

func rechargeTaskDigestValid(value string) bool {
	b, err := hex.DecodeString(value)
	return err == nil && len(b) == 32 && strings.ToLower(value) == value
}

func validateFinanceRechargeMaintenance(s Settings) error {
	if !s.FinanceRechargeMaintenanceEnabled {
		return errors.New("recharge maintenance disabled")
	}
	if !s.FinanceEnabled || s.LocalAuthBypass || strings.TrimSpace(s.SessionSecret) == "" || !filepath.IsAbs(s.StorePath) || !rechargeTaskDigestValid(s.FinanceRechargeProposalSHA256) {
		return errors.New("maintenance requires finance, signed sessions, fixed store and proposal digest")
	}
	dir := s.FinanceRechargeTaskDir
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || filepath.Dir(dir) == dir || sameStorePath(dir, s.StorePath) || sameStorePath(dir, s.UsageFactsStorePath) {
		return errors.New("maintenance requires an independent absolute private task directory")
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("maintenance task directory must be private and not a symlink")
	}
	return nil
}

func loadFinanceRechargeMaintenanceProposal(cfg Settings) (financeRechargeMaintenanceProposal, error) {
	var proposal financeRechargeMaintenanceProposal
	b, err := giftLocalReadJSON(filepath.Join(cfg.FinanceRechargeTaskDir, "proposal.json"), &proposal)
	if err != nil || giftLocalDigest(b) != cfg.FinanceRechargeProposalSHA256 {
		return proposal, errors.New("configured proposal missing or changed")
	}
	if proposal.Version != 1 || (proposal.Repair == nil) == (proposal.RecoverOriginal == nil) {
		return proposal, errors.New("proposal must select exactly one operation")
	}
	if proposal.Repair != nil && proposal.OriginalSHA256 != "" {
		return proposal, errors.New("repair proposal contains unrelated recovery fields")
	}
	if proposal.RecoverOriginal != nil {
		hash, err := financeRechargeGuardHash(*proposal.RecoverOriginal)
		if err != nil || hash != proposal.OriginalSHA256 {
			return proposal, errors.New("original recovery evidence digest mismatch")
		}
	}
	return proposal, nil
}

// Registration changes no files and starts no work. Disabled remains absent;
// invalid optional maintenance configuration fails only the maintenance request,
// not normal Monitor startup or the four business modules.
func (m *Monitor) registerFinanceRechargeRoutes(r *gin.Engine) {
	if !m.cfg.FinanceRechargeMaintenanceEnabled {
		return
	}
	root := r.Group("/finance/recharge-maintenance", noStoreSensitive, m.requireRole(roleRoot))
	root.POST("/prepare", m.serveFinanceRechargePrepare)
	root.POST("/tasks/:id/:action", m.serveFinanceRechargeConfiguredTask)
}

func (m *Monitor) financeRechargeMaintenanceAvailable(c *gin.Context) bool {
	if !m.cfg.FinanceRechargeMaintenanceEnabled {
		c.JSON(http.StatusNotFound, gin.H{"error": "比例维护未启用"})
		return false
	}
	if m.storeDB == nil || m.shuttingDown.Load() || validateFinanceRechargeMaintenance(m.cfg) != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "比例维护配置或存储未就绪；普通功能不受此开关影响"})
		return false
	}
	return true
}

func (m *Monitor) prepareConfiguredFinanceRecharge(ctx context.Context) (financeRechargePreparedTask, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var out financeRechargePreparedTask
	proposal, err := loadFinanceRechargeMaintenanceProposal(m.cfg)
	if err != nil {
		return out, err
	}
	root := m.cfg.FinanceRechargeTaskDir
	// Initialization is explicit-request only. O_EXCL and no-follow flock avoid
	// two prepares creating independently approvable tasks for one proposal.
	if err := giftLocalWriteNew(filepath.Join(root, "job.lock"), nil); err != nil && !os.IsExist(err) {
		return out, err
	}
	lock, err := giftLocalLock(root)
	if err != nil {
		return out, err
	}
	defer lock.Close()
	pointer := filepath.Join(root, "prepared.json")
	if _, err := giftLocalReadJSON(pointer, &out); err == nil {
		if out.ProposalSHA256 != m.cfg.FinanceRechargeProposalSHA256 || !rechargeTaskDigestValid(out.TaskID) {
			return out, errors.New("prepared task binding changed")
		}
		_, err = loadFinanceRechargeTask(ctx, m.storeDB, filepath.Join(root, out.TaskID), out.TaskID)
		return out, err // No renewal, automatic retry or write after restart.
	} else if !os.IsNotExist(err) {
		return out, err
	}
	plan := financeRechargeTaskPlan{Version: 1}
	now := time.Now().Unix()
	if proposal.Repair != nil {
		prepared, err := prepareFinanceRechargeGuardPlan(ctx, m.storeDB, *proposal.Repair, now)
		if err != nil {
			return out, err
		}
		plan.Repair = &prepared
	} else {
		prepared, err := prepareFinanceRechargeRecovery(ctx, m.storeDB, *proposal.RecoverOriginal, proposal.OriginalSHA256, now)
		if err != nil {
			return out, err
		}
		plan.Recovery = &prepared
	}
	plan.Receiver, err = financeRechargeTaskReceiver(ctx, m.storeDB)
	if err != nil {
		return out, err
	}
	id, err := financeRechargeGuardHash(plan)
	if err != nil {
		return out, err
	}
	digest, err := createFinanceRechargeTask(ctx, m.storeDB, filepath.Join(root, id), plan)
	if err != nil {
		return out, err
	}
	if digest != id {
		return out, errors.New("task binding changed during preparation")
	}
	out = financeRechargePreparedTask{m.cfg.FinanceRechargeProposalSHA256, id}
	if err := writeFinanceRechargeTaskJSON(pointer, out); err != nil {
		return out, err
	}
	return out, nil
}

func (m *Monitor) serveFinanceRechargePrepare(c *gin.Context) {
	if !m.financeRechargeMaintenanceAvailable(c) {
		return
	}
	var request struct {
		Confirmation string `json:"confirmation"`
	}
	if c.Request.URL.RawQuery != "" || !giftAuthorizationJSON(c, &request) {
		if !c.Writer.Written() {
			c.JSON(400, gin.H{"error": "不接受查询参数"})
		}
		return
	}
	if request.Confirmation != m.cfg.FinanceRechargeProposalSHA256 {
		c.JSON(409, gin.H{"error": "提案摘要不一致"})
		return
	}
	prepared, err := m.prepareConfiguredFinanceRecharge(c.Request.Context())
	if err != nil {
		c.JSON(409, gin.H{"error": "提案或任务核验失败；未执行比例修复"})
		return
	}
	c.JSON(200, gin.H{"mode": "prepared_only", "task_id": prepared.TaskID, "confirmation": prepared.TaskID, "execution_enabled": m.cfg.FinanceRechargeExecutionEnabled, "execution_started": false})
}

func (m *Monitor) serveFinanceRechargeConfiguredTask(c *gin.Context) {
	if !m.financeRechargeMaintenanceAvailable(c) {
		return
	}
	id, action := c.Param("id"), c.Param("action")
	if !rechargeTaskDigestValid(id) {
		c.JSON(400, gin.H{"error": "任务编号无效"})
		return
	}
	// Execution stays independently disabled; status and stop remain available.
	if action == "execute" && !m.cfg.FinanceRechargeExecutionEnabled {
		c.JSON(403, gin.H{"error": "比例修复执行开关未启用"})
		return
	}
	var prepared financeRechargePreparedTask
	if _, err := giftLocalReadJSON(filepath.Join(m.cfg.FinanceRechargeTaskDir, "prepared.json"), &prepared); err != nil || prepared.TaskID != id || prepared.ProposalSHA256 != m.cfg.FinanceRechargeProposalSHA256 {
		c.JSON(409, gin.H{"error": "任务不属于当前固定提案"})
		return
	}
	// A changed proposal blocks writes, but never hides status or prevents stop.
	if action != "status" && action != "stop" {
		if _, err := loadFinanceRechargeMaintenanceProposal(m.cfg); err != nil {
			c.JSON(409, gin.H{"error": "固定提案已变更，拒绝继续操作"})
			return
		}
	}
	serveFinanceRechargeTask(c, m.storeDB, filepath.Join(m.cfg.FinanceRechargeTaskDir, id), action)
}
