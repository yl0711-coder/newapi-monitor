//go:build unix

package monitor

// Private task workflow behind independently default-off maintenance/execution
// gates. No startup job or CLI runs tasks. The HTTP integration supplies an
// authenticated principal and a fixed server-owned task directory; clients
// never select a database or file path.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"golang.org/x/sys/unix"
	"gorm.io/gorm"
)

type financeRechargeTaskPlan struct {
	Version  int                          `json:"version"`
	Receiver string                       `json:"receiver"`
	Repair   *financeRechargeGuardPlan    `json:"repair,omitempty"`
	Recovery *financeRechargeRecoveryPlan `json:"recovery,omitempty"`
}

type financeRechargeTaskApproval struct {
	Confirmation string `json:"confirmation"`
	Actor        string `json:"actor"`
	ApprovedAt   int64  `json:"approved_at"`
	ExpiresAt    int64  `json:"expires_at"`
}

type financeRechargeTaskStatus struct {
	Confirmation     string                       `json:"confirmation"`
	Status           string                       `json:"status"`
	Committed        bool                         `json:"committed"`
	ReceiptAvailable bool                         `json:"receipt_available"`
	Receipt          *financeRechargeGuardReceipt `json:"receipt,omitempty"`
}

// Binding includes the actual open database path and inode, not a caller label.
// No credentials or whole live-file hash are used. Replacing the database means
// a new task/preflight is required even when the replacement has identical data.
func financeRechargeTaskReceiver(ctx context.Context, db *gorm.DB) (string, error) {
	var databases []struct{ Name, File string }
	if err := db.WithContext(ctx).Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
		return "", err
	}
	for _, database := range databases {
		if database.Name != "main" || database.File == "" {
			continue
		}
		path, err := filepath.Abs(database.File)
		if err != nil {
			return "", err
		}
		file, err := giftLocalOpenRegular(path, os.O_RDONLY)
		if err != nil {
			return "", err
		}
		var st unix.Stat_t
		err = unix.Fstat(int(file.Fd()), &st)
		file.Close()
		if err != nil {
			return "", err
		}
		return financeRechargeGuardHash(fmt.Sprintf("%s:%d:%d", path, st.Dev, st.Ino))
	}
	return "", errors.New("task requires a fixed file-backed SQLite receiver")
}

func (p financeRechargeTaskPlan) execution() (string, string, int64, FinanceRechargeRehearsalRequest, []ChannelFinanceVersion, error) {
	if p.Version != 1 || p.Receiver == "" || (p.Repair == nil) == (p.Recovery == nil) {
		return "", "", 0, FinanceRechargeRehearsalRequest{}, nil, errors.New("invalid task operation")
	}
	if p.Repair != nil {
		hash, err := financeRechargeGuardHash(*p.Repair)
		return hash, "guarded-recharge:" + hash, p.Repair.ExpiresAt, p.Repair.Request, p.Repair.Rows, err
	}
	hash, err := financeRechargeGuardHash(*p.Recovery)
	return hash, "guarded-recovery:" + hash, p.Recovery.ExpiresAt, p.Recovery.Original.Request, p.Recovery.Rows, err
}

func writeFinanceRechargeTaskJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > financeGiftLocalJSONLimit {
		return errors.New("task artifact exceeds size budget")
	}
	return giftLocalWriteNew(path, data)
}

func createFinanceRechargeTask(ctx context.Context, db *gorm.DB, dir string, plan financeRechargeTaskPlan) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	receiver, err := financeRechargeTaskReceiver(ctx, db)
	if err != nil {
		return "", err
	}
	plan.Version, plan.Receiver = 1, receiver
	_, _, expiry, request, rows, err := plan.execution()
	if err != nil || expiry <= time.Now().Unix() || len(rows) == 0 {
		return "", errors.New("task plan is invalid or expired")
	}
	if _, err := validateFinanceRechargeGuardRequest(request, time.Now().Unix()); err != nil {
		return "", err
	}
	digest, err := financeRechargeGuardHash(plan)
	if err != nil {
		return "", err
	}
	// Exclusive private directory: an existing job is never replaced or resumed.
	if err := os.Mkdir(dir, 0700); err != nil {
		return "", err
	}
	if err := giftLocalWriteNew(filepath.Join(dir, "job.lock"), nil); err != nil {
		return "", err
	}
	if err := writeFinanceRechargeTaskJSON(filepath.Join(dir, "plan.json"), plan); err != nil {
		return "", err
	}
	parent, err := os.Open(filepath.Dir(dir))
	if err != nil {
		return "", err
	}
	defer parent.Close()
	if err := parent.Sync(); err != nil {
		return "", err
	}
	return digest, nil
}

func loadFinanceRechargeTask(ctx context.Context, db *gorm.DB, dir, confirmation string) (financeRechargeTaskPlan, error) {
	var plan financeRechargeTaskPlan
	if _, err := giftLocalReadJSON(filepath.Join(dir, "plan.json"), &plan); err != nil {
		return plan, err
	}
	digest, err := financeRechargeGuardHash(plan)
	if err != nil || digest != confirmation {
		return plan, errors.New("task confirmation changed")
	}
	receiver, err := financeRechargeTaskReceiver(ctx, db)
	if err != nil || receiver != plan.Receiver {
		return plan, errors.New("task receiver changed")
	}
	_, _, _, request, rows, err := plan.execution()
	if err != nil {
		return plan, err
	}
	if len(rows) == 0 || len(rows) > financeRechargeCorrectionVersionLimit {
		return plan, errors.New("invalid task row budget")
	}
	if _, err := validateFinanceRechargeGuardRequest(request, time.Now().Unix()); err != nil {
		return plan, err
	}
	return plan, nil
}

func revalidateFinanceRechargeTask(ctx context.Context, db *gorm.DB, plan financeRechargeTaskPlan) error {
	if plan.Repair != nil {
		fresh, err := prepareFinanceRechargeGuardPlan(ctx, db, plan.Repair.Request, plan.Repair.CreatedAt)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(fresh, *plan.Repair) {
			return errors.New("repair preview changed; prepare a new task")
		}
	} else {
		fresh, err := prepareFinanceRechargeRecovery(ctx, db, plan.Recovery.Original, plan.Recovery.OriginalSHA256, plan.Recovery.CreatedAt)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(fresh, *plan.Recovery) {
			return errors.New("recovery preview changed; prepare a new task")
		}
	}
	return nil
}

// This is content confirmation/audit, not authentication. The HTTP boundary
// below checks the session's root role before invoking any task operation.
func approveFinanceRechargeTask(ctx context.Context, db *gorm.DB, dir, confirmation, actor string, now int64) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	lock, err := giftLocalLock(dir)
	if err != nil {
		return err
	}
	defer lock.Close()
	plan, err := loadFinanceRechargeTask(ctx, db, dir, confirmation)
	if err != nil {
		return err
	}
	_, _, expiry, _, _, err := plan.execution()
	if err != nil || actor == "" || len(actor) > 256 || now <= 0 || now >= expiry {
		return errors.New("invalid or expired task approval")
	}
	if _, err := os.Lstat(filepath.Join(dir, "stopped.json")); !os.IsNotExist(err) {
		return errors.New("task is stopped or stop state unreadable")
	}
	status, err := inspectFinanceRechargeTask(ctx, db, dir, confirmation, plan)
	if err != nil {
		return err
	}
	if status.Committed {
		return errors.New("task already committed; inspect existing receipt")
	}
	if err := revalidateFinanceRechargeTask(ctx, db, plan); err != nil {
		return err
	}
	var existing financeRechargeTaskApproval
	if _, err := giftLocalReadJSON(filepath.Join(dir, "approval.json"), &existing); err == nil {
		if existing.Confirmation != confirmation || existing.Actor != actor || existing.ExpiresAt != expiry {
			return errors.New("task approval conflict")
		}
		return nil // No extension of the original approval.
	} else if !os.IsNotExist(err) {
		return err
	}
	return writeFinanceRechargeTaskJSON(filepath.Join(dir, "approval.json"), financeRechargeTaskApproval{confirmation, actor, now, expiry})
}

// Inspect authoritative transaction markers even after approval expiry or a
// crash between SQLite commit and receipt-file persistence. Never retry writes
// just because the external receipt is missing, malformed or inaccessible.
func inspectFinanceRechargeTask(ctx context.Context, db *gorm.DB, dir, confirmation string, plan financeRechargeTaskPlan) (financeRechargeTaskStatus, error) {
	status := financeRechargeTaskStatus{Confirmation: confirmation, Status: "awaiting_approval"}
	hash, marker, _, request, expected, err := plan.execution()
	if err != nil || len(request.Corrections) != 1 || len(expected) == 0 {
		return status, errors.New("invalid persisted task")
	}
	var rows []ChannelFinanceVersion
	if err := db.WithContext(ctx).Where("domain=? AND updated_by=?", request.Corrections[0].Domain, marker).Order("version").Limit(financeRechargeCorrectionVersionLimit + 1).Find(&rows).Error; err != nil {
		return status, err
	}
	if len(rows) == 0 {
		if _, err := os.Lstat(filepath.Join(dir, "stopped.json")); err == nil {
			status.Status = "stopped"
			return status, nil
		} else if !os.IsNotExist(err) {
			return status, err
		}
		var approval financeRechargeTaskApproval
		if _, err := giftLocalReadJSON(filepath.Join(dir, "approval.json"), &approval); err == nil {
			_, _, expiry, _, _, _ := plan.execution()
			if approval.Confirmation != confirmation || approval.Actor == "" || approval.ExpiresAt != expiry {
				return status, errors.New("invalid stored approval")
			}
			status.Status = "awaiting_execution"
			if time.Now().Unix() >= expiry {
				status.Status = "expired"
			}
		} else if !os.IsNotExist(err) {
			return status, err
		}
		return status, nil
	}
	want := append([]ChannelFinanceVersion(nil), expected...)
	for i := range want {
		want[i].ID = 0
		want[i].UpdatedBy = marker
	}
	for i := range rows {
		rows[i].ID = 0
	}
	if !reflect.DeepEqual(rows, want) {
		return status, errors.New("partial or altered transaction evidence; manual inspection required")
	}
	status.Status, status.Committed = "committed", true
	var receipt financeRechargeGuardReceipt
	if _, err := giftLocalReadJSON(filepath.Join(dir, "receipt.json"), &receipt); err == nil && receipt.PlanSHA256 == hash && receipt.Status == "applied" && receipt.AddedVersions == len(want) {
		status.ReceiptAvailable = true
		status.Receipt = &receipt
	} else {
		status.Status = "committed_receipt_pending"
	}
	return status, nil
}

func runFinanceRechargeTask(ctx context.Context, db *gorm.DB, dir, confirmation, actor string, now int64) (financeRechargeTaskStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var empty financeRechargeTaskStatus
	lock, err := giftLocalLock(dir)
	if err != nil {
		return empty, err
	}
	defer lock.Close()
	plan, err := loadFinanceRechargeTask(ctx, db, dir, confirmation)
	if err != nil {
		return empty, err
	}
	status, err := inspectFinanceRechargeTask(ctx, db, dir, confirmation, plan)
	if err != nil || status.Committed {
		return status, err
	}
	if _, err := os.Lstat(filepath.Join(dir, "stopped.json")); !os.IsNotExist(err) {
		return status, errors.New("task stopped or stop state unreadable")
	}
	var approval financeRechargeTaskApproval
	if _, err := giftLocalReadJSON(filepath.Join(dir, "approval.json"), &approval); err != nil {
		return status, err
	}
	hash, _, expiry, _, _, _ := plan.execution()
	if approval.Confirmation != confirmation || approval.Actor != actor || actor == "" || approval.ApprovedAt <= 0 || now < approval.ApprovedAt || now >= expiry || approval.ExpiresAt != expiry {
		return status, errors.New("task execution lacks current matching approval")
	}
	// Durable intent is written before the money transaction. A partial write
	// blocks execution; an exact existing intent permits an explicit retry only.
	intent := filepath.Join(dir, "started.json")
	var prior financeRechargeTaskApproval
	if _, err := giftLocalReadJSON(intent, &prior); err == nil {
		if prior != approval {
			return status, errors.New("execution intent conflict")
		}
	} else if !os.IsNotExist(err) {
		return status, err
	} else if err := writeFinanceRechargeTaskJSON(intent, approval); err != nil {
		return status, err
	}
	var receipt financeRechargeGuardReceipt
	if plan.Repair != nil {
		receipt, err = executeFinanceRechargeGuardPlan(ctx, db, *plan.Repair, hash, now)
	} else {
		receipt, err = executeFinanceRechargeRecovery(ctx, db, *plan.Recovery, hash, now)
	}
	if err != nil {
		return status, err
	}
	if receipt.Status == "already_applied" {
		// Another task directory may have executed this exact kernel plan while
		// we waited for SQLite. Its no-op reply is not a new commit receipt.
		return inspectFinanceRechargeTask(ctx, db, dir, confirmation, plan)
	}
	status.Status, status.Committed = "committed", true
	status.Receipt = &receipt
	if err := writeFinanceRechargeTaskJSON(filepath.Join(dir, "receipt.json"), receipt); err != nil {
		// A missing external receipt is NOT a failed or rolled-back database write.
		status.Status = "committed_receipt_pending"
		return status, nil
	}
	status.ReceiptAvailable = true
	return status, nil
}

func stopFinanceRechargeTask(ctx context.Context, db *gorm.DB, dir, confirmation, actor string, now int64) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	lock, err := giftLocalLock(dir)
	if err != nil {
		return err
	}
	defer lock.Close()
	plan, err := loadFinanceRechargeTask(ctx, db, dir, confirmation)
	if err != nil {
		return err
	}
	status, err := inspectFinanceRechargeTask(ctx, db, dir, confirmation, plan)
	if err != nil {
		return err
	}
	if status.Committed {
		return errors.New("already committed; recovery requires a separate reviewed task")
	}
	if actor == "" || len(actor) > 256 || now <= 0 {
		return errors.New("invalid stop actor")
	}
	return writeFinanceRechargeTaskJSON(filepath.Join(dir, "stopped.json"), struct {
		Actor string
		At    int64
	}{actor, now})
}
