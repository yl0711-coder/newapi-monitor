package monitor

// Operator-only file conversion. This has no database, routes or background
// hooks. Hashes prevent accidental batch/target mixing; they are not digital
// signatures and cannot authenticate a human reviewer or attest source truth.
import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"
)

const (
	MonthlyCandidateBundleLimit = 32 << 20
	MonthlyReviewRequestLimit   = 64 << 10
)

// The operator must explicitly provide all fields after actual source/scope
// review. There are no default approvers, timestamps, confirmations or targets.
type MonthlyCandidateReview struct {
	Version                int    `json:"version"`
	GroupID                int64  `json:"group_id"`
	Month                  string `json:"month"`
	BundleSHA256           string `json:"bundle_sha256"`
	CandidateSHA256        string `json:"candidate_sha256"`
	Decision               string `json:"decision"`
	ReviewedBy             string `json:"reviewed_by"`
	ReviewedAt             int64  `json:"reviewed_at"`
	SourcesReviewed        bool   `json:"sources_reviewed"`
	MemberScopeReviewed    bool   `json:"member_scope_reviewed"`
	WalletBasisReviewed    bool   `json:"wallet_basis_reviewed"`
	ReconstructionReviewed bool   `json:"reconstruction_reviewed,omitempty"`
}

func monthlyReviewDecode(raw []byte, limit int, dst any) error {
	if len(raw) > limit {
		return errors.New("本机复核文件超过大小上限")
	}
	if err := monthlyUniqueJSONKeys(raw); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("本机复核文件包含多余内容")
	}
	return nil
}

// ReviewMonthlyCandidate recomputes the complete candidate bundle, binds the
// supplied approval to its exact bytes AND one candidate digest, and produces
// one version-2 (known statistics) or version-3 (nullable statistics) import
// file. It does not publish or bypass the importer's live
// local member/continuity checks. The caller must verify retained source files;
// the offline CLI does so before and after this pure calculation.
func ReviewMonthlyCandidate(inputRaw, contextRaw, bundleRaw, reviewRaw []byte, now time.Time) ([]byte, error) {
	var approval MonthlyCandidateReview
	if err := monthlyReviewDecode(reviewRaw, MonthlyReviewRequestLimit, &approval); err != nil {
		return nil, err
	}
	_, end, err := monthlyWindow(approval.Month, now)
	if err != nil || approval.Version != 1 || approval.GroupID <= 0 || len(approval.Month) != 7 || approval.Month != end.AddDate(0, -1, 0).Format("2006-01") || approval.Decision != "approve" || !approval.SourcesReviewed || !approval.MemberScopeReviewed || !approval.WalletBasisReviewed {
		return nil, errors.New("复核必须明确指定已结束月份、客户及全部确认项；不自动补选")
	}
	if approval.ReviewedAt < end.Unix() || approval.ReviewedAt > now.Unix() || approval.ReviewedBy == "" || strings.TrimSpace(approval.ReviewedBy) != approval.ReviewedBy || len(approval.ReviewedBy) > 128 || strings.IndexFunc(approval.ReviewedBy, unicode.IsControl) >= 0 {
		return nil, errors.New("必须提供有效的实际复核人和复核时间；不自动代填")
	}
	if !validMonthlyPublication(approval.BundleSHA256) || !validMonthlyPublication(approval.CandidateSHA256) || len(bundleRaw) > MonthlyCandidateBundleLimit || monthlyDigest(bundleRaw) != approval.BundleSHA256 {
		return nil, errors.New("复核记录与候选包文件指纹不一致")
	}
	var saved MonthlyPreparationBundle
	if err := monthlyReviewDecode(bundleRaw, MonthlyCandidateBundleLimit, &saved); err != nil {
		return nil, err
	}
	if saved.SourceIntegrity != "retained_files_sha256_verified_not_fact_attestation" || saved.Reconciliation.SourceIntegrity != saved.SourceIntegrity {
		return nil, errors.New("候选包缺少本机来源完整性核验记录")
	}
	rebuilt, err := PrepareMonthlyCandidateBundle(inputRaw, contextRaw, now)
	if err != nil {
		return nil, err
	}
	rebuilt.SourceIntegrity = saved.SourceIntegrity
	rebuilt.Reconciliation.SourceIntegrity = saved.SourceIntegrity
	expected, err := json.Marshal(rebuilt)
	if err != nil {
		return nil, err
	}
	actual, err := json.Marshal(saved)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(expected, actual) {
		return nil, errors.New("候选包与当前留存输入重算结果不一致，须重新准备并复核；不沿用旧批准")
	}
	for _, entry := range rebuilt.Entries {
		if entry.GroupID != approval.GroupID || entry.Month != approval.Month {
			continue
		}
		if entry.Status != "needs_review" || entry.Candidate == nil || len(entry.Issues) != 0 {
			return nil, errors.New("该客户月份仍有未核平项，不能用复核标记代替缺失证据")
		}
		if entry.CandidateSHA256 != approval.CandidateSHA256 {
			return nil, errors.New("复核记录与选定客户月份的候选指纹不一致")
		}
		candidate := entry.Candidate
		if len(candidate.ReconstructedUsers) > 0 && !approval.ReconstructionReviewed {
			return nil, errors.New("历史重建候选须单独确认重建依据；不能按原始月界快照直接批准")
		}
		e := MonthlyBillEvidence{
			Version: monthlyEvidenceVersion(candidate.Users), GroupID: candidate.GroupID, Month: candidate.Month,
			ReviewedBy: approval.ReviewedBy, ReviewedAt: approval.ReviewedAt,
			SourceReference: fmt.Sprintf("%s/bundle:%s/candidate:%s/review:%s", candidate.SourceReference, approval.BundleSHA256, approval.CandidateSHA256, monthlyDigest(reviewRaw)),
			Users:           candidate.Users,
		}
		if _, _, err := buildPublishedMonthlyBill(e, now); err != nil {
			return nil, err
		}
		// Compact deterministic JSON keeps the existing 2MiB import limit and
		// exact-repeat idempotency. No amount/statistic is edited during review.
		raw, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		raw = append(raw, '\n')
		if _, err := decodeMonthlyBillEvidence(raw); err != nil {
			return nil, err
		}
		return raw, nil
	}
	return nil, errors.New("复核记录指定的客户月份不在候选包中")
}
