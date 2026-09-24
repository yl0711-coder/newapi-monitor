package monitor

// usage_member_lifecycle.go owns the non-source member control plane. The
// authoritative transition is a single transaction in Monitor's main SQLite:
// active TrackedUser projection + durable revision/active control + append-only
// audit. The facts SQLite mirrors revisions asynchronously and is never used to
// authorize a member without rechecking this main-store control state.

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const usageMemberControlVersion = 1

var (
	errUsageMemberDifferentCompany = errors.New("用户已属于其他公司，请使用“纠正所属公司”")
	errUsageMemberNotActive        = errors.New("用户不在当前名单")
	errUsageMemberControlIntegrity = errors.New("用量成员控制数据不一致")
	errUsageMemberRequestConflict  = errors.New("幂等键已用于其他成员操作")
	errCustomerGroupHasMembers     = errors.New("公司仍有成员，请先逐个纠正所属公司")
	errCustomerGroupPortalEnabled  = errors.New("公司 Portal 仍已启用，请先停用 Portal 并确认旧会话失效")
	errPortalAuthorizationInvalid  = errors.New("portal authorization is no longer valid")

	usageMemberIdempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,95}$`)
	usageMemberRequestSequence       atomic.Uint64
	// Monitor is single-writer by deployment contract, but SQLite deferred
	// transactions can still let two concurrent HTTP handlers both observe a
	// missing request_id before either writes its audit. Member mutations are
	// rare administrator actions, so serializing this small control plane gives
	// deterministic revisions/idempotent replays without weakening facts/query
	// concurrency. The database UNIQUE key remains the durable final guard.
	usageMemberMutationMu sync.Mutex
)

// UsageMemberControl is the durable, per-user authorization/revision control
// in the main store. Rows survive ordinary removal and therefore make a later
// rejoin distinguishable from a first add.
type UsageMemberControl struct {
	UserID            int64 `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	Active            bool  `gorm:"index:idx_usage_member_control_active;column:active"`
	TrackedRevision   int64 `gorm:"not null;column:tracked_revision"`
	CurrentGroupID    int64 `gorm:"index;column:current_group_id"`
	FirstAddedAt      int64 `gorm:"column:first_added_at"`
	LastActivatedAt   int64 `gorm:"column:last_activated_at"`
	LastDeactivatedAt int64 `gorm:"column:last_deactivated_at"`
	UpdatedAt         int64 `gorm:"index;column:updated_at"`
}

// UsageMemberAudit is append-only. Before/after revisions are stored so an
// Idempotency-Key retry can return the original transition result without
// inspecting (possibly newer) current state.
type UsageMemberAudit struct {
	ID                    int64  `gorm:"primaryKey;column:id"`
	RequestID             string `gorm:"uniqueIndex;size:96;not null;column:request_id"`
	Action                string `gorm:"size:32;index;not null;column:action"`
	UserID                *int64 `gorm:"index;column:user_id"`
	BeforeGroupID         int64  `gorm:"column:before_group_id"`
	AfterGroupID          int64  `gorm:"column:after_group_id"`
	BeforeActive          bool   `gorm:"column:before_active"`
	AfterActive           bool   `gorm:"column:after_active"`
	BeforeTrackedRevision int64  `gorm:"column:before_tracked_revision"`
	AfterTrackedRevision  int64  `gorm:"column:after_tracked_revision"`
	ResultAddedAt         int64  `gorm:"column:result_added_at"`
	ResultUsername        string `gorm:"size:255;column:result_username"`
	ResultEmail           string `gorm:"size:255;column:result_email"`
	ResultNote            string `gorm:"size:200;column:result_note"`
	Actor                 string `gorm:"size:64;column:actor"`
	Reason                string `gorm:"size:500;column:reason"`
	CreatedAt             int64  `gorm:"index;column:created_at"`
}

func (UsageMemberAudit) BeforeUpdate(*gorm.DB) error {
	return errors.New("usage_member_audits is append-only")
}

func (UsageMemberAudit) BeforeDelete(*gorm.DB) error {
	return errors.New("usage_member_audits is append-only")
}

// UsageMemberControlMigration is a one-row proof that every TrackedUser that
// existed at additive-migration time received an active revision-1 control in
// the same main-store transaction.
type UsageMemberControlMigration struct {
	ID            uint   `gorm:"primaryKey;autoIncrement:false;column:id"`
	Version       int    `gorm:"not null;column:version"`
	ManifestHash  string `gorm:"size:64;not null;column:manifest_hash"`
	InitializedAt int64  `gorm:"column:initialized_at"`
}

func (UsageMemberControlMigration) TableName() string {
	return "usage_member_control_migration_state"
}

// AfterCreate is a last-line invariant for internal fixtures/importers that
// insert TrackedUser directly. Runtime reads never recreate a missing control;
// deleting a control after initialization therefore still fails closed.
func (u *TrackedUser) AfterCreate(tx *gorm.DB) error {
	if u == nil || u.UserID <= 0 {
		return nil
	}
	// 有过生命周期审计却缺失 control 不是“首次加入”，而是损坏。
	// 如果这里重建 rev=1，旧 PublishedMember rev=1 就可能复活。
	var lifecycleRows int64
	if err := tx.Model(&UsageMemberAudit{}).Where("user_id = ?", u.UserID).Count(&lifecycleRows).Error; err != nil {
		return err
	}
	if lifecycleRows > 0 {
		var existing int64
		if err := tx.Model(&UsageMemberControl{}).Where("user_id = ?", u.UserID).Count(&existing).Error; err != nil {
			return err
		}
		if existing == 0 {
			return fmt.Errorf("%w: user_id=%d lifecycle exists but control is missing", errUsageMemberControlIntegrity, u.UserID)
		}
	}
	now := time.Now().Unix()
	addedAt := u.AddedAt
	if addedAt <= 0 {
		addedAt = now
	}
	control := UsageMemberControl{
		UserID: u.UserID, Active: true, TrackedRevision: 1,
		CurrentGroupID: u.GroupID, FirstAddedAt: addedAt,
		LastActivatedAt: addedAt, UpdatedAt: now,
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&control).Error
}

func usageMemberControlManifest(controls []UsageMemberControl) string {
	ordered := append([]UsageMemberControl(nil), controls...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].UserID < ordered[j].UserID })
	h := sha256.New()
	for _, row := range ordered {
		_, _ = fmt.Fprintf(h, "%d:%t:%d:%d\n", row.UserID, row.Active, row.TrackedRevision, row.CurrentGroupID)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func migrateUsageMemberControls(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		for _, statement := range []string{
			`CREATE TRIGGER IF NOT EXISTS usage_member_audits_reject_update
			 BEFORE UPDATE ON usage_member_audits
			 BEGIN SELECT RAISE(ABORT, 'usage_member_audits is append-only'); END`,
			`CREATE TRIGGER IF NOT EXISTS usage_member_audits_reject_delete
			 BEFORE DELETE ON usage_member_audits
			 BEGIN SELECT RAISE(ABORT, 'usage_member_audits is append-only'); END`,
		} {
			if err := tx.Exec(statement).Error; err != nil {
				return err
			}
		}
		var migrated UsageMemberControlMigration
		err := tx.First(&migrated, 1).Error
		if err == nil {
			if migrated.Version != usageMemberControlVersion || migrated.InitializedAt <= 0 || migrated.ManifestHash == "" {
				return fmt.Errorf("%w: migration state invalid", errUsageMemberControlIntegrity)
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		var tracked []TrackedUser
		if err := tx.Order("user_id").Find(&tracked).Error; err != nil {
			return err
		}
		controls := make([]UsageMemberControl, 0, len(tracked))
		now := time.Now().Unix()
		for _, member := range tracked {
			var control UsageMemberControl
			err := tx.First(&control, "user_id = ?", member.UserID).Error
			switch {
			case errors.Is(err, gorm.ErrRecordNotFound):
				addedAt := member.AddedAt
				if addedAt <= 0 {
					addedAt = now
				}
				control = UsageMemberControl{
					UserID: member.UserID, Active: true, TrackedRevision: 1,
					CurrentGroupID: member.GroupID, FirstAddedAt: addedAt,
					LastActivatedAt: addedAt, UpdatedAt: now,
				}
				if err := tx.Create(&control).Error; err != nil {
					return err
				}
			case err != nil:
				return err
			case !control.Active || control.TrackedRevision < 1 || control.CurrentGroupID != member.GroupID:
				return fmt.Errorf("%w: user_id=%d projection/control mismatch during migration", errUsageMemberControlIntegrity, member.UserID)
			}
			controls = append(controls, control)
		}
		migrated = UsageMemberControlMigration{
			ID: 1, Version: usageMemberControlVersion,
			ManifestHash: usageMemberControlManifest(controls), InitializedAt: now,
		}
		return tx.Create(&migrated).Error
	})
}

type usageMemberControlSnapshot struct {
	Tracked   []TrackedUser
	Controls  map[int64]UsageMemberControl
	Manifest  string
	Migration UsageMemberControlMigration
}

// usageFactPublishedMemberCompatible is the only legacy exception for serving
// revisions: snapshots created before this schema have revision zero and may
// be served only while the authoritative member is still on its original,
// active revision one. Once remove/rejoin advances the control revision, that
// old row can never become visible again.
func usageFactPublishedMemberCompatible(published UsageFactPublishedMember, control UsageMemberControl) bool {
	if !control.Active || control.TrackedRevision < 1 {
		return false
	}
	if published.TrackedRevision == 0 {
		return control.TrackedRevision == 1
	}
	return published.TrackedRevision == control.TrackedRevision
}

func usageFactPublishedMemberCurrent(published UsageFactPublishedMember, control UsageMemberControl) bool {
	return control.Active && control.TrackedRevision >= 1 && published.TrackedRevision == control.TrackedRevision
}

// usageFactMemberDayHistoryReady is deliberately stricter than the current
// serving audit. Existing 7/30/90-day rows keep serving through ContentHash;
// they are not silently promoted to all-history coverage until an actual
// source query has supplied both independent hashes and version metadata.
func usageFactMemberDayHistoryReady(row UsageFactMemberDayState) bool {
	return row.Status == "complete" && row.SourceResultHash != "" && row.FactContentHash != "" &&
		row.ClassificationVersion == userTrafficClassificationVersion &&
		row.QuerySemanticsVersion == usageFactQuerySemanticsVersion && row.SourceEpoch != "" &&
		row.SourceCheckedAt > 0 && row.CompletedAt > 0
}

func usageMemberControlSnapshotsEqual(a, b usageMemberControlSnapshot) bool {
	return a.Manifest != "" && a.Manifest == b.Manifest
}

func (m *Monitor) usageFactBatchRevisionCurrent(ctx context.Context, batch []UsageFactMemberState) error {
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	for _, member := range batch {
		control, ok := snapshot.Controls[member.UserID]
		if !ok || !control.Active || control.TrackedRevision != member.TrackedRevision {
			return fmt.Errorf("%w: user_id=%d stale facts revision=%d", errUsageMemberControlIntegrity, member.UserID, member.TrackedRevision)
		}
	}
	return nil
}

func (m *Monitor) usageFactJobRevisionCurrent(ctx context.Context, job UsageFactJob) error {
	if job.UserID == nil || *job.UserID <= 0 || job.TrackedRevision < 1 {
		return fmt.Errorf("%w: facts job has no member revision", errUsageMemberControlIntegrity)
	}
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	control, ok := snapshot.Controls[*job.UserID]
	if !ok || !control.Active || control.TrackedRevision != job.TrackedRevision {
		return fmt.Errorf("%w: user_id=%d stale job revision=%d", errUsageMemberControlIntegrity, *job.UserID, job.TrackedRevision)
	}
	if m.usageFactsFullHistoryEnabled() {
		epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
		if epoch == "" || job.SourceEpoch != epoch {
			return fmt.Errorf("%w: user_id=%d stale source epoch", errUsageMemberControlIntegrity, *job.UserID)
		}
		var state UsageFactMemberState
		if err := m.usageFactsStore().WithContext(ctx).First(&state, "user_id = ?", *job.UserID).Error; err != nil {
			return fmt.Errorf("%w: user_id=%d facts state unavailable: %w", errUsageMemberControlIntegrity, *job.UserID, err)
		}
		if !state.Active || state.TrackedRevision != job.TrackedRevision || state.SourceEpoch != epoch ||
			state.ClassificationVersion != userTrafficClassificationVersion ||
			state.QuerySemanticsVersion != usageFactQuerySemanticsVersion {
			return fmt.Errorf("%w: user_id=%d stale facts job signature", errUsageMemberControlIntegrity, *job.UserID)
		}
	}
	return nil
}

// currentPublishedUsageMembers intersects facts publication with the current
// authoritative member control twice. The second manifest read closes the
// cross-database window where an in-flight Portal request could otherwise
// return a member after remove/correct-company committed in the main store.
type usageFactPublishedMembership struct {
	Members   []TrackedUser
	Active    int
	Published int
	Complete  bool
}

// currentPublishedUsageMembership returns one manifest-consistent view of the
// current authority and the compatible facts publication. Complete is scoped:
// when groupID is non-nil it only describes that company, otherwise it
// describes the entire tracked set. This lets a completed member become useful
// without allowing an incomplete company/global aggregate to masquerade as 0.
func (m *Monitor) currentPublishedUsageMembership(ctx context.Context, groupID *int64) (usageFactPublishedMembership, error) {
	for attempt := 0; attempt < 2; attempt++ {
		before, err := m.loadUsageMemberControlSnapshot(ctx)
		if err != nil {
			return usageFactPublishedMembership{}, err
		}
		var published []UsageFactPublishedMember
		if err := m.usageFactsStore().WithContext(ctx).Order("user_id").Find(&published).Error; err != nil {
			return usageFactPublishedMembership{}, err
		}
		after, err := m.loadUsageMemberControlSnapshot(ctx)
		if err != nil {
			return usageFactPublishedMembership{}, err
		}
		if !usageMemberControlSnapshotsEqual(before, after) {
			continue
		}
		publishedByID := make(map[int64]UsageFactPublishedMember, len(published))
		for _, row := range published {
			publishedByID[row.UserID] = row
		}
		out := make([]TrackedUser, 0, len(after.Tracked))
		active := 0
		for _, member := range after.Tracked {
			if groupID != nil && member.GroupID != *groupID {
				continue
			}
			active++
			publishedRow, ok := publishedByID[member.UserID]
			control := after.Controls[member.UserID]
			if !ok || !usageFactPublishedMemberCompatible(publishedRow, control) {
				continue
			}
			out = append(out, member)
		}
		return usageFactPublishedMembership{
			Members: out, Active: active, Published: len(out), Complete: len(out) == active,
		}, nil
	}
	return usageFactPublishedMembership{}, fmt.Errorf("%w: member manifest changed during facts read", errUsageMemberControlIntegrity)
}

func (m *Monitor) currentPublishedUsageMembers(ctx context.Context, groupID *int64) ([]TrackedUser, error) {
	membership, err := m.currentPublishedUsageMembership(ctx, groupID)
	return membership.Members, err
}

type portalUsageAuthorizationSnapshot struct {
	GroupID            int64
	AuthVersion        int64
	MemberManifest     string
	ServingFingerprint string
	// ServingGeneration fences not only membership changes, but also a
	// publication withdrawal or an atomically repaired facts day.  Without it,
	// a request that already read the old cache/result could still commit after
	// an audit had revoked the affected member while the main-store manifest and
	// Portal credentials remained unchanged.
	ServingGeneration    int64
	PublishedFingerprint string
	PublishedFrom        int64
	PublishedThrough     int64
}

func (s portalUsageAuthorizationSnapshot) equal(other portalUsageAuthorizationSnapshot) bool {
	return s.GroupID == other.GroupID && s.AuthVersion == other.AuthVersion &&
		s.MemberManifest != "" && s.MemberManifest == other.MemberManifest &&
		s.ServingFingerprint != "" && s.ServingFingerprint == other.ServingFingerprint &&
		s.ServingGeneration == other.ServingGeneration &&
		s.PublishedFingerprint == other.PublishedFingerprint &&
		s.PublishedFrom == other.PublishedFrom && s.PublishedThrough == other.PublishedThrough
}

func (m *Monitor) loadPortalUsageAuthorizationSnapshot(ctx context.Context, groupID int64) (portalUsageAuthorizationSnapshot, error) {
	var group CustomerGroup
	if err := m.storeDB.WithContext(ctx).First(&group, groupID).Error; err != nil {
		return portalUsageAuthorizationSnapshot{}, fmt.Errorf("%w: %w", errPortalAuthorizationInvalid, err)
	}
	if strings.TrimSpace(group.PortalEmail) == "" {
		return portalUsageAuthorizationSnapshot{}, errPortalAuthorizationInvalid
	}
	memberSnapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return portalUsageAuthorizationSnapshot{}, err
	}
	groupControls := make([]UsageMemberControl, 0)
	for _, control := range memberSnapshot.Controls {
		if control.CurrentGroupID == groupID {
			groupControls = append(groupControls, control)
		}
	}
	serving, err := m.portalTrackedMembersForUsageRead(ctx, groupID)
	if err != nil {
		return portalUsageAuthorizationSnapshot{}, err
	}
	var publication UsageFactSyncState
	if err := m.usageFactsStore().WithContext(ctx).First(&publication, 1).Error; err != nil {
		return portalUsageAuthorizationSnapshot{}, err
	}
	if m.usageFactsReadEnabled() && !m.usageFactsLocalSnapshotReadOnly() {
		servingSnapshot, snapshotErr := m.loadUsageFactServingReadSnapshot(ctx)
		if snapshotErr != nil {
			// Writers commit the durable generation before publishing its in-memory
			// cache namespace/bounds. Refuse that hand-off interval so a request
			// cannot pair new authorization with an old cache key or old range.
			return portalUsageAuthorizationSnapshot{}, snapshotErr
		}
		publication.ServingGeneration = servingSnapshot.ServingGeneration
		publication.PublishedFingerprint = servingSnapshot.PublishedFingerprint
		publication.PublishedRangeStart = servingSnapshot.From
		publication.PublishedThrough = servingSnapshot.Through
	}
	return portalUsageAuthorizationSnapshot{
		GroupID: groupID, AuthVersion: group.PortalAuthVer,
		MemberManifest:       usageMemberControlManifest(groupControls),
		ServingFingerprint:   portalMemberFingerprint(serving),
		ServingGeneration:    publication.ServingGeneration,
		PublishedFingerprint: publication.PublishedFingerprint,
		PublishedFrom:        publication.PublishedRangeStart,
		PublishedThrough:     publication.PublishedThrough,
	}, nil
}

// loadUsageMemberControlSnapshot validates both directions of the active
// projection. Missing controls, extra active controls, inactive controls or a
// group mismatch all fail closed after the migration state exists.
func (m *Monitor) loadUsageMemberControlSnapshot(ctx context.Context) (usageMemberControlSnapshot, error) {
	var out usageMemberControlSnapshot
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&out.Migration, 1).Error; err != nil {
			return fmt.Errorf("%w: migration state missing: %w", errUsageMemberControlIntegrity, err)
		}
		if out.Migration.Version != usageMemberControlVersion || out.Migration.InitializedAt <= 0 || out.Migration.ManifestHash == "" {
			return fmt.Errorf("%w: migration state invalid", errUsageMemberControlIntegrity)
		}
		if err := tx.Order("added_at, user_id").Find(&out.Tracked).Error; err != nil {
			return err
		}
		var controls []UsageMemberControl
		if err := tx.Where("active = ?", true).Order("user_id").Find(&controls).Error; err != nil {
			return err
		}
		if len(controls) != len(out.Tracked) {
			return fmt.Errorf("%w: active projection=%d controls=%d", errUsageMemberControlIntegrity, len(out.Tracked), len(controls))
		}
		trackedByID := make(map[int64]TrackedUser, len(out.Tracked))
		for _, member := range out.Tracked {
			trackedByID[member.UserID] = member
		}
		out.Controls = make(map[int64]UsageMemberControl, len(controls))
		for _, control := range controls {
			member, ok := trackedByID[control.UserID]
			if !ok || control.TrackedRevision < 1 || control.CurrentGroupID != member.GroupID {
				return fmt.Errorf("%w: user_id=%d active/group/revision mismatch", errUsageMemberControlIntegrity, control.UserID)
			}
			out.Controls[control.UserID] = control
		}
		for _, member := range out.Tracked {
			if _, ok := out.Controls[member.UserID]; !ok {
				return fmt.Errorf("%w: user_id=%d control missing", errUsageMemberControlIntegrity, member.UserID)
			}
		}
		out.Manifest = usageMemberControlManifest(controls)
		return nil
	})
	if err != nil {
		slog.Error("用量成员控制完整性校验失败，已拒绝发布/读取", "err", err)
	}
	return out, err
}

type usageMemberMutationMeta struct {
	RequestID string
	Actor     string
	Reason    string
}

type usageMemberMutationResult struct {
	User            TrackedUser
	Action          string
	TrackedRevision int64
	Active          bool
	GroupID         int64
	AddedAt         int64
	Replayed        bool
}

func generatedUsageMemberRequestID() string {
	b := make([]byte, 16)
	if _, err := cryptorand.Read(b); err == nil {
		return "member-" + hex.EncodeToString(b)
	}
	return "member-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(usageMemberRequestSequence.Add(1), 36)
}

func normalizeUsageMemberMutationMeta(meta usageMemberMutationMeta) (usageMemberMutationMeta, error) {
	meta.RequestID = strings.TrimSpace(meta.RequestID)
	if meta.RequestID == "" {
		meta.RequestID = generatedUsageMemberRequestID()
	}
	if !usageMemberIdempotencyKeyPattern.MatchString(meta.RequestID) {
		return meta, errors.New("幂等键必须为 1–96 位字母、数字或 ._:/- 字符")
	}
	meta.Actor = strings.TrimSpace(meta.Actor)
	if meta.Actor == "" {
		meta.Actor = "system"
	}
	if len(meta.Actor) > 64 {
		meta.Actor = meta.Actor[:64]
	}
	meta.Reason = strings.TrimSpace(meta.Reason)
	if len(meta.Reason) > 500 {
		meta.Reason = meta.Reason[:500]
	}
	return meta, nil
}

func usageMemberMutationMetaFromGin(c *gin.Context, bodyKey, reason string) (usageMemberMutationMeta, error) {
	keys := []string{strings.TrimSpace(bodyKey), strings.TrimSpace(c.GetHeader("Idempotency-Key")), strings.TrimSpace(c.GetHeader("X-Idempotency-Key"))}
	requestID := ""
	for _, key := range keys {
		if key == "" {
			continue
		}
		if requestID != "" && requestID != key {
			return usageMemberMutationMeta{}, errors.New("请求体与 Header 的幂等键不一致")
		}
		requestID = key
	}
	return normalizeUsageMemberMutationMeta(usageMemberMutationMeta{RequestID: requestID, Actor: c.GetString("uname"), Reason: reason})
}

func usageMemberAuditUserID(id int64) *int64 {
	v := id
	return &v
}

func mutationResultFromAudit(a UsageMemberAudit) usageMemberMutationResult {
	userID := int64(0)
	if a.UserID != nil {
		userID = *a.UserID
	}
	return usageMemberMutationResult{
		User: TrackedUser{
			UserID: userID, Username: a.ResultUsername, Email: a.ResultEmail,
			GroupID: a.AfterGroupID, Note: a.ResultNote, AddedAt: a.ResultAddedAt,
		},
		Action: a.Action, TrackedRevision: a.AfterTrackedRevision,
		Active: a.AfterActive, GroupID: a.AfterGroupID, AddedAt: a.ResultAddedAt, Replayed: true,
	}
}

func loadUsageMemberAudit(tx *gorm.DB, requestID string) (UsageMemberAudit, bool, error) {
	var audit UsageMemberAudit
	err := tx.Where("request_id = ?", requestID).First(&audit).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return audit, false, nil
	}
	return audit, err == nil, err
}

func auditMatchesUser(a UsageMemberAudit, userID int64, afterGroup int64, actions ...string) bool {
	if a.UserID == nil || *a.UserID != userID || a.AfterGroupID != afterGroup {
		return false
	}
	for _, action := range actions {
		if a.Action == action {
			return true
		}
	}
	return false
}

func currentUsageMemberControl(tx *gorm.DB, userID int64) (UsageMemberControl, bool, error) {
	var control UsageMemberControl
	err := tx.First(&control, "user_id = ?", userID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return control, false, nil
	}
	return control, err == nil, err
}

func (m *Monitor) addUsageMember(ctx context.Context, resolved TrackedUser, groupID int64, meta usageMemberMutationMeta) (usageMemberMutationResult, error) {
	meta, err := normalizeUsageMemberMutationMeta(meta)
	if err != nil {
		return usageMemberMutationResult{}, err
	}
	if resolved.UserID <= 0 || groupID < 0 {
		return usageMemberMutationResult{}, errors.New("用户或公司参数无效")
	}
	usageMemberMutationMu.Lock()
	defer usageMemberMutationMu.Unlock()
	var result usageMemberMutationResult
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		result, err = addUsageMemberTx(tx, resolved, groupID, meta)
		return err
	})
	return result, err
}

// addUsageMemberTx 是成员加入的唯一事务内实现。普通添加自己开事务；
// “创建公司并加入首个成员”在创建公司后调用它，使两步一起提交或一起回滚。
func addUsageMemberTx(tx *gorm.DB, resolved TrackedUser, groupID int64, meta usageMemberMutationMeta) (usageMemberMutationResult, error) {
	var result usageMemberMutationResult
	if existingAudit, found, err := loadUsageMemberAudit(tx, meta.RequestID); err != nil {
		return result, err
	} else if found {
		if !auditMatchesUser(existingAudit, resolved.UserID, groupID, "add", "rejoin") {
			return result, errUsageMemberRequestConflict
		}
		result = mutationResultFromAudit(existingAudit)
		return result, nil
	}
	if groupID > 0 {
		var count int64
		if err := tx.Model(&CustomerGroup{}).Where("id = ?", groupID).Count(&count).Error; err != nil {
			return result, err
		}
		if count == 0 {
			return result, errors.New("所选公司不存在")
		}
	}

	var current TrackedUser
	trackedErr := tx.First(&current, "user_id = ?", resolved.UserID).Error
	trackedExists := trackedErr == nil
	if trackedErr != nil && !errors.Is(trackedErr, gorm.ErrRecordNotFound) {
		return result, trackedErr
	}
	control, controlExists, err := currentUsageMemberControl(tx, resolved.UserID)
	if err != nil {
		return result, err
	}
	if !controlExists {
		var lifecycleRows int64
		if err := tx.Model(&UsageMemberAudit{}).Where("user_id = ?", resolved.UserID).Count(&lifecycleRows).Error; err != nil {
			return result, err
		}
		if lifecycleRows > 0 {
			return result, fmt.Errorf("%w: user_id=%d lifecycle exists but control is missing", errUsageMemberControlIntegrity, resolved.UserID)
		}
	}
	now := time.Now().Unix()
	action := "add"
	beforeActive, beforeGroup, beforeRevision := false, int64(0), int64(0)

	switch {
	case trackedExists:
		if !controlExists || !control.Active || control.TrackedRevision < 1 || control.CurrentGroupID != current.GroupID {
			return result, fmt.Errorf("%w: user_id=%d", errUsageMemberControlIntegrity, resolved.UserID)
		}
		if current.GroupID != groupID {
			return result, errUsageMemberDifferentCompany
		}
		beforeActive, beforeGroup, beforeRevision = true, current.GroupID, control.TrackedRevision
		// Idempotent same-company add refreshes only source-owned profile fields.
		if err := tx.Model(&TrackedUser{}).Where("user_id = ?", resolved.UserID).
			Updates(map[string]any{"username": resolved.Username, "email": resolved.Email}).Error; err != nil {
			return result, err
		}
		// Return the refreshed values while retaining AddedAt/Note/GroupID.
		if err := tx.First(&resolved, "user_id = ?", current.UserID).Error; err != nil {
			return result, err
		}
		result = usageMemberMutationResult{User: resolved, Action: action, TrackedRevision: control.TrackedRevision, Active: true, GroupID: groupID, AddedAt: current.AddedAt}

	case controlExists:
		if control.Active || control.TrackedRevision < 1 {
			return result, fmt.Errorf("%w: user_id=%d orphan active control", errUsageMemberControlIntegrity, resolved.UserID)
		}
		var count int64
		if err := tx.Model(&TrackedUser{}).Count(&count).Error; err != nil {
			return result, err
		}
		if count >= maxTrackedUsers {
			return result, fmt.Errorf("名单已达上限 %d 个", maxTrackedUsers)
		}
		action = "rejoin"
		beforeGroup, beforeRevision = control.CurrentGroupID, control.TrackedRevision
		control.Active = true
		control.TrackedRevision++
		control.CurrentGroupID = groupID
		control.LastActivatedAt = now
		control.UpdatedAt = now
		if control.FirstAddedAt <= 0 {
			control.FirstAddedAt = now
		}
		resolved.GroupID, resolved.AddedAt = groupID, control.FirstAddedAt
		if err := tx.Save(&control).Error; err != nil {
			return result, err
		}
		if err := tx.Create(&resolved).Error; err != nil {
			return result, err
		}
		result = usageMemberMutationResult{User: resolved, Action: action, TrackedRevision: control.TrackedRevision, Active: true, GroupID: groupID, AddedAt: resolved.AddedAt}

	default:
		var count int64
		if err := tx.Model(&TrackedUser{}).Count(&count).Error; err != nil {
			return result, err
		}
		if count >= maxTrackedUsers {
			return result, fmt.Errorf("名单已达上限 %d 个", maxTrackedUsers)
		}
		resolved.GroupID, resolved.AddedAt = groupID, now
		control = UsageMemberControl{
			UserID: resolved.UserID, Active: true, TrackedRevision: 1,
			CurrentGroupID: groupID, FirstAddedAt: now, LastActivatedAt: now, UpdatedAt: now,
		}
		if err := tx.Create(&control).Error; err != nil {
			return result, err
		}
		if err := tx.Create(&resolved).Error; err != nil {
			return result, err
		}
		result = usageMemberMutationResult{User: resolved, Action: action, TrackedRevision: 1, Active: true, GroupID: groupID, AddedAt: now}
	}

	audit := UsageMemberAudit{
		RequestID: meta.RequestID, Action: action, UserID: usageMemberAuditUserID(resolved.UserID),
		BeforeGroupID: beforeGroup, AfterGroupID: groupID,
		BeforeActive: beforeActive, AfterActive: true,
		BeforeTrackedRevision: beforeRevision, AfterTrackedRevision: result.TrackedRevision,
		ResultAddedAt: result.AddedAt, ResultUsername: result.User.Username,
		ResultEmail: result.User.Email, ResultNote: result.User.Note,
		Actor: meta.Actor, Reason: meta.Reason, CreatedAt: now,
	}
	if err := tx.Create(&audit).Error; err != nil {
		return result, err
	}
	return result, nil
}

// addCustomerWithFirstMember atomically reuses/creates a company and adds the
// resolved NewAPI user. A failed member transition rolls a newly-created empty
// company back in the same SQLite transaction.
func (m *Monitor) addCustomerWithFirstMember(ctx context.Context, company string, resolved TrackedUser, meta usageMemberMutationMeta) (CustomerGroup, usageMemberMutationResult, error) {
	meta, err := normalizeUsageMemberMutationMeta(meta)
	if err != nil {
		return CustomerGroup{}, usageMemberMutationResult{}, err
	}
	usageMemberMutationMu.Lock()
	defer usageMemberMutationMu.Unlock()
	var group CustomerGroup
	var result usageMemberMutationResult
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if existingAudit, found, err := loadUsageMemberAudit(tx, meta.RequestID); err != nil {
			return err
		} else if found {
			if existingAudit.UserID == nil || *existingAudit.UserID != resolved.UserID ||
				(existingAudit.Action != "add" && existingAudit.Action != "rejoin") {
				return errUsageMemberRequestConflict
			}
			if err := tx.First(&group, existingAudit.AfterGroupID).Error; err != nil {
				return errUsageMemberRequestConflict
			}
			if group.Name != company {
				return errUsageMemberRequestConflict
			}
			result = mutationResultFromAudit(existingAudit)
			return nil
		}

		findErr := tx.Where("name = ?", company).First(&group).Error
		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			var count int64
			if err := tx.Model(&CustomerGroup{}).Count(&count).Error; err != nil {
				return err
			}
			if count >= maxCustomerGroups {
				return fmt.Errorf("分组已达上限 %d 个", maxCustomerGroups)
			}
			group = CustomerGroup{Name: company, Stage: "active", CreatedAt: time.Now().Unix()}
			if err := tx.Create(&group).Error; err != nil {
				return err
			}
		} else if findErr != nil {
			return findErr
		}
		var err error
		result, err = addUsageMemberTx(tx, resolved, group.ID, meta)
		return err
	})
	return group, result, err
}

func (m *Monitor) removeUsageMember(ctx context.Context, userID int64, meta usageMemberMutationMeta) (usageMemberMutationResult, error) {
	meta, err := normalizeUsageMemberMutationMeta(meta)
	if err != nil {
		return usageMemberMutationResult{}, err
	}
	usageMemberMutationMu.Lock()
	defer usageMemberMutationMu.Unlock()
	var result usageMemberMutationResult
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		result, err = removeUsageMemberTx(tx, userID, meta)
		return err
	})
	return result, err
}

// removeUsageMemberTx is the one transactional implementation of a member
// removal. Standalone removal owns its transaction above; company dissolution
// calls this helper repeatedly inside the same transaction so controls,
// revisions, the tracked projection and every audit either all commit or all
// roll back together.
func removeUsageMemberTx(tx *gorm.DB, userID int64, meta usageMemberMutationMeta) (usageMemberMutationResult, error) {
	if existingAudit, found, err := loadUsageMemberAudit(tx, meta.RequestID); err != nil {
		return usageMemberMutationResult{}, err
	} else if found {
		if !auditMatchesUser(existingAudit, userID, existingAudit.AfterGroupID, "remove") {
			return usageMemberMutationResult{}, errUsageMemberRequestConflict
		}
		return mutationResultFromAudit(existingAudit), nil
	}
	var current TrackedUser
	if err := tx.First(&current, "user_id = ?", userID).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return usageMemberMutationResult{}, errUsageMemberNotActive
	} else if err != nil {
		return usageMemberMutationResult{}, err
	}
	control, found, err := currentUsageMemberControl(tx, userID)
	if err != nil {
		return usageMemberMutationResult{}, err
	}
	if !found || !control.Active || control.TrackedRevision < 1 || control.CurrentGroupID != current.GroupID {
		return usageMemberMutationResult{}, fmt.Errorf("%w: user_id=%d", errUsageMemberControlIntegrity, userID)
	}
	now := time.Now().Unix()
	beforeRevision := control.TrackedRevision
	control.Active = false
	control.TrackedRevision++
	control.LastDeactivatedAt = now
	control.UpdatedAt = now
	if err := tx.Save(&control).Error; err != nil {
		return usageMemberMutationResult{}, err
	}
	if err := tx.Delete(&TrackedUser{}, "user_id = ?", userID).Error; err != nil {
		return usageMemberMutationResult{}, err
	}
	result := usageMemberMutationResult{
		User: current, Action: "remove", TrackedRevision: control.TrackedRevision,
		Active: false, GroupID: current.GroupID, AddedAt: current.AddedAt,
	}
	audit := UsageMemberAudit{
		RequestID: meta.RequestID, Action: "remove", UserID: usageMemberAuditUserID(userID),
		BeforeGroupID: current.GroupID, AfterGroupID: current.GroupID,
		BeforeActive: true, AfterActive: false,
		BeforeTrackedRevision: beforeRevision, AfterTrackedRevision: control.TrackedRevision,
		ResultAddedAt: current.AddedAt, ResultUsername: current.Username,
		ResultEmail: current.Email, ResultNote: current.Note,
		Actor: meta.Actor, Reason: meta.Reason, CreatedAt: now,
	}
	if err := tx.Create(&audit).Error; err != nil {
		return usageMemberMutationResult{}, err
	}
	return result, nil
}

func (m *Monitor) correctUsageMemberCompany(ctx context.Context, userID, groupID int64, meta usageMemberMutationMeta) (usageMemberMutationResult, error) {
	meta, err := normalizeUsageMemberMutationMeta(meta)
	if err != nil {
		return usageMemberMutationResult{}, err
	}
	usageMemberMutationMu.Lock()
	defer usageMemberMutationMu.Unlock()
	var result usageMemberMutationResult
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if existingAudit, found, err := loadUsageMemberAudit(tx, meta.RequestID); err != nil {
			return err
		} else if found {
			if !auditMatchesUser(existingAudit, userID, groupID, "correct_company") {
				return errUsageMemberRequestConflict
			}
			result = mutationResultFromAudit(existingAudit)
			return nil
		}
		if groupID > 0 {
			var count int64
			if err := tx.Model(&CustomerGroup{}).Where("id = ?", groupID).Count(&count).Error; err != nil {
				return err
			}
			if count == 0 {
				return errors.New("公司不存在")
			}
		}
		var current TrackedUser
		if err := tx.First(&current, "user_id = ?", userID).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return errUsageMemberNotActive
		} else if err != nil {
			return err
		}
		control, found, err := currentUsageMemberControl(tx, userID)
		if err != nil {
			return err
		}
		if !found || !control.Active || control.TrackedRevision < 1 || control.CurrentGroupID != current.GroupID {
			return fmt.Errorf("%w: user_id=%d", errUsageMemberControlIntegrity, userID)
		}
		now := time.Now().Unix()
		if err := tx.Model(&TrackedUser{}).Where("user_id = ?", userID).Update("group_id", groupID).Error; err != nil {
			return err
		}
		beforeGroup := control.CurrentGroupID
		control.CurrentGroupID = groupID
		control.UpdatedAt = now
		if err := tx.Save(&control).Error; err != nil {
			return err
		}
		result = usageMemberMutationResult{User: current, Action: "correct_company", TrackedRevision: control.TrackedRevision, Active: true, GroupID: groupID, AddedAt: current.AddedAt}
		result.User.GroupID = groupID
		audit := UsageMemberAudit{
			RequestID: meta.RequestID, Action: "correct_company", UserID: usageMemberAuditUserID(userID),
			BeforeGroupID: beforeGroup, AfterGroupID: groupID,
			BeforeActive: true, AfterActive: true,
			BeforeTrackedRevision: control.TrackedRevision, AfterTrackedRevision: control.TrackedRevision,
			ResultAddedAt: current.AddedAt, ResultUsername: current.Username,
			ResultEmail: current.Email, ResultNote: current.Note,
			Actor: meta.Actor, Reason: meta.Reason, CreatedAt: now,
		}
		return tx.Create(&audit).Error
	})
	return result, err
}

func (m *Monitor) removeCustomerGroup(ctx context.Context, groupID int64, meta usageMemberMutationMeta) (bool, error) {
	meta, err := normalizeUsageMemberMutationMeta(meta)
	if err != nil {
		return false, err
	}
	usageMemberMutationMu.Lock()
	defer usageMemberMutationMu.Unlock()
	replayed := false
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if existingAudit, found, err := loadUsageMemberAudit(tx, meta.RequestID); err != nil {
			return err
		} else if found {
			if existingAudit.Action != "group_delete" || existingAudit.BeforeGroupID != groupID || existingAudit.UserID != nil {
				return errUsageMemberRequestConflict
			}
			replayed = true
			return nil
		}
		var group CustomerGroup
		if err := tx.First(&group, groupID).Error; err != nil {
			return err
		}
		var members int64
		if err := tx.Model(&TrackedUser{}).Where("group_id = ?", groupID).Count(&members).Error; err != nil {
			return err
		}
		if members > 0 {
			return errCustomerGroupHasMembers
		}
		if strings.TrimSpace(group.PortalEmail) != "" || group.PortalPwAdmin != "" || group.PortalPwUser != "" {
			return errCustomerGroupPortalEnabled
		}
		now := time.Now().Unix()
		if err := tx.Delete(&CustomerGroup{}, groupID).Error; err != nil {
			return err
		}
		audit := UsageMemberAudit{
			RequestID: meta.RequestID, Action: "group_delete",
			BeforeGroupID: groupID, AfterGroupID: 0,
			BeforeActive: true, AfterActive: false,
			Actor: meta.Actor, Reason: meta.Reason, CreatedAt: now,
		}
		return tx.Create(&audit).Error
	})
	return replayed, err
}

func usageMemberDissolveChildMeta(parent usageMemberMutationMeta, groupID, userID int64) usageMemberMutationMeta {
	// RequestID is capped at 96 bytes, so appending a user ID to an arbitrary
	// parent key is unsafe. A fixed-size digest gives every member a stable,
	// retry-safe audit key while Reason keeps the human-readable parent link.
	sum := sha256.Sum256([]byte(parent.RequestID + "\x00group_dissolve\x00" + strconv.FormatInt(groupID, 10) + "\x00" + strconv.FormatInt(userID, 10)))
	parentLink := "公司解散父请求 " + parent.RequestID
	reason := parentLink
	if detail := strings.TrimSpace(parent.Reason); detail != "" {
		reason += "；" + detail
	}
	if len(reason) > 500 {
		reason = reason[:500]
	}
	return usageMemberMutationMeta{
		RequestID: "dissolve-" + hex.EncodeToString(sum[:]),
		Actor:     parent.Actor,
		Reason:    reason,
	}
}

// dissolveCustomerGroup removes every tracked member and the company in one
// SQLite transaction. It does not touch NewAPI accounts, usage facts or request
// logs. Portal credentials are checked before the first member transition so a
// rejected dissolve cannot leave an empty or partially removed company.
func (m *Monitor) dissolveCustomerGroup(ctx context.Context, groupID int64, meta usageMemberMutationMeta) (bool, error) {
	meta, err := normalizeUsageMemberMutationMeta(meta)
	if err != nil {
		return false, err
	}
	usageMemberMutationMu.Lock()
	defer usageMemberMutationMu.Unlock()
	replayed := false
	err = m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if existingAudit, found, err := loadUsageMemberAudit(tx, meta.RequestID); err != nil {
			return err
		} else if found {
			if existingAudit.Action != "group_dissolve" || existingAudit.BeforeGroupID != groupID || existingAudit.UserID != nil {
				return errUsageMemberRequestConflict
			}
			replayed = true
			return nil
		}

		var group CustomerGroup
		if err := tx.First(&group, groupID).Error; err != nil {
			return err
		}
		if strings.TrimSpace(group.PortalEmail) != "" || group.PortalPwAdmin != "" || group.PortalPwUser != "" {
			return errCustomerGroupPortalEnabled
		}

		var members []TrackedUser
		if err := tx.Where("group_id = ?", groupID).Order("user_id").Find(&members).Error; err != nil {
			return err
		}
		for _, member := range members {
			childMeta := usageMemberDissolveChildMeta(meta, groupID, member.UserID)
			if _, err := removeUsageMemberTx(tx, member.UserID, childMeta); err != nil {
				return err
			}
		}
		if result := tx.Delete(&CustomerGroup{}, groupID); result.Error != nil {
			return result.Error
		} else if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		now := time.Now().Unix()
		return tx.Create(&UsageMemberAudit{
			RequestID: meta.RequestID, Action: "group_dissolve",
			BeforeGroupID: groupID, AfterGroupID: 0,
			BeforeActive: true, AfterActive: false,
			Actor: meta.Actor, Reason: meta.Reason, CreatedAt: now,
		}).Error
	})
	return replayed, err
}
