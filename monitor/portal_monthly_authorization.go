package monitor

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const monthlyAuthorizationKey = "monthlyBillAuthorization"

type monthlyBillAuthorization struct {
	Group       CustomerGroup
	Members     []TrackedUser
	Fingerprint string
	Manifest    string
}

func (s monthlyBillAuthorization) equal(other monthlyBillAuthorization) bool {
	return s.Group.ID == other.Group.ID && s.Group.PortalAuthVer == other.Group.PortalAuthVer &&
		s.Group.PortalEmail == other.Group.PortalEmail && s.Manifest != "" && s.Manifest == other.Manifest &&
		s.Fingerprint != "" && s.Fingerprint == other.Fingerprint
}

// Current authorization is still authoritative. Only the live usage projection
// (classification, backfill, serving generation) is irrelevant to frozen bills.
// Current member/control revisions gate new publications and detect concurrent
// authorization changes. They do not replace a saved bill's frozen roster:
// verified historical bills remain owned by the original organization.
func loadMonthlyBillAuthorization(ctx context.Context, db *gorm.DB, gid int64) (monthlyBillAuthorization, error) {
	var s monthlyBillAuthorization
	if gid <= 0 {
		return s, errPortalAuthorizationInvalid
	}
	if err := db.WithContext(ctx).First(&s.Group, gid).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return s, errPortalAuthorizationInvalid
		}
		return s, err
	}
	m := &Monitor{storeDB: db}
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return s, err
	}
	controls := []UsageMemberControl{}
	for _, member := range snapshot.Tracked {
		if member.GroupID == gid {
			s.Members = append(s.Members, member)
			controls = append(controls, snapshot.Controls[member.UserID])
		}
	}
	s.Fingerprint = portalMemberFingerprint(s.Members)
	s.Manifest = usageMemberControlManifest(controls)
	return s, nil
}

func (m *Monitor) portalMonthlyAuthorizationGuard(next gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "private, no-store")
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		gid, version := c.GetInt64("portalGID"), c.GetInt64("portalAuthVer")
		for attempt := 0; attempt < 2; attempt++ {
			before, err := loadMonthlyBillAuthorization(ctx, m.storeDB, gid)
			if err != nil || before.Group.PortalAuthVer != version || strings.TrimSpace(before.Group.PortalEmail) == "" {
				if errors.Is(err, errPortalAuthorizationInvalid) || (err == nil && (before.Group.PortalAuthVer != version || strings.TrimSpace(before.Group.PortalEmail) == "")) {
					c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
				} else {
					c.JSON(http.StatusServiceUnavailable, gin.H{"error": "成员权限状态暂不可用，请稍后重试"})
				}
				return
			}
			c.Set(monthlyAuthorizationKey, before)
			original := c.Writer
			buffered := newPortalBufferedResponseWriter(original)
			func() {
				c.Writer = buffered
				defer func() { c.Writer = original }()
				next(c)
			}()
			if buffered.Status() < 200 || buffered.Status() >= 300 {
				buffered.commitTo(original)
				return
			}
			after, err := loadMonthlyBillAuthorization(ctx, m.storeDB, gid)
			if err != nil || after.Group.PortalAuthVer != version || strings.TrimSpace(after.Group.PortalEmail) == "" {
				if errors.Is(err, errPortalAuthorizationInvalid) || (err == nil && (after.Group.PortalAuthVer != version || strings.TrimSpace(after.Group.PortalEmail) == "")) {
					c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
				} else {
					c.JSON(http.StatusServiceUnavailable, gin.H{"error": "成员权限状态暂不可用，请稍后重试"})
				}
				return
			}
			if before.equal(after) {
				buffered.commitTo(original)
				return
			}
		}
		c.JSON(http.StatusConflict, gin.H{"error": "成员范围已变化，请重试"})
	}
}

func monthlyAuthorizedScope(c *gin.Context) (monthlyBillAuthorization, bool) {
	v, ok := c.Get(monthlyAuthorizationKey)
	s, valid := v.(monthlyBillAuthorization)
	if !ok || !valid || s.Group.ID != c.GetInt64("portalGID") || s.Manifest == "" || s.Fingerprint == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "账单成员权限状态暂不可用"})
		return s, false
	}
	return s, true
}
