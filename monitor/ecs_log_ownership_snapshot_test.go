package monitor

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestECSOwnershipOfflineSnapshotPreservesIdentity(t *testing.T) {
	for _, purpose := range []string{ecsLogCandidatePurpose, "production-v1"} {
		t.Run(purpose, func(t *testing.T) {
			owner := ownershipFixture(t)
			registerECSTestSource(t, owner, strings.Repeat("a", 32), "reject")
			if err := owner.storeDB.Model(&ECSLogOwnershipBinding{}).Where("id = 1").Update("purpose", purpose).Error; err != nil {
				t.Fatal(err)
			}
			if err := owner.storeDB.Model(&ECSLogOwnership{}).Where("1 = 1").Update("purpose", purpose).Error; err != nil {
				t.Fatal(err)
			}
			var binding ECSLogOwnershipBinding
			var before []ECSLogOwnership
			if err := owner.storeDB.First(&binding).Error; err != nil {
				t.Fatal(err)
			}
			if err := owner.storeDB.Find(&before).Error; err != nil {
				t.Fatal(err)
			}
			for _, bypass := range []bool{false, true} {
				local := &Monitor{storeDB: owner.storeDB, cfg: Settings{LocalSnapshotOnly: true, LocalAuthBypass: bypass, AlertsDisabled: true}}
				for i := 0; i < 2; i++ {
					if err := local.initECSLogOwnership(owner.storeDB); err != nil {
						t.Fatal(err)
					}
				}
				if ecsLogRuntimeEnabled(local.cfg) || !local.ecsOwnershipRequired() {
					t.Fatal("offline inspection altered runtime/ownership gates")
				}
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(http.MethodPost, "/ecs/register", strings.NewReader(`{}`))
				local.registerECSLogHTTP(c)
				if w.Code != http.StatusServiceUnavailable {
					t.Fatalf("offline receiver accepted registration: %d", w.Code)
				}
			}
			var afterBinding ECSLogOwnershipBinding
			var after []ECSLogOwnership
			if err := owner.storeDB.First(&afterBinding).Error; err != nil {
				t.Fatal(err)
			}
			if err := owner.storeDB.Find(&after).Error; err != nil {
				t.Fatal(err)
			}
			if binding != afterBinding || !reflect.DeepEqual(before, after) {
				t.Fatal("offline inspection mutated receiver identity")
			}
			live := &Monitor{cfg: Settings{AlertsDisabled: true}}
			if err := live.initECSLogOwnership(owner.storeDB); err == nil {
				t.Fatal("online downgrade became allowed")
			}
		})
	}
}

func TestECSOwnershipOfflineSnapshotRejectsActiveConfiguration(t *testing.T) {
	owner := ownershipFixture(t)
	for name, mutate := range map[string]func(*Settings){
		"ecs":        func(s *Settings) { s.ECSLogEnabled = true },
		"production": func(s *Settings) { s.ECSLogProductionEnabled = true },
		"ownership":  func(s *Settings) { s.ECSLogOwnershipEnabled = true },
		"archive":    func(s *Settings) { s.ECSArchiveEnabled = true },
		"source":     func(s *Settings) { s.ProdDSN = "nonempty" },
		"newapi":     func(s *Settings) { s.NewAPIBaseURL = "https://example.invalid" },
		"worker":     func(s *Settings) { s.SourceWorkerEnabled = true },
		"lease":      func(s *Settings) { s.SourceLeaseRequired = true },
		"upstream":   func(s *Settings) { s.UpstreamSyncEnabled = true },
		"nginx":      func(s *Settings) { s.NginxEnabled = true },
		"infra":      func(s *Settings) { s.InfraEnabled = true },
		"cloudwatch": func(s *Settings) { s.CloudWatchLogsEnabled = true },
		"alerts":     func(s *Settings) { s.AlertsDisabled = false },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := Settings{LocalSnapshotOnly: true, AlertsDisabled: true}
			mutate(&cfg)
			local := &Monitor{cfg: cfg}
			if err := local.initECSLogOwnership(owner.storeDB); err == nil {
				t.Fatal("active configuration accepted")
			}
		})
	}
}

func TestECSOwnershipOfflineSnapshotRejectsCorruptBinding(t *testing.T) {
	for _, kind := range []string{"version", "audience", "purpose", "missing", "extra", "partial"} {
		t.Run(kind, func(t *testing.T) {
			owner := ownershipFixture(t)
			var err error
			switch kind {
			case "version":
				err = owner.storeDB.Model(&ECSLogOwnershipBinding{}).Where("id = 1").Update("version", 99).Error
			case "audience":
				err = owner.storeDB.Model(&ECSLogOwnershipBinding{}).Where("id = 1").Update("audience", "").Error
			case "purpose":
				err = owner.storeDB.Model(&ECSLogOwnershipBinding{}).Where("id = 1").Update("purpose", "unknown").Error
			case "missing":
				err = owner.storeDB.Delete(&ECSLogOwnershipBinding{}, "id = 1").Error
			case "extra":
				err = owner.storeDB.Create(&ECSLogOwnershipBinding{ID: 2}).Error
			case "partial":
				err = owner.storeDB.Migrator().DropTable(&ECSLogOwnership{})
			}
			if err != nil {
				t.Fatal(err)
			}
			local := &Monitor{cfg: Settings{LocalSnapshotOnly: true, AlertsDisabled: true}}
			if err := local.initECSLogOwnership(owner.storeDB); err == nil {
				t.Fatal("corrupt identity accepted")
			}
		})
	}
}
