package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestRequestLengthCloudWatchParsingAndLocalPersistence(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	for _, tc := range []struct {
		value, state string
		want         *int64
	}{{"", "missing", nil}, {"null", "null", nil}, {"0", "valid", ptrTokenMetering(0)}, {`"1234"`, "valid", ptrTokenMetering(1234)}, {`"-"`, "sentinel", nil}, {"1.5", "invalid", nil}, {`"bad"`, "invalid", nil}} {
		t.Run(tc.state+tc.value, func(t *testing.T) {
			now := time.Now().Unix()
			message := `{"log_schema":2,"request_method":"POST","uri":"/v1/responses","status":"200","request_time":"0.1","oneapi_request_id":"synthetic","request_completion":"OK"`
			if tc.value != "" {
				message += `,"request_length":` + tc.value
			}
			message += "}"
			item, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNginx, EventID: "synthetic-event", TimestampMS: now * 1000, Message: message})
			if err != nil || item.RequestLengthState != tc.state || !sameTokenMeteringLength(item.RequestLength, tc.want) {
				t.Fatalf("%+v %v", item, err)
			}
			m, _, _ := tokenMeteringFixture(t)
			// Existing checkpoint tables used by the continuous writer, with no source calls.
			if err := m.nginxEvidenceDB.AutoMigrate(&CloudWatchNginxEvidenceCheckpoint{}); err != nil {
				t.Fatal(err)
			}
			if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), []cloudWatchStructuredEvidence{item}, now-1, now+1, "synthetic-batch"); err != nil {
				t.Fatal(err)
			}
			var row NginxRequestEvidence
			if err := m.nginxEvidenceDB.First(&row).Error; err != nil {
				t.Fatal(err)
			}
			if row.RequestLengthState != tc.state || !sameTokenMeteringLength(row.RequestLength, tc.want) {
				t.Fatalf("%+v", row)
			}
		})
	}

	t.Run("invalid_internal_snapshot_does_not_publish", func(t *testing.T) {
		now := time.Now().Unix()
		item, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNginx, EventID: "synthetic-invalid", TimestampMS: now * 1000, Message: `{"request_method":"POST","uri":"/v1/responses","status":"200","request_time":"0.1","oneapi_request_id":"synthetic","request_length":10}`})
		if err != nil {
			t.Fatal(err)
		}
		item.RequestLength = ptrTokenMetering(-1)
		m, _, _ := tokenMeteringFixture(t)
		if err := m.nginxEvidenceDB.AutoMigrate(&CloudWatchNginxEvidenceCheckpoint{}); err != nil {
			t.Fatal(err)
		}
		if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), []cloudWatchStructuredEvidence{item}, now-1, now+1, "invalid"); err == nil || !strings.Contains(err.Error(), "request length metadata invalid") {
			t.Fatalf("invalid length snapshot was not rejected at the persistence boundary: %v", err)
		}
		var count int64
		if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("%d %v", count, err)
		}
	})
	n, state := parseCloudWatchRequestLength(cloudWatchEvidenceInput{Fields: map[string]string{"request_length": "123"}})
	if state != "valid" || n == nil || *n != 123 {
		t.Fatal("Insights projection not parsed")
	}
}
func TestRequestLengthNodePayloadOldHashShapeAndIngest(t *testing.T) {
	legacy := nginxEvidenceEvent{EventID: "old"}
	got, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"event_id":"old","event_ms":0,"route":"","method":"","status":0,"upstream_status":0,"upstream_attempts":0,"request_ms":0,"upstream_ms":0,"upstream_present":false,"connect_ms":0,"header_ms":0,"bytes_sent":0,"completion":""}`
	if string(got) != want {
		t.Fatalf("legacy wire changed %s", got)
	}
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	in := validEvidenceBatch()
	in.Events[0].RequestLength = ptrTokenMetering(0)
	in.Events[0].RequestLengthState = "valid"
	in.Events[0].RequestLengthVersion = "request-length-v1"
	if w := postNginxEvidence(t, m, in); w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var row NginxRequestEvidence
	if err := m.nginxEvidenceDB.First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.RequestLength == nil || *row.RequestLength != 0 || row.RequestLengthState != "valid" {
		t.Fatalf("%+v", row)
	}
}
func TestRequestLengthMigrationBackupsAndHistoricalUnknown(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "evidence.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec("CREATE TABLE nginx_request_evidences (event_id TEXT PRIMARY KEY, event_ms INTEGER); INSERT INTO nginx_request_evidences VALUES ('old',1)"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backupNginxLengthMigration(path); err != nil {
		t.Fatal(err)
	}
	backups, err := filepath.Glob(filepath.Join(root, "nginx-evidence-migration-backups", preMigrationSnapshotPrefix+"*"))
	if err != nil || len(backups) == 0 {
		t.Fatalf("snapshot absent %v %v", backups, err)
	}
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	dbSQL, _ := db.DB()
	defer dbSQL.Close()
	// Add only the nullable fields here; older unrelated schemas are outside this migration.
	if err := db.Exec("ALTER TABLE nginx_request_evidences ADD COLUMN request_length INTEGER").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ALTER TABLE nginx_request_evidences ADD COLUMN request_length_state TEXT").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ALTER TABLE nginx_request_evidences ADD COLUMN request_length_version TEXT").Error; err != nil {
		t.Fatal(err)
	}
	var row struct {
		Length *int64
		State  *string
	}
	if err := db.Raw("SELECT request_length AS length, request_length_state AS state FROM nginx_request_evidences").Scan(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Length != nil || row.State != nil {
		t.Fatalf("historical null turned into zero %+v", row)
	}
	if err := backupNginxLengthMigration(path); err != nil {
		t.Fatal(err)
	}
}
func TestRequestLengthMigrationBackupFailureDoesNotAlterSource(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "evidence.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE nginx_request_evidences(event_id TEXT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nginx-evidence-migration-backups"), []byte("block"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := backupNginxLengthMigration(path); err == nil {
		t.Fatal("backup error ignored")
	}
	ro, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	var n int
	if err := ro.QueryRow("SELECT count(*) FROM pragma_table_info('nginx_request_evidences') WHERE name='request_length'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
}
func TestRequestLengthAllFixedNginxProjections(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		kind  cloudWatchFixedQueryKind
		value string
	}{
		{cwQueryWorkerRequestID, "synthetic-id"}, {cwQueryWorkerPath, "/v1/responses"}, {cwQueryWorkerShadowNginx, ""}, {cwQueryWorkerNginxContinuous, ""},
	} {
		_, query, _, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{Kind: tc.kind, From: now.Add(-time.Minute), To: now, Value: tc.value, Limit: 10})
		if err != nil || !strings.Contains(query, "bytes_sent, request_length, request_completion") {
			t.Fatalf("%s: %s %v", tc.kind, query, err)
		}
	}
}
