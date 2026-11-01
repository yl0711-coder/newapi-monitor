package monthlydraft

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type capturedPreparationTestConnector struct{ driver driver.Driver }

func (c capturedPreparationTestConnector) Driver() driver.Driver { return c.driver }

func assertCapturedPreparationDBClosed(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	// An arbitrary connection error is NOT proof of closing: an unreachable
	// but leaked lazy handle would otherwise make this assertion falsely pass.
	if err := db.PingContext(ctx); err == nil || err.Error() != "sql: database is closed" {
		t.Fatal("expected a closed owned database handle", err)
	}
}

func (c capturedPreparationTestConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.driver.Open("")
}

// Only replace the network transport, not collection, normalization, authority,
// schema validation, dependency orchestration, CAS or customer publication.
func capturedPreparationTestOpener(f capturedHistoryFixture) func(*mysql.Config) (*sql.DB, error) {
	return func(*mysql.Config) (*sql.DB, error) {
		db := sql.OpenDB(capturedPreparationTestConnector{f.wallet.funding.source.Driver()})
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		return db, nil
	}
}

func capturedPreparationConfigFixture(t *testing.T, f capturedHistoryFixture) (CapturedPreparationConfig, monthlysource.MonthlyObservationFile) {
	t.Helper()
	_, catalogue := capturedHistoryCatalogueFixture(t, f)
	var seq int
	var name, path string
	if err := f.wallet.funding.capture.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	path, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	connection := []byte(`{"network":"tcp","address":"127.0.0.1:9","user":"synthetic_select_only","password":"never-a-real-secret","database":"synthetic"}`)
	connectionPath := filepath.Join(f.cfg.EvidenceRoot, "synthetic-source-connection.json")
	if err := os.WriteFile(connectionPath, connection, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := CapturedPreparationConfig{Version: 1, SourceIdentity: f.wallet.funding.sourceIdentity,
		Connection:  monthlysource.MonthlyObservationFile{Path: connectionPath, SHA256: generationHash(connection)},
		CapturePath: path, SpoolDirectory: f.spool.directory, History: f.sources.History, ScopeCatalogue: catalogue, RecoverTokenWindows: f.wallet.funding.tokenRecovery, PartitionUsage: f.wallet.funding.partitionUsage, RetainCompletedSeals: f.spool.retainCompleted}
	raw := materialPreparationJSON(t, cfg)
	configPath := filepath.Join(f.cfg.EvidenceRoot, "captured-preparation.json")
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return cfg, monthlysource.MonthlyObservationFile{Path: configPath, SHA256: generationHash(raw)}
}

func TestCapturedPreparationStartupCustomerJourney(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	addCapturedHistoryReconstruction(t, &f)
	testCapturedHistoryPublicationAdapter(t, f, true, true, true, true)
}

func TestCapturedPreparationStartupIsLazyAndOwnsOnlySeparateResources(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	cfg, ref := capturedPreparationConfigFixture(t, f)
	// Real MySQL connector pointed at a synthetic unreachable loopback endpoint:
	// installation cannot pass if it contacts a server at startup.
	if err := d.EnableCapturedSourcePreparation(t.Context(), "captured-scope", ref); err != nil {
		t.Fatal(err)
	}
	res := d.capturedPreparation
	if res == nil || !d.MonthlyCalendarEnabled() || d.runtime.preparation.dependencies == nil || d.runtime.Phase() != "disabled" || res.source.Stats().OpenConnections != 0 {
		t.Fatal("installation started source I/O or omitted a stage")
	}
	if err := d.EnableCapturedSourcePreparation(t.Context(), "captured-scope", ref); err == nil {
		t.Fatal("duplicate installation accepted")
	}
	if _, _, _, err := d.openCapturedPreparationStore(t.Context(), cfg.CapturePath); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("second capture owner acquired the same directory", err)
	}
	preparationCounts(t, f.f, 0, 0)
	closeDeploymentTest(t, d)
	assertCapturedPreparationDBClosed(t, res.source)
	assertCapturedPreparationDBClosed(t, res.capture)
	if err := f.wallet.funding.capture.Ping(); err != nil {
		t.Fatal("closed a borrowed handle", err)
	}
	root, lock, err := openCapturedOutput(filepath.Dir(cfg.CapturePath))
	if err != nil {
		t.Fatal("capture lock not released on close", err)
	}
	if err := errors.Join(root.Close(), lock.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestCapturedPreparationStartupRejectsUnsafeConfigurationBeforeSource(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	original, ref := capturedPreparationConfigFixture(t, f)
	for _, scenario := range []string{"wrong_pin", "unknown_field", "wrong_version", "wrong_source", "missing_capture", "uninitialized_capture", "shared_database", "public_capture", "alias_capture", "missing_history", "invalid_connection", "bad_spool"} {
		t.Run(scenario, func(t *testing.T) {
			cfg, selected := original, ref
			switch scenario {
			case "wrong_version":
				cfg.Version++
			case "wrong_source":
				cfg.SourceIdentity = generationHash([]byte("another server"))
			case "missing_capture":
				cfg.CapturePath = filepath.Join(filepath.Dir(cfg.CapturePath), "missing.sqlite")
			case "uninitialized_capture":
				cfg.CapturePath = filepath.Join(filepath.Dir(cfg.CapturePath), "empty.sqlite")
				empty := generationDB(t, cfg.CapturePath, false)
				if err := empty.Ping(); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(cfg.CapturePath, 0600); err != nil {
					t.Fatal(err)
				}
			case "shared_database":
				cfg.CapturePath = f.cfg.ReceiverPath
			case "public_capture":
				if err := os.Chmod(cfg.CapturePath, 0644); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(cfg.CapturePath, 0600) })
			case "alias_capture":
				cfg.CapturePath += ".alias"
				if err := os.Symlink(original.CapturePath, cfg.CapturePath); err != nil {
					t.Fatal(err)
				}
			case "missing_history":
				cfg.History.Path += ".absent"
			case "invalid_connection":
				cfg.Connection.SHA256 = generationHash([]byte("not the pinned credential"))
			case "bad_spool":
				cfg.SpoolDirectory += ".absent"
			}
			raw := materialPreparationJSON(t, cfg)
			if scenario == "unknown_field" {
				raw = append([]byte(`{"approve":true,`), raw[1:]...)
			}
			if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			selected.SHA256 = generationHash(raw)
			if scenario == "wrong_pin" {
				selected.SHA256 = generationHash([]byte("changed manifest"))
			}
			opened := 0
			err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", selected, func(*mysql.Config) (*sql.DB, error) {
				opened++
				return nil, errors.New("must not expose never-a-real-secret")
			})
			if err == nil || strings.Contains(err.Error(), "never-a-real-secret") || d.capturedPreparation != nil || d.MonthlyCalendarEnabled() {
				t.Fatal("unsafe configuration installed or leaked credentials", err)
			}
			if scenario != "bad_spool" && opened != 0 {
				t.Fatal("source connector opened before validating inputs", opened)
			}
			if _, err := os.Stat(cfg.CapturePath); scenario == "missing_capture" && !os.IsNotExist(err) {
				t.Fatal("missing capture database created implicitly")
			}
			preparationCounts(t, f.f, 0, 0)
		})
	}
}

func TestCapturedPreparationPartialInstallationCannotStartAndReleasesOwnedResources(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	cfg, ref := capturedPreparationConfigFixture(t, f)
	d.receiver.SetMaxOpenConns(1)
	if _, err := d.receiver.Exec(`PRAGMA query_only=ON`); err != nil {
		t.Fatal(err)
	}
	var source *sql.DB
	err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, func(cfg *mysql.Config) (*sql.DB, error) {
		var err error
		source, err = capturedPreparationTestOpener(f)(cfg)
		return source, err
	})
	if err == nil || !d.preparationInstallFailed || d.capturedPreparation != nil || source == nil {
		t.Fatal("partial installation not rejected", err)
	}
	assertCapturedPreparationDBClosed(t, source)
	if err := d.Start(t.Context()); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("partial configuration started a worker", err)
	}
	root, lock, err := openCapturedOutput(filepath.Dir(cfg.CapturePath))
	if err != nil {
		t.Fatal("failed configuration retained capture lock", err)
	}
	if err := errors.Join(root.Close(), lock.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestCapturedPreparationCloseTimeoutRetainsHandlesUntilCalendarDrains(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	cfg, ref := capturedPreparationConfigFixture(t, f)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	calendar := d.runtime.service.(*registrationCalendarService)
	calendar.read = func(ctx context.Context, _ []int64) ([]monthlysource.RegistrationTime, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return nil, ctx.Err()
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := d.ReadMonthlyCalendar(t.Context(), f.req.Owner.GroupID, f.req.Owner.AuthVersion)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("calendar did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	err := d.Close(ctx)
	cancel()
	res := d.capturedPreparation
	// Release before an assertion can abort: cleanup must not leave a blocked
	// test operation that masks the original lifecycle failure.
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) || d.closed || res.capture.Ping() != nil {
		t.Fatal("close raced an active calendar request", err)
	}
	if _, _, _, err := d.openCapturedPreparationStore(t.Context(), cfg.CapturePath); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("timed-out close prematurely released ownership", err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled request returned a calendar")
		}
	case <-time.After(time.Second):
		t.Fatal("calendar did not drain")
	}
	closeDeploymentTest(t, d)
	assertCapturedPreparationDBClosed(t, res.capture)
}

func TestCapturedPreparationConnectionPolicy(t *testing.T) {
	base := capturedPreparationConnection{Network: "tcp", Address: "127.0.0.1:3306", User: "synthetic_reader", Password: "synthetic-secret", Database: "synthetic"}
	for _, scenario := range []string{"loopback", "verified_tls", "unix", "root", "unverified_remote", "unknown_field", "multiple_json", "bad_port"} {
		t.Run(scenario, func(t *testing.T) {
			c := base
			valid := scenario == "loopback" || scenario == "verified_tls" || scenario == "unix"
			switch scenario {
			case "verified_tls":
				c.Address, c.TLS = "database.example.invalid:3306", true
			case "unix":
				c.Network, c.Address = "unix", "/tmp/synthetic-mysql.sock"
			case "root":
				c.User = "ROOT"
			case "unverified_remote":
				c.Address = "database.example.invalid:3306"
			case "bad_port":
				c.Address = "127.0.0.1:0"
			}
			raw, _ := json.Marshal(c)
			if scenario == "unknown_field" {
				raw = append([]byte(`{"multiStatements":true,`), raw[1:]...)
			}
			if scenario == "multiple_json" {
				raw = append(raw, []byte(` {}`)...)
			}
			got, err := capturedConnectionConfig(raw)
			if (err == nil) != valid {
				t.Fatal("wrong connection policy result", err)
			}
			if valid && (got.MultiStatements || got.AllowAllFiles || got.AllowCleartextPasswords || got.MaxAllowedPacket != 2<<20 || got.Timeout != 2*time.Second || (c.TLS && got.TLSConfig != "true")) {
				t.Fatal("unsafe connector defaults")
			}
		})
	}
}
