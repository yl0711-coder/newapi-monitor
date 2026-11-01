package monthlydraft

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestCapturedPreparationPreInstallFailureCanBeCorrectedWithoutLeakingHandles(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	d := f.open(t)
	cfg, ref := capturedPreparationConfigFixture(t, f)
	validSpool := cfg.SpoolDirectory
	cfg.SpoolDirectory += ".absent"
	raw := materialPreparationJSON(t, cfg)
	if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ref.SHA256 = generationHash(raw)
	var source *sql.DB
	err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, func(cfg *mysql.Config) (*sql.DB, error) {
		var err error
		source, err = capturedPreparationTestOpener(f)(cfg)
		return source, err
	})
	if err == nil || source == nil || d.preparationInstallFailed || d.runtime.preparation != nil {
		t.Fatal("pre-install failure mutated runtime configuration", err)
	}
	assertCapturedPreparationDBClosed(t, source)
	root, lock, err := openCapturedOutput(filepath.Dir(cfg.CapturePath))
	if err != nil {
		t.Fatal("failed preparation retained directory ownership", err)
	}
	if err := errors.Join(root.Close(), lock.Close()); err != nil {
		t.Fatal(err)
	}
	cfg.SpoolDirectory = validSpool
	raw = materialPreparationJSON(t, cfg)
	if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ref.SHA256 = generationHash(raw)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal("corrected configuration cannot install", err)
	}
}
