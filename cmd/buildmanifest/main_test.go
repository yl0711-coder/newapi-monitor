package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestIncludesEmbeddedInputsAndRejectsTampering(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	for name, body := range map[string]string{
		"go.mod": "module fixture\n\ngo 1.26.10\n", "go.sum": "",
		"main.go":   "package main\nimport _ \"embed\"\n//go:embed page.html\nvar page string\nfunc main(){}\n",
		"page.html": "first", ".env": "SECRET=must-not-be-included", "runtime.db": "runtime",
	} {
		if err := os.WriteFile(name, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(root, "proof")
	if err := run(out, "", "revision-dirty", "v-test", ""); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(out, "source-manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(manifest), "page.html") || strings.Contains(string(manifest), ".env") || strings.Contains(string(manifest), "runtime.db") {
		t.Fatal(string(manifest))
	}
	var info struct {
		Digest string `json:"source_manifest_sha256"`
	}
	raw, err := os.ReadFile(filepath.Join(out, "build-info.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		t.Fatal(err)
	}
	if err := run("", "", "", "", info.Digest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("page.html", []byte("changed embedded page"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run("", "", "", "", info.Digest); err == nil || !strings.Contains(err.Error(), "source manifest mismatch") {
		t.Fatalf("tampered embed accepted: %v", err)
	}
}
