package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrivateStateOwnershipAndRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "receiver")
	digest := strings.Repeat("a", 64)
	lock, err := prepareState(dir, digest)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if other, err := prepareState(dir, digest); err == nil {
		other.Close()
		t.Fatal("two receivers acquired same store")
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, err := prepareState(dir, digest)
	if err != nil {
		t.Fatal(err)
	}
	resumed.Close()
	if other, err := prepareState(dir, strings.Repeat("b", 64)); err == nil {
		other.Close()
		t.Fatal("changed identity adopted existing data")
	}
}

func TestStateCannotAdoptExistingDirectoryOrSymlink(t *testing.T) {
	digest := strings.Repeat("a", 64)
	root := t.TempDir()
	if lock, err := prepareState(root, digest); err == nil {
		lock.Close()
		t.Fatal("unmarked directory adopted")
	}
	link := filepath.Join(t.TempDir(), "linked")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if lock, err := prepareState(link, digest); err == nil {
		lock.Close()
		t.Fatal("symlink followed")
	}
}

func TestConfigIsStrictPrivateAndBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	for _, raw := range []string{`{"prod_dsn":"forbidden"}`, `{} {}`, strings.Repeat(" ", maxConfigBytes+1)} {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadConfig(path); err == nil {
			t.Fatal("unbounded/unknown configuration accepted")
		}
	}
	if err := os.WriteFile(path, []byte(`{"region":"us-west-2"}`), 0600); err != nil {
		t.Fatal(err)
	}
	c, digest, err := loadConfig(path)
	if err != nil || c.Region != "us-west-2" || len(digest) != 64 {
		t.Fatal("valid private configuration failed", err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadConfig(path); err == nil {
		t.Fatal("publicly readable credential file accepted")
	}
}
