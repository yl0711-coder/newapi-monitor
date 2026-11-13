// buildmanifest records the exact main-module source and embedded assets used by
// go build. It never walks runtime data, credentials or unrelated workspace files.
package main

import (
	"bytes"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
)

type file struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func main() {
	out := flag.String("out", "", "manifest output directory")
	binary := flag.String("binary", "", "optional compiled program to fingerprint")
	revision := flag.String("revision", "", "actual checkout revision (include dirty suffix for uncommitted inputs)")
	version := flag.String("version", "", "image version")
	expected := flag.String("expect", "", "expected source manifest SHA256; mismatch fails")
	flag.Parse()
	if err := run(*out, *binary, *revision, *version, *expected); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(out, binary, revision, version, expected string) error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	data, err := exec.Command("go", "list", "-deps", "-json", ".").Output()
	if err != nil {
		return fmt.Errorf("go list failed: %w", err)
	}
	// The stream consists of consecutive package objects, not a JSON array.
	reader := json.NewDecoder(bytes.NewReader(data))
	names := map[string]bool{"go.mod": true, "go.sum": true}
	for {
		var p struct {
			Dir                           string
			GoFiles, CgoFiles, EmbedFiles []string
			Module                        *struct{ Main bool }
		}
		if err := reader.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if p.Module == nil || !p.Module.Main {
			continue
		}
		for _, name := range append(append(p.GoFiles, p.CgoFiles...), p.EmbedFiles...) {
			rel, err := filepath.Rel(root, filepath.Join(p.Dir, name))
			if err != nil {
				return err
			}
			names[filepath.ToSlash(rel)] = true
		}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	files := make([]file, 0, len(ordered))
	for _, name := range ordered {
		content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil {
			return err
		}
		sum := sha256.Sum256(content)
		files = append(files, file{name, hex.EncodeToString(sum[:])})
	}
	manifest, err := json.Marshal(files)
	if err != nil {
		return err
	}
	manifest = append(manifest, '\n')
	sum := sha256.Sum256(manifest)
	digest := hex.EncodeToString(sum[:])
	if expected != "" && expected != digest {
		return fmt.Errorf("source manifest mismatch: expected %s actual %s", expected, digest)
	}
	if out != "" {
		if err := os.MkdirAll(out, 0755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(out, "source-manifest.json"), manifest, 0644); err != nil {
			return err
		}
		info := map[string]any{"revision": revision, "version": version, "source_manifest_sha256": digest}
		if binary != "" {
			b, err := os.ReadFile(binary)
			if err != nil {
				return err
			}
			hash := sha256.Sum256(b)
			build, err := buildinfo.ReadFile(binary)
			if err != nil {
				return err
			}
			info["binary_sha256"] = hex.EncodeToString(hash[:])
			info["binary_bytes"] = len(b)
			info["go_version"] = build.GoVersion
			info["build_settings"] = build.Settings
		}
		encoded, err := json.MarshalIndent(info, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(out, "build-info.json"), append(encoded, '\n'), 0644); err != nil {
			return err
		}
	}
	fmt.Println(digest)
	return nil
}
