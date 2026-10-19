// financeawspackage combines reviewed LOCAL artifacts. No network or DB access.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	_ "time/tzdata" // Keep the offline command portable to minimal Linux images.

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const maxInputBytes = 16 << 20

type paths []string

func (p *paths) String() string     { return fmt.Sprint([]string(*p)) }
func (p *paths) Set(s string) error { *p = append(*p, s); return nil }

type receipt struct {
	Mode        string            `json:"mode"`
	Output      string            `json:"output"`
	SHA256      string            `json:"sha256"`
	InputSHA256 map[string]string `json:"input_sha256"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string, out, diagnostics io.Writer) error {
	f := flag.NewFlagSet("financeawspackage", flag.ContinueOnError)
	f.SetOutput(diagnostics)
	var cur paths
	f.Var(&cur, "cur", "reviewed local individual CUR artifact JSON; repeat for each month")
	services := f.String("ce-services", "", "saved CE SERVICE response")
	records := f.String("ce-record-types", "", "saved CE RECORD_TYPE response")
	daily := f.String("ce-daily", "", "saved CE daily response")
	from := f.String("ce-from", "", "first CE month YYYY-MM")
	to := f.String("ce-to-exclusive", "", "exclusive CE end month YYYY-MM")
	basis := f.String("accounting-basis", "", "explicit reviewed basis: aws_billing_month_v1")
	destination := f.String("out", "", "NEW output file; never overwrites or publishes")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *destination == "" || len(cur) < 1 || len(cur) > 36 {
		return fmt.Errorf("requires -out and 1 to 36 -cur files, no positional arguments")
	}
	hashes := map[string]string{}
	remaining := maxInputBytes
	read := func(path string) ([]byte, error) {
		if path == "" {
			return nil, fmt.Errorf("missing required input path")
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		if _, ok := hashes[absolute]; ok {
			return nil, fmt.Errorf("duplicate input path: %s", absolute)
		}
		data, err := readRegular(absolute, remaining)
		if err != nil {
			return nil, err
		}
		remaining -= len(data)
		h := sha256.Sum256(data)
		hashes[absolute] = hex.EncodeToString(h[:])
		return data, nil
	}
	in := monitor.FinanceAWSPackageInput{FromMonth: *from, ToMonthExclusive: *to, AccountingBasis: *basis}
	for _, path := range cur {
		b, err := read(path)
		if err != nil {
			return err
		}
		in.CURArtifacts = append(in.CURArtifacts, b)
	}
	var err error
	if in.Services, err = read(*services); err != nil {
		return err
	}
	if in.RecordTypes, err = read(*records); err != nil {
		return err
	}
	if in.Daily, err = read(*daily); err != nil {
		return err
	}
	data, err := monitor.BuildFinanceAWSPackage(in)
	if err != nil {
		return err
	}
	// Recheck exact input bytes before creating output. Packaging does not
	// silently accept an evidence refresh halfway through the operation.
	for path, want := range hashes {
		b, err := readRegular(path, maxInputBytes)
		if err != nil {
			return err
		}
		h := sha256.Sum256(b)
		if hex.EncodeToString(h[:]) != want {
			return fmt.Errorf("input changed while packaging: %s", path)
		}
	}
	absolute, err := filepath.Abs(*destination)
	if err != nil {
		return err
	}
	if err := writeNewAtomic(absolute, data); err != nil {
		return err
	}
	h := sha256.Sum256(data)
	return json.NewEncoder(out).Encode(receipt{Mode: "offline_validated_not_published", Output: absolute, SHA256: hex.EncodeToString(h[:]), InputSHA256: hashes})
}

func readRegular(path string, limit int) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > int64(limit) {
		return nil, fmt.Errorf("input must be a bounded regular file: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(info, opened) {
		return nil, fmt.Errorf("input identity changed: %s", path)
	}
	b, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > limit {
		return nil, fmt.Errorf("input grew past size limit: %s", path)
	}
	return b, nil
}

// Link a fully written same-directory temporary file into a NEW name. Unlike
// rename, link refuses existing files/symlinks and exposes no partial JSON.
func writeNewAtomic(path string, data []byte) error {
	parent := filepath.Dir(path)
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("output parent must be a regular directory")
	}
	tmp, err := os.CreateTemp(parent, ".aws-package-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Link(tmp.Name(), path); err != nil {
		return fmt.Errorf("create new output (existing paths are never replaced): %w", err)
	}
	return nil
}
