package monitor

import (
	"context"
	"io"
	"os"
)

// Use the validated, content-addressed statement rather than size/mtime:
// copying a corrected artifact with preserved metadata must invalidate a
// cached cost. Invalid/missing optional CUR inputs get distinct non-financial
// versions so the report can still show its user/upstream data and CUR error.
// Both source formats retain bounded input and integrity checks.
func financeCURArtifactSourceVersion(ctx context.Context, path string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return "missing", nil
	}
	if err != nil {
		return "unreadable", nil
	}
	if !info.Mode().IsRegular() {
		return "invalid-type", nil
	}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return "missing", nil
	}
	if err != nil {
		return "unreadable", nil
	}
	defer file.Close()
	source, err := readFinanceCURSource(financeCURContextReader{ctx: ctx, reader: file})
	if canceled := ctx.Err(); canceled != nil {
		return "", canceled
	}
	if err != nil {
		return "invalid", nil
	}
	return source.version(), nil
}

type financeCURContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r financeCURContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
