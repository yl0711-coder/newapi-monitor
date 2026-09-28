//go:build unix

package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

func giftSeriesFileHash(ctx context.Context, path string) (string, error) {
	if err := giftLocalClosedBackup(path); err != nil {
		return "", err
	}
	f, err := giftLocalOpenRegular(path, os.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	buffer := make([]byte, 64<<10)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buffer)
		if n > 0 {
			h.Write(buffer[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
