package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Deliberately READ ONLY: no output directory, connection, capture, write,
// wallet-store mutation, task or approval parameters exist on this command.
func runTokenWalletReuse(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("inspect-token-wallet", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("archive", "", "private canonical original token-month archive")
	expected := fs.String("expected-archive", "", "independently pinned original archive SHA256")
	if fs.Parse(args) != nil || fs.NArg() != 0 || !validSHA(*expected) {
		return errInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := privateRead(*path, monthlysource.TokenUsageArchiveLimit)
	if err != nil {
		return err
	}
	report, err := monthlysource.ReviewTokenWalletReuse(ctx, raw, *expected, time.Now())
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}
