package ecslogagent

import (
	"context"
	"errors"
	"time"
)

const producerStartupGrace = 120 * time.Second
const metadataRetryInterval = 2 * time.Second

// WaitMetadata permits an isolated collector to START before its producer.
// This is needed for reverse shutdown ordering. It does not grant a lease or
// declare the collector ready/healthy; Monitor still verifies the task via AWS.
func WaitMetadata(ctx context.Context, uri, container string) (Metadata, error) {
	ctx, cancel := context.WithTimeout(ctx, producerStartupGrace)
	defer cancel()
	return waitMetadata(ctx, metadataRetryInterval, func(ctx context.Context) (Metadata, error) { return ReadMetadata(ctx, uri, container) })
}

func waitMetadata(ctx context.Context, interval time.Duration, read func(context.Context) (Metadata, error)) (Metadata, error) {
	for {
		if err := ctx.Err(); err != nil {
			return Metadata{}, err
		}
		meta, err := read(ctx)
		if ctx.Err() != nil {
			return Metadata{}, ctx.Err()
		}
		if err == nil {
			return meta, nil
		}
		// Permanent endpoint/config/ambiguous identity failures fail closed;
		// only not-yet-started producers or transient metadata errors retry.
		if !errors.Is(err, errMetadataNotReady) && !errors.Is(err, errMetadataUnavailable) {
			return Metadata{}, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Metadata{}, ctx.Err()
		case <-timer.C:
		}
	}
}
