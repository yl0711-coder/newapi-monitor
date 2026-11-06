package monthlydraft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPublicationJournalRetentionProtectsPendingAndBindsSnapshot(t *testing.T) {
	for _, state := range []string{publicationRunning, publicationRetry, publicationPaused, publicationSaved, "invalidated"} {
		t.Run(state, func(t *testing.T) {
			f, old, s := publicationFixture(t)
			_ = retentionReady(t, f, 2) // supersede the old draft for the same month
			ctx := context.Background()
			asOf := time.Now().Add(45 * 24 * time.Hour)
			before, err := InspectGenerationRetention(ctx, f.receiver, asOf)
			if err != nil {
				t.Fatal(err)
			}
			retentionItem(t, before, old, "archive_copy_candidate")
			l, ok, err := s.claim(ctx, old)
			if err != nil || !ok {
				t.Fatal("claim", err)
			}
			want := "keep_active"
			switch state {
			case publicationRetry:
				err = s.finish(ctx, l, errors.New("temporary synthetic fault"))
				want = "keep_retryable"
			case publicationPaused:
				err = s.finish(ctx, l, ErrGenerationEvidence)
				want = "manual_review"
			case publicationSaved:
				if err = s.bind(ctx, l, []byte("synthetic evidence")); err == nil {
					err = s.finish(ctx, l, nil)
				}
				want = "archive_copy_candidate"
			case "invalidated":
				err = f.g.tasks.Invalidate(ctx, old.Request.Owner, old.ID)
				want = "archive_copy_candidate"
			}
			if err != nil {
				t.Fatal(err)
			}
			after, err := InspectGenerationRetention(ctx, f.receiver, asOf)
			if err != nil {
				t.Fatal(err)
			}
			retentionItem(t, after, old, want)
			if after.SnapshotSHA256 == before.SnapshotSHA256 {
				t.Fatal("publication journal did not invalidate archive snapshot")
			}
			if _, err := CopyGenerationArchive(ctx, f.receiver, archiveDirectory(t), before.SnapshotSHA256, asOf); !errors.Is(err, ErrGenerationChanged) {
				t.Fatal("stale archive plan accepted", err)
			}
		})
	}
}

func TestPublicationJournalFencesCompletedPayloadRelease(t *testing.T) {
	for _, pending := range []bool{true, false} {
		t.Run(map[bool]string{true: "claim_first", false: "archive_first"}[pending], func(t *testing.T) {
			f, old, _, receipt, asOf := completedArchiveFixture(t)
			ctx := context.Background()
			s, err := newPublicationStore(f.receiver)
			if err != nil {
				t.Fatal(err)
			}
			if err := InitCompletedGenerationRetention(ctx, f.receiver); err != nil {
				t.Fatal(err)
			}
			if pending {
				if _, ok, err := s.claim(ctx, old); err != nil || !ok {
					t.Fatal(err)
				}
				if err := ChangeCompletedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, false); !errors.Is(err, ErrGenerationInput) {
					t.Fatal("archive released pending publication input", err)
				}
				if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 2 {
					t.Fatal("pending payload was removed")
				}
			} else {
				if err := ChangeCompletedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, false); err != nil {
					t.Fatal(err)
				}
				if _, ok, err := s.claim(ctx, old); !errors.Is(err, ErrGenerationChanged) || ok {
					t.Fatal("publication claimed already archived payload", ok, err)
				}
			}
		})
	}
}

func TestPublicationJournalRetentionLegacyAndUnknownSchema(t *testing.T) {
	f, _, _ := publicationFixture(t)
	ctx := context.Background()
	// Empty recovery tables can be absent on an older draft-only receiver.
	for _, table := range []string{"portal_monthly_publication_jobs", "portal_monthly_publication_control"} {
		if _, err := f.receiver.Exec("DROP TABLE " + table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := InspectGenerationRetention(ctx, f.receiver, time.Now()); err != nil {
		t.Fatal("legacy draft inventory blocked", err)
	}
	if err := InitAutomaticPublicationReceiver(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_publication_control SET version=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectGenerationRetention(ctx, f.receiver, time.Now()); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("unknown journal version ignored during maintenance", err)
	}
}
