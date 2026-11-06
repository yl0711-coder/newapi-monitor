package monthlysource

import (
	"testing"
	"time"
)

// The first whole-month sweep still needs its full independent gap. Once a
// real second observation exists, a later split must not block unrelated
// windows that already satisfy their own independent gap.
func TestRecoveringMonthReadinessAfterLateSplit(t *testing.T) {
	for _, marker := range []string{"ordinary", "adaptive", "none"} {
		t.Run(marker, func(t *testing.T) {
			m := newRecoveringMonthFixture(t, false)
			now := m.f.now.Add(20 * time.Minute)
			half := len(m.plan.Scans) / 2
			s := recoveringTokenInventory{plan: m.plan, old: map[int]recoveringTokenSlot{}, days: map[int]recoveringTokenDay{}, progress: TokenMonthCaptureProgress{Total: half * 2}, oldEnd: now.Add(-10 * time.Second).UnixMilli()}
			for i := 0; i < half; i++ {
				s.old[i] = recoveringTokenSlot{completed: now.Add(-10 * time.Minute).UnixMilli()}
			}
			leaf := UsageWindowRequest{From: 100, To: 200}
			waiting := adaptiveTokenState{leaves: []UsageWindowRequest{leaf}, records: map[adaptiveTokenSlot]adaptiveTokenRecord{{adaptiveTokenNode{100, 200}, 0}: {completed: s.oldEnd}}}
			s.days[0] = recoveringTokenDay{state: waiting, phase: -1, progress: AdaptiveTokenWindowProgress{NextEligibleMS: s.oldEnd + 300000}}
			s.old[1+half] = recoveringTokenSlot{completed: now.Add(-time.Minute).UnixMilli()}
			if marker == "none" {
				delete(s.old, 1+half)
			}
			if marker == "adaptive" {
				delete(s.old, 1+half)
				complete := adaptiveTokenState{leaves: []UsageWindowRequest{leaf}, records: map[adaptiveTokenSlot]adaptiveTokenRecord{
					{adaptiveTokenNode{100, 200}, 0}: {completed: now.Add(-10 * time.Minute).UnixMilli()},
					{adaptiveTokenNode{100, 200}, 1}: {started: now.Add(-time.Minute).UnixMilli(), completed: now.Add(-time.Minute).UnixMilli()},
				}}
				s.days[1] = recoveringTokenDay{state: complete, phase: -1, progress: AdaptiveTokenWindowProgress{Complete: true}}
			}
			p, action := recoveringTokenProgress(s, now)
			if marker == "none" {
				if action != nil || p.NextEligibleMS != s.oldEnd+300000 {
					t.Fatal("initial whole-first-sweep gap bypassed", p, action)
				}
				return
			}
			if action == nil || action.child || action.ordinal != half+2 || p.NextEligibleMS != 0 {
				t.Fatal("late waiting split blocked an independently ready ordinary window", p, action)
			}
			// Even after the initial gap, a new ordinary first observation is
			// not permission to read that same window's second view early.
			o := s.old[2]
			o.completed = s.oldEnd
			s.old[2] = o
			_, action = recoveringTokenProgress(s, now)
			if action == nil || action.ordinal != half+3 {
				t.Fatal("individual ordinary gap not respected", action)
			}
			// Missing first reads retain priority over second observations.
			delete(s.old, 4)
			_, action = recoveringTokenProgress(s, now)
			if action == nil || action.child || action.ordinal != 4 {
				t.Fatal("first-read priority changed", action)
			}
			// If every other window really is done, retain the remaining
			// child's exact wake-up instead of spinning or reading it early.
			for i := 1; i < half; i++ {
				s.old[i] = recoveringTokenSlot{completed: now.Add(-10 * time.Minute).UnixMilli()}
				s.old[i+half] = recoveringTokenSlot{started: now.Add(-time.Minute).UnixMilli(), completed: now.Add(-time.Minute).UnixMilli()}
			}
			p, action = recoveringTokenProgress(s, now)
			if action != nil || p.NextEligibleMS != s.oldEnd+300000 || p.TraversalsFinished {
				t.Fatal("all-waiting state lost its independent wake-up", p, action)
			}
		})
	}
}

func TestRecoveringMonthLateSplitReadyWorkPreservesOriginalMoney(t *testing.T) {
	m := newRecoveringMonthFixture(t, true)
	// Fail the third day's second observation, after two actual second
	// originals have committed. The fixture's unequal event set follows it.
	root, err := tokenCaptureRequest(m.plan, 2)
	if err != nil {
		t.Fatal(err)
	}
	m.f.root = root
	ready := false
	for i := 0; i < 100; i++ {
		p, err := m.advance(t)
		if err != nil {
			t.Fatal(err)
		}
		var children int
		if err := m.f.store.QueryRow(`SELECT count(*) FROM monthly_adaptive_token_observations WHERE phase=0`).Scan(&children); err != nil {
			t.Fatal(err)
		}
		if children == 2 {
			ready = true
			break
		}
		if p.NextEligibleMS > m.f.now.UnixMilli() {
			m.f.now = time.UnixMilli(p.NextEligibleMS)
		}
	}
	if !ready {
		t.Fatal("late split first originals missing")
	}
	at, queries := m.f.now, m.f.queries()
	p, err := m.advance(t)
	want, wantErr := tokenCaptureRequest(m.plan, len(m.plan.Scans)/2+3)
	if err != nil || wantErr != nil || !p.Captured || m.f.queries() <= queries || m.f.now.Sub(at) >= time.Second || len(m.requests) == 0 || m.requests[len(m.requests)-1] != want {
		t.Fatal("ready day blocked behind late child wait", p, err)
	}
	finishRecoveringMonth(t, m)
	pairs, report, err := LoadRecoveredTokenMonth(t.Context(), m.f.store, m.f.job, m.scope, root.UserID)
	if err != nil || report.ConsumeQuota != 11604 || report.RefundQuota != 0 || len(pairs) != 31 || m.parentCalls != 2 {
		t.Fatal("ready-work scheduling changed original money or repeated failed parent", report, err, m.parentCalls)
	}
	for _, pair := range pairs {
		if !TokenUsageWindowsMatch(pair.Older, pair.Newer) || pair.Newer.StartedAtMS-pair.Older.CompletedAtMS < 300000 {
			t.Fatal("independent source gap or original equality changed")
		}
	}
}
