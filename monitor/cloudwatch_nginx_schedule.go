package monitor

import (
	"errors"
	"sync/atomic"
	"time"
)

// Only AWS execution evidence justifies a smaller scan. A submission/RPC
// timeout says nothing about range density; retry it with the same boundaries.
func cloudWatchNginxTimeoutStage(err error) string {
	if cloudWatchPreRouteErrorCode(err) != "timeout" {
		return ""
	}
	var e *cloudWatchLogsError
	if errors.As(err, &e) {
		switch e.Operation {
		case "start_query":
			return "submission"
		case "get_query_results":
			return "receive"
		case "poll_query", "query_status":
			return "execution"
		}
	}
	return "budget"
}

func cloudWatchNginxRetryDelay(err error, poll time.Duration) time.Duration {
	switch cloudWatchNginxTimeoutStage(err) {
	case "submission":
		return max(30*time.Second, min(poll, time.Minute))
	case "receive":
		return 30 * time.Second
	case "execution":
		return cloudWatchNginxCatchupDelay
	default:
		return max(30*time.Second, poll)
	}
}

func cloudWatchNginxRecoveredWidth(width, attempted int64, queries int, elapsed time.Duration, fast *atomic.Int64, limit int64) int64 {
	// Eight complete unsplit windows with headroom, including replay/repair
	// and evidence, demonstrate recovery. A 6–11s network round trip should
	// not strand a 60s window forever below the former 5s threshold.
	if width < 60 || width >= limit || attempted < width || queries != 1 || elapsed >= cloudWatchLogsTotalTimeout*3/4 {
		fast.Store(0)
		return width
	}
	if fast.Add(1) < 8 {
		return width
	}
	fast.Store(0)
	return min(limit, width+max(int64(60), width/2/60*60))
}

type cloudWatchNginxLane uint8

const (
	nginxForward cloudWatchNginxLane = iota
	nginxEvidence
	nginxReplay
	nginxRepair
	nginxRecent
)

func (l cloudWatchNginxLane) String() string {
	return [...]string{"forward", "evidence", "replay", "repair", "recent_evidence"}[l]
}

type cloudWatchNginxSchedule struct {
	next                 int
	due                  [5]time.Time
	recentFrom, recentTo int64
}

func cloudWatchNginxReplayWindow(state CloudWatchNginxCursor, now time.Time, poll time.Duration) (int64, int64, int64, bool) {
	from, end := state.ReplayNextTs, state.ReplayThroughTs
	if end <= from {
		if state.ThroughTs <= state.CoverageFromTs || now.Unix() < state.ReplayLastAt+int64(poll/time.Second) {
			return 0, 0, 0, false
		}
		end = state.ThroughTs
		from = max(state.CoverageFromTs, end-int64(cloudWatchNginxReplay/time.Second))
	}
	width := state.ReplayReadWindowSeconds
	if width < 60 || width > int64(cloudWatchNginxWindow/time.Second) {
		width = int64(cloudWatchNginxWindow / time.Second)
	}
	return from, min(end, from+width), end, end > from
}

// Weighted round robin bounds every ready lane's opportunity to six turns.
// An error cools down only that lane. Replay has its own durable endpoint,
// so a newly finalized minute never moves the goalposts of a split replay.
func (s *cloudWatchNginxSchedule) choose(state CloudWatchNginxCursor, target int64, now time.Time, poll time.Duration) (cloudWatchNginxLane, int64, int64, int64, bool) {
	lanes := []cloudWatchNginxLane{nginxForward, nginxEvidence, nginxForward, nginxReplay, nginxForward, nginxRepair}
	if s.recentTo > s.recentFrom {
		lanes = []cloudWatchNginxLane{nginxRecent, nginxForward, nginxRecent, nginxEvidence, nginxRecent, nginxReplay, nginxRecent, nginxRepair}
	}
	for i := 0; i < len(lanes); i++ {
		lane := lanes[s.next%len(lanes)]
		s.next = (s.next + 1) % len(lanes)
		if now.Before(s.due[lane]) {
			continue
		}
		var from, to, end int64
		var ok bool
		switch lane {
		case nginxRecent:
			from, to, ok = s.recentFrom, s.recentTo, s.recentTo > s.recentFrom
		case nginxForward:
			from, to = cloudWatchNginxNextWindow(state, target)
			ok = to > from
		case nginxEvidence:
			from, to, ok = cloudWatchNginxEvidenceWindow(state, target)
		case nginxReplay:
			from, to, end, ok = cloudWatchNginxReplayWindow(state, now, poll)
		case nginxRepair:
			if now.Unix() < state.LastRepairAt+int64(cloudWatchNginxRepairEvery/time.Second) {
				continue
			}
			from, to, ok = cloudWatchNginxRepairWindow(state, target)
		}
		if ok {
			return lane, from, to, end, true
		}
	}
	return 0, 0, 0, 0, false
}

func (s *cloudWatchNginxSchedule) idleDelay(now time.Time, poll time.Duration) time.Duration {
	delay := min(max(time.Second, poll), time.Minute)
	for _, due := range s.due {
		if due.After(now) {
			delay = min(delay, due.Sub(now))
		}
	}
	return delay
}
