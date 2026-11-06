package monthlydraft

import (
	"context"
	"encoding/json"
	"os"
)

// Bounded, read-only capacity identity. Existing sealed/broken-complete output
// is NOT permission to recapture; it needs its original verification path.
func (s *CapturedSourceSpool) continuationCapacityStamp(ctx context.Context, req SourcePreparationRequest) (string, error) {
	root, lock, err := openCapturedOutput(s.directory)
	if err != nil {
		return "", err
	}
	defer root.Close()
	defer lock.Close()
	info, err := root.Stat(".")
	if err != nil || !os.SameFile(info, s.identity) {
		return "", ErrGenerationIntegrity
	}
	i, err := inspectCapturedSpoolPolicy(ctx, root, s.retainCompleted)
	if err != nil {
		return "", err
	}
	job := s.jobName(req)
	last := 0
	if _, err := root.Lstat(job); os.IsNotExist(err) {
		if !s.canAllocateJob(i) {
			return "", ErrGenerationCapacity
		}
	} else if err != nil {
		return "", err
	} else {
		j, err := root.OpenRoot(job)
		if err != nil {
			return "", err
		}
		var complete string
		last, complete, err = inspectCapturedJob(j)
		closeErr := j.Close()
		if err != nil {
			return "", err
		}
		if closeErr != nil {
			return "", closeErr
		}
		if complete != "" || last >= capturedSpoolAttempts {
			return "", ErrPreparationResumeDenied
		}
	}
	if s.limit-i.bytes-(64<<10) <= 4<<20 {
		return "", ErrGenerationCapacity
	}
	raw, _ := json.Marshal([]any{s.directory, job, last, i.jobs, i.closedJobs, i.bytes, s.limit, s.retainCompleted})
	return generationHash(raw), ctx.Err()
}
