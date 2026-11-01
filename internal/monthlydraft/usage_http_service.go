package monthlydraft

import (
	"context"

	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

type usageHTTPService struct {
	g             *Generation
	authorization *gorm.DB
	worker        *LocalWorker
}

// NewUsageHTTPService only adapts an explicitly constructed local generation
// service; it registers no routes, starts no worker and publishes no bill.
func NewUsageHTTPService(g *Generation, authorization *gorm.DB) (monitor.MonthlyGenerationService, error) {
	if g == nil || g.receiver == nil || g.tasks == nil || g.root == nil || g.funding == nil || g.verifyCurrent == nil || authorization == nil {
		return nil, ErrGenerationInput
	}
	return &usageHTTPService{g: g, authorization: authorization}, nil
}

// NewUsageHTTPServiceWithWorker connects a coalesced wakeup and safe progress. The local
// caller must start and stop Serve explicitly; neither constructor starts it.
func NewUsageHTTPServiceWithWorker(g *Generation, authorization *gorm.DB, worker *LocalWorker) (monitor.MonthlyGenerationService, error) {
	if worker == nil || worker.g != g {
		return nil, ErrGenerationInput
	}
	service, err := NewUsageHTTPService(g, authorization)
	if err != nil {
		return nil, err
	}
	s := service.(*usageHTTPService)
	s.worker = worker
	return s, nil
}

func (s *usageHTTPService) SubmitMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	task, err := s.g.SubmitUsageMonth(ctx, s.authorization, gid, version, month)
	if err != nil {
		return monitor.MonthlyGenerationView{}, monitor.MonthlyGenerationFailure(PublicUsageGenerationProblem(err).Code)
	}
	if s.worker != nil {
		s.worker.Wake()
	}
	// Read the current selected state rather than exposing the internal Task or
	// returning a stale submission snapshot after a concurrent source change.
	view, err := s.ReadMonthly(ctx, gid, version, month)
	if err != nil {
		return monitor.MonthlyGenerationView{}, err
	}
	if err := s.g.currentTask(ctx, task); err != nil {
		return monitor.MonthlyGenerationView{}, monitor.MonthlyGenerationFailure(PublicUsageGenerationProblem(err).Code)
	}
	return view, nil
}

func (s *usageHTTPService) ReadMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	status, err := s.g.ReadUsageMonthStatus(ctx, s.authorization, gid, version, month)
	if err != nil {
		return monitor.MonthlyGenerationView{}, monitor.MonthlyGenerationFailure(PublicUsageGenerationProblem(err).Code)
	}
	if s.worker != nil && s.worker.finalize != nil && status.State == "pending_verification" {
		status.State, err = s.worker.finalizationState(ctx, status.taskID)
		if err != nil {
			return monitor.MonthlyGenerationView{}, monitor.MonthlyGenerationFailure(PublicUsageGenerationProblem(err).Code)
		}
	}
	return monitor.MonthlyGenerationView{Month: status.Month, State: status.State}, nil
}
