package monthlytasks

import (
	"context"
	"database/sql"
	"errors"
)

// Find locates an exact server-derived descriptor without creating, retrying,
// renewing or repairing a task. It is not authorization and not a customer DTO.
func (s *Store) Find(parent context.Context, request Request) (Task, error) {
	raw, err := requestBytes(request)
	if err != nil {
		return Task{}, err
	}
	ctx, cancel := context.WithTimeout(parent, operationLimit)
	defer cancel()
	r, err := load(ctx, s.db, "task_key", hash(raw))
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, ErrNotFound
	}
	if err != nil {
		return Task{}, err
	}
	if r.Request != request || r.descriptor != string(raw) {
		return Task{}, ErrCorrupt
	}
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	return r.Task, nil
}
