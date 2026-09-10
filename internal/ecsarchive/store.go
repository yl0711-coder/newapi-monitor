package ecsarchive

import (
	"context"
	"errors"
	"time"
)

var ErrExists = errors.New("archive object already exists")

type Object struct {
	Key, ETag string
	Modified  time.Time
	Body      []byte
}
type Entry struct {
	Key, ETag string
	Modified  time.Time
	Size      int64
}
type Page struct {
	Entries []Entry
	Next    string
}

// Put must be create-only. Neither writer nor recovery worker has Delete.
// Modified/ETag must come from the trusted store, never from envelope fields.
type Writer interface {
	Binding() string
	Put(context.Context, string, []byte) error
	Get(context.Context, string) (Object, error)
}
type Reader interface {
	Get(context.Context, string) (Object, error)
	List(context.Context, string) (Page, error)
}
