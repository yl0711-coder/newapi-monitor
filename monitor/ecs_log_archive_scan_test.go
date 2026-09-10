package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

type archiveReaderFixture struct {
	objects       map[string]ecsarchive.Object
	pages         map[string]ecsarchive.Page
	failList      bool
	failListToken string
	failGet       string
	lastToken     string
	gets          int
	lists         int
	reads         map[string]int
	delay         time.Duration
	cancelAt      int
	cancel        context.CancelFunc
}

func (f *archiveReaderFixture) List(_ context.Context, token string) (ecsarchive.Page, error) {
	f.lastToken = token
	f.lists++
	if f.failList || f.failListToken != "" && token == f.failListToken {
		return ecsarchive.Page{}, errors.New("AWS unavailable")
	}
	return f.pages[token], nil
}
func (f *archiveReaderFixture) Get(ctx context.Context, key string) (ecsarchive.Object, error) {
	f.gets++
	if f.reads == nil {
		f.reads = map[string]int{}
	}
	f.reads[key]++
	if f.cancelAt > 0 && f.gets == f.cancelAt {
		f.cancel()
	}
	if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ecsarchive.Object{}, ctx.Err()
		case <-timer.C:
		}
	}
	if ctx.Err() != nil {
		return ecsarchive.Object{}, ctx.Err()
	}
	if f.failGet == key {
		return ecsarchive.Object{}, errors.New("read unavailable")
	}
	o, ok := f.objects[key]
	if !ok {
		return o, errors.New("missing object")
	}
	return o, nil
}
func archiveEntry(o ecsarchive.Object) ecsarchive.Entry {
	return ecsarchive.Entry{Key: o.Key, ETag: o.ETag, Modified: o.Modified, Size: int64(len(o.Body))}
}

func TestECSLogArchiveReverseOrderRestartAndPagination(t *testing.T) {
	m := newECSLogTestMonitor(t)
	ctx := context.Background()
	s, key := registerECSTestSource(t, m, strings.Repeat("a", 32), "reject")
	seal := func(id, previous string) ecsarchive.Object {
		body := ecsLaneTestBody(t, s.Node, s.Lane, 1)
		var in map[string]any
		if err := json.Unmarshal(body, &in); err != nil {
			t.Fatal(err)
		}
		in["batch_id"] = id
		body, _ = json.Marshal(in)
		e, err := ecsarchive.SealAfter(m.cfg.ECSLogAudience, s.Node, s.Lane, body, previous, key)
		if err != nil {
			t.Fatal(err)
		}
		k, err := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32), e)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(e)
		return ecsarchive.Object{Key: k, ETag: "fixture-etag", Modified: time.Now(), Body: raw}
	}
	first := seal("archive-first-abcdefgh", "")
	envelope, _ := ecsarchive.Decode(first.Body)
	second := seal("archive-second-abcdefgh", envelope.Hash)
	reader := &archiveReaderFixture{objects: map[string]ecsarchive.Object{first.Key: first, second.Key: second}, pages: map[string]ecsarchive.Page{
		"":       {Entries: []ecsarchive.Entry{archiveEntry(second)}, Next: "page-2"},
		"page-2": {Entries: []ecsarchive.Entry{archiveEntry(first)}},
	}}
	// Fail the actual second-page request rather than assume one poll can only
	// read one page. The dependency and persisted outage cursor remain mandatory.
	reader.failListToken = "page-2"
	r := newECSArchiveReplayer(m, "isolated/")
	if err := r.scan(ctx, reader, "fixture-binding", time.Now()); err == nil {
		t.Fatal("AWS outage hidden")
	}
	var pending ECSLogArchiveReceipt
	if err := m.storeDB.First(&pending, "object_key = ?", second.Key).Error; err != nil || pending.Status != 425 {
		t.Fatalf("out-of-order not deferred %+v %v", pending, err)
	}
	var scan ECSLogArchiveScan
	if err := m.storeDB.First(&scan, 1).Error; err != nil || scan.NextToken != "page-2" || scan.LastFailure == 0 {
		t.Fatalf("outage erased cursor %+v %v", scan, err)
	}
	// Close/reopen the actual temporary SQLite. Scanner state must survive.
	var files []struct{ Name, File string }
	if err := m.storeDB.Raw("PRAGMA database_list").Scan(&files).Error; err != nil {
		t.Fatal(err)
	}
	var path string
	for _, f := range files {
		if f.Name == "main" {
			path = f.File
		}
	}
	if path == "" {
		t.Fatal("missing test database")
	}
	m.Close()
	reopened := &Monitor{cfg: m.cfg, ecsLogPolicies: m.ecsLogPolicies}
	if err := reopened.openStore(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	r = newECSArchiveReplayer(reopened, "isolated/")
	reader.failListToken = ""
	for i := 0; i < 4; i++ {
		if err := r.scan(ctx, reader, "fixture-binding", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	var accepted int64
	if err := reopened.storeDB.Model(&ECSLogArchiveReceipt{}).Where("status = 200").Count(&accepted).Error; err != nil || accepted != 2 {
		t.Fatalf("not recovered %d %v", accepted, err)
	}
	if rows := reopened.storeRejections(time.Now().Unix() - 120); len(rows) != 1 || rows[0].Count != 2 {
		t.Fatalf("facts lost or duplicated %+v", rows)
	}
	if r.scan(ctx, reader, "different-bucket", time.Now()) == nil {
		t.Fatal("silent archive destination change")
	}
}

func TestECSLogArchiveLeaseGapNotRetroactivelyAuthorized(t *testing.T) {
	m := newECSLogTestMonitor(t)
	s, key := registerECSTestSource(t, m, strings.Repeat("d", 32), "reject")
	gap := time.Now().Add(-time.Minute)
	if err := m.storeDB.Model(&s).Updates(map[string]any{"first_seen": gap.Unix() - 1000, "lease_until": gap.Unix() + 1000}).Error; err != nil {
		t.Fatal(err)
	}
	e, err := ecsarchive.Seal(m.cfg.ECSLogAudience, s.Node, s.Lane, ecsLaneTestBody(t, s.Node, s.Lane, 1), key)
	if err != nil {
		t.Fatal(err)
	}
	k, _ := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("d", 32), e)
	raw, _ := json.Marshal(e)
	o := ecsarchive.Object{Key: k, ETag: "fixture", Modified: gap, Body: raw}
	if got := newECSArchiveReplayer(m, "isolated/").replay(context.Background(), o, time.Now()); got.Status != 403 {
		t.Fatalf("unverified gap accepted %+v", got)
	}
}
