package monitor

import "testing"

// TestSnapshotCache:TTL 内命中缓存(同一指针)、超 TTL 重算。
func TestSnapshotCache(t *testing.T) {
	m := newTestMonitor(t)
	now := int64(1_700_000_000)
	s1, err := m.GetSnapshot(60, now)
	if err != nil {
		t.Fatal(err)
	}
	if s2, _ := m.GetSnapshot(60, now+5); s1 != s2 {
		t.Error("TTL 内应命中缓存(返回同一快照指针)")
	}
	if s3, _ := m.GetSnapshot(60, now+snapCacheTTL+1); s3 == s1 {
		t.Error("超过 TTL 应重算(新指针)")
	}
}
