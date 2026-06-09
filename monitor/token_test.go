package monitor

import "testing"

// TestStoreTokens:按令牌聚合——三态/成功率/成本/排序对不对。
func TestStoreTokens(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60
	if err := m.upsertTokenSamples([]TokenSample{
		{BucketTs: bucket, TokenName: "key-a", Success: 8, Anomaly: 0, Failed: 2, Tokens: 100, Quota: 500000},
		{BucketTs: bucket, TokenName: "key-b", Success: 20, Anomaly: 0, Failed: 0, Tokens: 50, Quota: 0},
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := m.storeTokens(int64(bucket-60), 900)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("应 2 个令牌,实际 %d", len(rows))
	}
	if rows[0].Key != "key-a" { // 按 failed 降序,key-a(failed=2)在前
		t.Errorf("应按错误降序,key-a 在前,实际 %q", rows[0].Key)
	}
	byk := map[string]TokenRow{rows[0].Key: rows[0], rows[1].Key: rows[1]}
	if r := byk["key-a"]; r.Total != 10 || !approx(r.SuccessRate, 80) || !approx(r.CostUSD, 1.0) {
		t.Errorf("key-a: total=%d successRate=%v cost=%v(应 10 / 80%% / $1)", r.Total, r.SuccessRate, r.CostUSD)
	}
	if r := byk["key-b"]; r.Total != 20 || !approx(r.SuccessRate, 100) {
		t.Errorf("key-b: total=%d successRate=%v(应 20 / 100%%)", r.Total, r.SuccessRate)
	}

	// 空令牌名 → 显示 (无令牌名)
	if err := m.upsertTokenSamples([]TokenSample{{BucketTs: bucket + 60, TokenName: "", Failed: 1}}); err != nil {
		t.Fatal(err)
	}
	rows, _ = m.storeTokens(int64(bucket-60), 900)
	found := false
	for _, r := range rows {
		if r.Key == "(无令牌名)" {
			found = true
		}
	}
	if !found {
		t.Error("空 token_name 应聚成 (无令牌名)")
	}
}
