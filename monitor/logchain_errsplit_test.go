package monitor

import (
	"strings"
	"testing"
)

func TestSplitErrAnomOnlyForErrAnom(t *testing.T) {
	base := logChainScope{FromTs: 1, ToTs: 2, Limit: 50}
	for _, k := range []string{"", anomalyAll, anomalyStream} {
		s := base
		s.Anomaly = k
		if _, _, ok := logChainSplitErrAnom(s, nil); ok {
			t.Errorf("anomaly=%q 不该拆分", k)
		}
	}
	s := base
	s.Anomaly = anomalyErrAnom
	w, _, ok := logChainSplitErrAnom(s, nil)
	if !ok {
		t.Fatal("err_anom 必须拆分")
	}
	if strings.Contains(w, "type = 5") {
		t.Errorf("第二半不该含 type=5，那是第一半的活: %q", w)
	}
}

// TestSplitErrAnomHalvesCoverOriginal 两半合起来必须与原谓词等价。
//
// err_anom 的定义是 type=5 OR anomalyAll。若第二半漏掉某个异常分支，
// 页面会少行，而少的那些正是该看的问题请求。
func TestSplitErrAnomHalvesCoverOriginal(t *testing.T) {
	s := logChainScope{FromTs: 1, ToTs: 2, Limit: 50, Anomaly: anomalyErrAnom}
	second, _, _ := logChainSplitErrAnom(s, nil)
	full := logChainAnomalySQL(anomalyErrAnom)
	// 原谓词去掉 type = 5 分支后，剩下的每一段都必须出现在第二半里。
	for _, frag := range []string{"end_reason", "error_count", "quota"} {
		if strings.Contains(full, frag) && !strings.Contains(second, frag) {
			t.Errorf("第二半缺 %q，err_anom 会漏行", frag)
		}
	}
}
