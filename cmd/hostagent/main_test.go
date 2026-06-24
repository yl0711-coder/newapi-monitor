package main

import "testing"

func TestParseMeminfo(t *testing.T) {
	// MemTotal 2097152 kB=2048MB, MemAvailable 1048576 kB=1024MB, Swap 已用=(2097152-1048576)kB=1024MB
	b := []byte("MemTotal:       2097152 kB\nMemFree:         100000 kB\nMemAvailable:   1048576 kB\nSwapTotal:      2097152 kB\nSwapFree:       1048576 kB\n")
	mi := parseMeminfo(b)
	if mi.totalMB != 2048 {
		t.Fatalf("totalMB 应 2048,得 %v", mi.totalMB)
	}
	if mi.availMB != 1024 {
		t.Fatalf("availMB 应 1024,得 %v", mi.availMB)
	}
	if mi.swapUsedMB != 1024 {
		t.Fatalf("swapUsedMB 应 1024,得 %v", mi.swapUsedMB)
	}
}

func TestParseLoadavg(t *testing.T) {
	l1, l5, l15 := parseLoadavg("0.52 0.31 0.20 1/234 5678\n")
	if l1 != 0.52 || l5 != 0.31 || l15 != 0.20 {
		t.Fatalf("load 解析错: %v %v %v", l1, l5, l15)
	}
}

func TestParseLoadavgShort(t *testing.T) {
	// 字段不足时不应 panic,缺的归 0
	l1, l5, l15 := parseLoadavg("0.10")
	if l1 != 0.10 || l5 != 0 || l15 != 0 {
		t.Fatalf("短输入应只取到 l1: %v %v %v", l1, l5, l15)
	}
}

func TestDiskUsedPctSmoke(t *testing.T) {
	pct, err := diskUsedPct("/")
	if err != nil {
		t.Fatalf("statfs / 失败: %v", err)
	}
	if pct <= 0 || pct >= 100 {
		t.Fatalf("磁盘用量应在 0~100 之间,得 %v", pct)
	}
}
