package monitor

import "testing"

func TestSimpleChineseAmount(t *testing.T) {
	for cents, want := range map[int64]string{
		1000000: "壹万元整", 2000000: "贰万元整", 1000: "壹拾元整", 100500: "壹仟零伍元整", 1000500: "壹万零伍元整",
		10000000: "壹拾万元整", 101000: "壹仟零壹拾元整", 12345: "壹佰贰拾叁元肆角伍分", 105: "壹元零伍分", 50: "伍角整",
		0: "零元整", 1000000000000: "壹佰亿元整", 100000100: "壹佰万零壹元整", 999999999: "玖佰玖拾玖万玖仟玖佰玖拾玖元玖角玖分",
	} {
		if got := simpleChineseAmount(cents); got != want {
			t.Errorf("%d: got %s want %s", cents, got, want)
		}
	}
}
