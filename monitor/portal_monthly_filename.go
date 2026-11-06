package monitor

import (
	"strings"
	"time"
	"unicode"
)

// Only the downloaded file name is normalized. Account names in immutable bills
// and evidence are never rewritten. Keep this policy aligned with the browser
// helper and its shared synthetic test cases.
func monthlyBillFilename(account, month string) string {
	if _, err := time.Parse("2006-01", month); err != nil || len(month) != 7 {
		return ""
	}
	name := strings.Map(func(r rune) rune {
		if r < 32 || r >= 0x7f && r <= 0x9f || strings.ContainsRune(`<>:"/\|?*`, r) ||
			r >= 0x202a && r <= 0x202e || r >= 0x2066 && r <= 0x2069 || r == 0xfeff {
			return '_'
		}
		if unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, account)
	name = strings.Trim(strings.Join(strings.Fields(name), " "), ". ")
	runes := []rune(name)
	if len(runes) > 48 {
		name = string(runes[:48])
	}
	name = strings.TrimRight(name, ". ")
	if name == "" {
		name = "组织"
	}
	return "NexusAPI-" + name + "-月度账单-" + month + ".xlsx"
}
