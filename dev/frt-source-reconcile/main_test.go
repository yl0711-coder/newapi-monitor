package main

import "testing"

func TestValidateReadOnlyDSN(t *testing.T) {
	for _, tc := range []struct {
		dsn  string
		want string
	}{
		{"monitor_ro:test@tcp(localhost:3306)/nexusapi", "monitor_ro"},
		{"nexus_ro:test@tcp(localhost:13306)/nexusapi?parseTime=true", "nexus_ro"},
		{"nexus_ro:test@tcp(localhost:13306)/nexusapi_other", ""},
		{"nexus_ro:test@unix(/tmp/mysql.sock)/nexusapi", ""},
		{"root:test@tcp(localhost:3306)/nexusapi", ""},
		{"", ""},
		{"invalid", ""},
	} {
		user, err := validateReadOnlyDSN(tc.dsn)
		if (err == nil) != (tc.want != "") || user != tc.want {
			t.Errorf("allowlist validation: want user=%q got user=%q err=%v", tc.want, user, err)
		}
	}
}
