package main

import (
	"go/token"
	"go/types"
	"testing"
)

func TestSQLResultDecodersAreNotExecutions(t *testing.T) {
	for _, c := range []struct {
		pkg, name string
		want      bool
	}{
		{"database/sql", "Scan", true}, {"gorm.io/gorm", "Scan", false}, {"database/sql", "QueryContext", false}, {"example.invalid/local", "Scan", false},
	} {
		obj := types.NewFunc(token.NoPos, types.NewPackage(c.pkg, "test"), c.name, nil)
		if sqlResultDecoder(obj) != c.want {
			t.Fatalf("wrong method classification %s.%s", c.pkg, c.name)
		}
	}
	if sqlResultDecoder(nil) {
		t.Fatal("nil method is not a decoder")
	}
}
