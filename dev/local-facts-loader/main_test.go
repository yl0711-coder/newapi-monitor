package main

import "testing"

func TestValidateLocalDSNHardBoundary(t *testing.T) {
	accepted := []string{
		"local_loader:p@tcp(127.0.0.1:13316)/newapi_local_acceptance",
		"local_loader:p@tcp(localhost:13316)/newapi_local_acceptance",
		"local_loader:p@tcp([::1]:13316)/newapi_local_acceptance",
	}
	for _, dsn := range accepted {
		if _, _, err := validateLocalDSN(dsn, ""); err != nil {
			t.Fatalf("local DSN rejected %q: %v", dsn, err)
		}
	}
	rejected := []string{
		"root:p@tcp(127.0.0.1:13316)/newapi_local_acceptance",
		"local_loader:p@tcp(127.0.0.1:3306)/production",
		"local_loader:p@tcp(10.0.0.8:3306)/newapi_local_acceptance",
		"local_loader:p@tcp(db.internal:3306)/newapi_local_acceptance",
		"local_loader:p@unix(/tmp/mysql.sock)/newapi_local_acceptance",
	}
	for _, dsn := range rejected {
		if _, _, err := validateLocalDSN(dsn, ""); err == nil {
			t.Fatalf("unsafe DSN accepted: %q", dsn)
		}
	}
	containerDSN := "local_loader:p@tcp(nxmon-facts-mysql-20260814:3306)/newapi_local_acceptance"
	if _, _, err := validateLocalDSN(containerDSN, "nxmon-facts-mysql-20260814"); err != nil {
		t.Fatalf("explicit isolated container rejected: %v", err)
	}
	if _, _, err := validateLocalDSN(containerDSN, "nxmon-facts-mysql-other"); err == nil {
		t.Fatal("mismatched isolated container host accepted")
	}
	if _, _, err := validateLocalDSN("local_loader:p@tcp(mysql.prod:3306)/newapi_local_acceptance", "mysql.prod"); err == nil {
		t.Fatal("non-acceptance hostname accepted through container exception")
	}
}
