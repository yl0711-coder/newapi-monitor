package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Constant derived aggregates can execute during EXPLAIN. They must not be
// blindly submitted to a large replica under a metadata-only label.
func TestSQLAuditDerivedMySQLPlans(t *testing.T) {
	name := os.Getenv("MONITOR_SQL_AUDIT_DERIVED_CONTAINER")
	if name == "" {
		t.Skip("explicit task-owned isolated container required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	meta, err := exec.CommandContext(ctx, "docker", "inspect", "--format", `{{index .Config.Labels "ai.task"}} {{.HostConfig.NetworkMode}}`, name).CombinedOutput()
	if err != nil || strings.TrimSpace(string(meta)) != "monitor-sql-audit-20261126 none" {
		t.Fatal("not task-owned isolated MySQL")
	}
	run := func(q string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "docker", "exec", "-i", name, "mysql", "--protocol=socket", "-uroot", "--batch", "--raw", "--skip-column-names")
		cmd.Stdin = strings.NewReader(q)
		var out, diagnostic bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &diagnostic
		if err := cmd.Run(); err != nil {
			t.Fatalf("isolated MySQL: %v: %s", err, diagnostic.String())
		}
		return strings.TrimSpace(out.String())
	}
	run("CREATE DATABASE sql_audit_synthetic; USE sql_audit_synthetic; CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,user_id BIGINT,username VARCHAR(80),model_name VARCHAR(80),token_name VARCHAR(80),channel_id INT,`group` VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),INDEX idx_created_at_type(created_at,type)); INSERT INTO logs VALUES (1,1795683600,2,100,168,'GT_Infra001','gpt-6.1-sol','audit',269,'codex-1.2x','audit-absent-request','audit-absent-upstream'),(2,1795683601,5,0,168,'GT_Infra001','gpt-6.1-sol','audit',269,'codex-1.2x','r2','u2');")
	data, err := os.ReadFile("../docs/sql-audit-20261126/mysql-catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		Cases []mysqlAuditStatement `json:"cases"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err = dec.Decode(&catalog); err != nil {
		t.Fatal(err)
	}
	type result struct {
		ID, Owner, Variant, Status, Environment string
		Plan                                    json.RawMessage
	}
	results := []result{}
	for _, c := range catalog.Cases {
		if !strings.Contains(c.SQL, "bounds.row_count") {
			continue
		}
		args := append([]any{}, c.Args...)
		for i, v := range args {
			if n, ok := v.(json.Number); ok {
				args[i], err = n.Int64()
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		q := usageLogsSyntheticBind(t, c.SQL, args)
		plan := run("USE sql_audit_synthetic; EXPLAIN FORMAT=JSON " + q + ";")
		if !json.Valid([]byte(plan)) {
			t.Fatal("invalid plan JSON")
		}
		results = append(results, result{c.ID, c.Owner, c.Variant, "explained", "isolated_mysql_8.4_synthetic_not_replica", json.RawMessage(plan)})
	}
	if len(results) != 13 {
		t.Fatalf("derived cases changed: %d", len(results))
	}
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../docs/sql-audit-20261126/mysql-plans-derived-isolated.json", append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log(fmt.Sprintf("isolated derived plans=%d; no production source used", len(results)))
}
