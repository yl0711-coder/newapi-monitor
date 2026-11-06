package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

type sourceDiagnosticNetworkError struct{ timeout bool }

func (e sourceDiagnosticNetworkError) Error() string   { return "private host, password, SQL and token" }
func (e sourceDiagnosticNetworkError) Timeout() bool   { return e.timeout }
func (e sourceDiagnosticNetworkError) Temporary() bool { return false }

func TestSourceFailureClassUsesOnlyTypedSafeCauses(t *testing.T) {
	const private = "private-user:private-password@private-host/private-database SELECT private_column"
	for _, tc := range []struct {
		name, class string
		err         error
	}{
		{"deadline", "deadline", context.DeadlineExceeded},
		{"cancel", "canceled", context.Canceled},
		{"sql_limit", "sql_execution_budget", &mysql.MySQLError{Number: 3024, Message: private}},
		{"sql_permission", "mysql_error", &mysql.MySQLError{Number: 1142, Message: private}},
		{"sql_connections", "mysql_error", &mysql.MySQLError{Number: 1040, Message: private}},
		{"bad_connection", "invalid_connection", driver.ErrBadConn},
		{"mysql_connection", "invalid_connection", mysql.ErrInvalidConn},
		{"eof", "connection_closed", io.EOF},
		{"short_read", "connection_closed", io.ErrUnexpectedEOF},
		{"network_timeout", "network_timeout", &net.OpError{Op: private, Net: private, Err: sourceDiagnosticNetworkError{true}}},
		{"network_other", "network_error", &net.OpError{Op: private, Net: private, Err: sourceDiagnosticNetworkError{false}}},
		{"untyped_driver_text", "connection_error", errors.New("network_timeout mysql_error_1142 " + private)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("%s: %w", private, tc.err)
			if got := safeSourceFailureClass(wrapped, "connection_error"); got != tc.class {
				t.Fatal("unexpected typed error class", got, tc.class)
			}
			failure := &observationTransportFailure{cause: wrapped}
			text := safeError(failure)
			if !errors.Is(failure, tc.err) || !strings.Contains(text, "stage=transport_prepare, class="+tc.class) ||
				!strings.Contains(text, "未开始业务查询") || strings.Contains(text, "private") || strings.Contains(text, "SELECT") {
				t.Fatal("safe output leaked values or changed original failure semantics")
			}
			parsed := regexp.MustCompile(`class=([a-z_]+)`).FindStringSubmatch(text)
			if len(parsed) != 2 || parsed[1] != tc.class {
				t.Fatal("diagnostic broke existing fixed-class operator parser")
			}
			var sqlError *mysql.MySQLError
			if tc.class == "mysql_error" && errors.As(tc.err, &sqlError) {
				if !strings.Contains(text, ", mysql_errno="+strconv.FormatUint(uint64(sqlError.Number), 10)) {
					t.Fatal("safe numeric MySQL cause missing")
				}
			} else if strings.Contains(text, "mysql_errno=") {
				t.Fatal("untyped or canceled failure acquired an unrelated SQL errno")
			}
		})
	}
	if got := safeSourceFailureClass(errors.New(private), "source_or_validation_error"); got != "source_or_validation_error" {
		t.Fatal("untyped text became a trusted source error")
	}
}

func TestSourceFailureClassKeepsCancellationPrecedence(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{context.DeadlineExceeded, "deadline"},
		{context.Canceled, "canceled"},
	} {
		err := errors.Join(&mysql.MySQLError{Number: 1142, Message: "private"}, io.EOF, sourceDiagnosticNetworkError{true}, tc.err)
		if got := safeSourceFailureClass(err, "source_or_validation_error"); got != tc.want {
			t.Fatal("typed connection error concealed cancellation", got)
		}
	}
}
