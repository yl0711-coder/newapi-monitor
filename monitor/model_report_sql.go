package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	sqlite "github.com/glebarez/go-sqlite"
	"gorm.io/gorm"
)

const modelReportRowFunction = "monitor_internal_report_row_v1"

type modelReportSQLScope struct{}

type modelReportRowSink struct {
	ctx     context.Context
	consume func([]driver.Value) error
}

// Register before the SQLite pool opens. QueryContext in the pinned driver
// stops its interrupt watcher before Rows.Next does the expensive first step.
// ExecContext keeps it alive during that step. An outer SUM forces all rows
// through this non-deterministic callback in that one step, without a temp
// table or a Go copy of the entire result. Only these report queries call it.
var modelReportSQLSinks struct {
	next atomic.Int64
	rows sync.Map
}

func init() {
	sqlite.MustRegisterScalarFunction(modelReportRowFunction, -1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		if len(args) == 0 {
			return nil, errors.New("missing model report sink")
		}
		id, ok := args[0].(int64)
		if !ok {
			return nil, errors.New("invalid model report sink")
		}
		active, ok := modelReportSQLSinks.rows.Load(id)
		if !ok {
			return nil, errors.New("expired model report sink")
		}
		sink := active.(modelReportRowSink)
		if err := sink.ctx.Err(); err != nil {
			return nil, err
		}
		return int64(1), sink.consume(args[1:])
	})
}

func modelReportSQLScoped(ctx context.Context) bool {
	return ctx.Value(modelReportSQLScope{}) != nil
}

// columns and query are static, internal SQL. User filters are only passed
// as bound arguments. Exec and the synchronous callbacks finish before the
// caller releases the single heavy-report slot or the SQLite transaction.
func consumeModelReportSQL(ctx context.Context, db *gorm.DB, columns, query string, args []any, consume func([]driver.Value) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id := modelReportSQLSinks.next.Add(1)
	modelReportSQLSinks.rows.Store(id, modelReportRowSink{ctx: ctx, consume: consume})
	defer modelReportSQLSinks.rows.Delete(id)
	q := "SELECT SUM(" + modelReportRowFunction + "(?, " + strings.TrimSpace(columns) + ")) FROM (" + query + ") AS report_rows"
	params := append([]any{id}, args...)
	err := db.WithContext(ctx).Exec(q, params...).Error
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return err
}

func scanModelReportValues(values []driver.Value, fields ...any) error {
	if len(values) != len(fields) {
		return errors.New("invalid model report row shape")
	}
	for i, value := range values {
		switch dst := fields[i].(type) {
		case *int64:
			n, err := modelReportSQLInt(value)
			if err != nil {
				return err
			}
			*dst = n
		case *int:
			n, err := modelReportSQLInt(value)
			if err != nil || int64(int(n)) != n {
				return errors.New("invalid model report integer")
			}
			*dst = int(n)
		case *string:
			switch v := value.(type) {
			case nil:
				*dst = ""
			case string:
				*dst = v
			case []byte:
				*dst = string(v)
			default:
				var s sql.NullString
				if err := s.Scan(value); err != nil {
					return err
				}
				*dst = s.String
			}
		default:
			return errors.New("invalid model report destination")
		}
	}
	return nil
}

func modelReportSQLInt(value driver.Value) (int64, error) {
	if value == nil {
		return 0, nil
	}
	if v, ok := value.(int64); ok {
		return v, nil
	}
	var v sql.NullInt64
	err := v.Scan(value)
	return v.Int64, err
}
