package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

var errCustomerHealthClassificationBudget = errors.New("customer health classification budget exhausted")
var errCustomerHealthClassificationChanged = errors.New("customer health source changed during classification")

const customerHealthClassificationRows = 2048
const customerHealthClassificationBytes = 8 << 20

type customerHealthClassificationKey struct {
	Bucket, User, Channel int64
	Model, Group          string
}

// This is an admin-only projection of the same source evidence that the
// troubleshooting list uses. It performs no enrichment or external queries.
func customerHealthDiagnosticRow(r LogChainRow, other string) LogChainRow {
	if o := parseLogOther(other); o != nil {
		r.UpstreamErrorCode, r.UpstreamErrorType, r.UpstreamStatusCode = o.ErrorCode, o.ErrorType, o.UpstreamStatusCode
		r.RequestPath, r.IsModelMapped, r.UpstreamModelName = o.RequestPath, o.IsModelMapped, o.UpstreamModelName
	}
	r.DiagnosticFacts = logChainParseDiagnosticFacts(r.Content, other, "logs", r.CreatedAt)
	r.DiagnosticFacts = logChainRowDiagnosticFacts(r)
	return r
}

func customerHealthDiagnosticFailure(r LogChainRow) (bool, logChainFault) {
	f := logChainAttributeFaultWithEvidence(r, r.AnomalyTags)
	// The stability formula still excludes only wholly customer-attributed
	// errors. Combined, unknown, platform, upstream and network stay counted.
	return f.Fault != faultDownstream, f
}

func customerHealthClassificationSQL() string {
	return `SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,created_at,user_id,COALESCE(channel_id,0),COALESCE(model_name,''),COALESCE(` + "`group`" + `,''),
	COALESCE(request_id,''),SUBSTR(COALESCE(content,''),1,16385),SUBSTR(COALESCE(other,''),1,32769)
	FROM logs WHERE created_at>=? AND created_at<? AND type=5 AND NOT (` + channelTestLogPredicateSQL() + `)
	ORDER BY created_at,id LIMIT 2049`
}

// The source is append-only. Match every minute/dimension's observed error
// count before publishing the revised attribution; concurrent late arrivals,
// truncation or a read failure cannot certify a partial classification.
// The existing aggregate and these bounded error reads share one source slot
// and the original overall deadline. General metric columns are untouched.
func classifyCustomerHealthErrors(ctx context.Context, db *sql.DB, from, through int64, batch []CapacityUserMinuteSample) error {
	expected := map[customerHealthClassificationKey]int64{}
	var total int64
	for _, r := range batch {
		k := customerHealthClassificationKey{r.BucketTs, r.UserID, int64(r.ChannelID), r.ModelName, r.Grp}
		expected[k] = r.Failed
		total += r.Failed
	}
	if total > customerHealthClassificationRows {
		return errCustomerHealthClassificationBudget
	}
	actual, failed := map[customerHealthClassificationKey]int64{}, map[customerHealthClassificationKey]int64{}
	if total > 0 {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		rows, err := db.QueryContext(ctx, customerHealthClassificationSQL(), from, through)
		if err != nil {
			return err
		}
		defer rows.Close()
		read, bytes := 0, 0
		for rows.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var r LogChainRow
			var other string
			r.Type = 5
			if err := rows.Scan(&r.ID, &r.CreatedAt, &r.UserID, &r.ChannelID, &r.ModelName, &r.Group, &r.RequestID, &r.Content, &other); err != nil {
				return err
			}
			read++
			bytes += len(r.Content) + len(other)
			if read > customerHealthClassificationRows || bytes > customerHealthClassificationBytes || len(r.Content) > 16384 || len(other) > 32768 {
				return errCustomerHealthClassificationBudget
			}
			k := customerHealthClassificationKey{r.CreatedAt / 60 * 60, r.UserID, r.ChannelID, r.ModelName, r.Group}
			actual[k]++
			if actual[k] > expected[k] {
				return errCustomerHealthClassificationChanged
			}
			if count, _ := customerHealthDiagnosticFailure(customerHealthDiagnosticRow(r, other)); count {
				failed[k]++
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	for k, want := range expected {
		if actual[k] != want {
			return fmt.Errorf("%w: expected %d, read %d", errCustomerHealthClassificationChanged, want, actual[k])
		}
	}
	for i := range batch {
		r := &batch[i]
		k := customerHealthClassificationKey{r.BucketTs, r.UserID, int64(r.ChannelID), r.ModelName, r.Grp}
		r.CustomerHealthFailed = failed[k]
		r.CustomerHealthVersion = customerHealthStabilityPolicyVersion
	}
	return nil
}

// A general sampler must not erase a complete customer-health projection just
// because the optional diagnostic read yielded. Keep its classification only
// if the append-only error population is unchanged; new errors invalidate it.
func upsertCustomerHealthDeferredSamples(db *gorm.DB, rows []CapacityUserMinuteSample) error {
	if len(rows) == 0 {
		return nil
	}
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(&CapacityUserMinuteSample{}); err != nil {
		return err
	}
	updates := map[string]any{}
	for _, f := range stmt.Schema.Fields {
		if f.DBName == "" || f.PrimaryKey {
			continue
		}
		updates[f.DBName] = clause.Column{Table: "excluded", Name: f.DBName}
	}
	for _, name := range []string{"customer_health_failed", "customer_health_version"} {
		updates[name] = gorm.Expr("CASE WHEN " + stmt.Table + ".failed=excluded.failed THEN " + stmt.Table + "." + name + " ELSE 0 END")
	}
	return db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "bucket_ts"}, {Name: "user_id"}, {Name: "channel_id"}, {Name: "model_name"}, {Name: "grp"}}, DoUpdates: clause.Assignments(updates)}).CreateInBatches(rows, 200).Error
}
