package usagelogs

import (
	"errors"
	"strings"
)

// Filter holds the search criteria shared by the list, count, and statistics queries.
type Filter struct {
	Type              int
	Start             int64
	End               int64
	ModelName         string
	Username          string
	TokenName         string
	Group             string
	RequestID         string
	UpstreamRequestID string
	Channel           int
	// userID is never parsed from the request. It is set only by
	// usageLogsUsernameUserID once an exact username is proven equivalent to
	// one user_id, so COUNT/SUM can use the (user_id, created_at, type) index.
	userID int64
}

// usageLogMatch returns an exact-match clause, or a LIKE clause when the value
// carries explicit % wildcards (username / model name only).
func usageLogMatch(column, value string) (string, any, error) {
	if !strings.Contains(value, "%") {
		return column + " = ?", value, nil
	}
	if strings.Contains(value, "%%") {
		return "", nil, errors.New("搜索模式中不允许包含连续的 % 通配符")
	}
	if strings.Count(value, "%") > 2 {
		return "", nil, errors.New("搜索模式中最多允许包含 2 个 % 通配符")
	}
	if len(strings.ReplaceAll(value, "%", "")) < 2 {
		return "", nil, errors.New("使用模糊搜索时，关键词长度至少为 2 个字符")
	}
	escaped := strings.NewReplacer("!", "!!", "_", "!_").Replace(value)
	return column + " LIKE ? ESCAPE '!'", escaped, nil
}

type usageLogWhere struct {
	clauses []string
	args    []any
}

func (w *usageLogWhere) add(clause string, args ...any) {
	w.clauses = append(w.clauses, clause)
	w.args = append(w.args, args...)
}

func (w *usageLogWhere) sql() string {
	if len(w.clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(w.clauses, " AND ")
}

type usageLogWhereScope int

const (
	usageLogWhereList usageLogWhereScope = iota // every filter
	usageLogWhereStat                           // type forced to consume; ids ignored
	usageLogWhereRate                           // as stat, but the caller adds its own time window
)

func buildUsageLogWhere(f Filter, scope usageLogWhereScope) (*usageLogWhere, error) {
	w := &usageLogWhere{}
	switch scope {
	case usageLogWhereList:
		if f.Type != 0 {
			w.add("type = ?", f.Type)
		}
	default:
		w.add("type = ?", 2)
	}
	if f.ModelName != "" {
		clause, arg, err := usageLogMatch("model_name", f.ModelName)
		if err != nil {
			return nil, err
		}
		w.add(clause, arg)
	}
	if f.userID > 0 {
		w.add("user_id = ?", f.userID)
	} else if f.Username != "" {
		clause, arg, err := usageLogMatch("username", f.Username)
		if err != nil {
			return nil, err
		}
		w.add(clause, arg)
	}
	if f.TokenName != "" {
		w.add("token_name = ?", f.TokenName)
	}
	if scope != usageLogWhereRate {
		if f.Start != 0 {
			w.add("created_at >= ?", f.Start)
		}
		if f.End != 0 {
			w.add("created_at <= ?", f.End)
		}
	}
	if f.Channel != 0 {
		w.add("channel_id = ?", f.Channel)
	}
	if f.Group != "" {
		w.add("`group` = ?", f.Group)
	}
	if scope == usageLogWhereList {
		if f.RequestID != "" {
			w.add("request_id = ?", f.RequestID)
		}
		if f.UpstreamRequestID != "" {
			w.add("upstream_request_id = ?", f.UpstreamRequestID)
		}
	}
	return w, nil
}
