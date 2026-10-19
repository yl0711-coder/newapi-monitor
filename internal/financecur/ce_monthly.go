package financecur

// CE monthly evidence is deliberately NOT a CUR Artifact or TimeBucket. These
// account totals are UTC billing periods, not Shanghai operational days. This
// reader never fills a CUR gap or changes an operating-profit statement.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"sort"
	"strings"
	"time"
)

const maxCEEvidenceBytes = 8 << 20

type CEMonthAmount struct {
	Name string `json:"name"`
	USD  string `json:"usd"`
}

type CEMonthEvidence struct {
	Period       string          `json:"period"`
	FromUTC      string          `json:"from_utc"`
	ToUTC        string          `json:"to_utc_exclusive"`
	NetUSD       string          `json:"net_usd"`
	Estimated    bool            `json:"estimated"`
	VerifiedDays int             `json:"verified_days"`
	Services     []CEMonthAmount `json:"services"`
	RecordTypes  []CEMonthAmount `json:"record_types"`
}

type CEMonthlyEvidence struct {
	Kind         string            `json:"kind"`
	TimeZone     string            `json:"time_zone"`
	Currency     string            `json:"currency"`
	Metric       string            `json:"metric"`
	SourceSHA256 map[string]string `json:"source_sha256"`
	Months       []CEMonthEvidence `json:"months"`
}

type ceMetric struct{ Amount, Unit string }
type cePeriod struct{ Start, End string }
type ceGroup struct {
	Keys    []string
	Metrics map[string]ceMetric
}
type ceResult struct {
	TimePeriod cePeriod
	Total      map[string]ceMetric
	Groups     []ceGroup
	Estimated  *bool
}
type ceResponse struct {
	GroupDefinitions         []struct{ Type, Key string }
	ResultsByTime            []ceResult
	DimensionValueAttributes json.RawMessage
	NextPageToken            string
}

// ReconcileCEMonths verifies every requested month by three independent AWS
// result shapes. Inputs must be trusted saved GetCostAndUsage responses: their
// hashes establish content identity, not AWS authenticity or account identity.
// Estimated=false is preserved but is NOT treated as invoice-finalization proof.
func ReconcileCEMonths(services, records, daily io.Reader, fromMonth, toMonthExclusive string) (CEMonthlyEvidence, error) {
	fail := func(err error) (CEMonthlyEvidence, error) { return CEMonthlyEvidence{}, err }
	from, err := time.Parse("2006-01", fromMonth)
	if err != nil || from.Format("2006-01") != fromMonth {
		return fail(fmt.Errorf("invalid starting CE month"))
	}
	to, err := time.Parse("2006-01", toMonthExclusive)
	if err != nil || to.Format("2006-01") != toMonthExclusive || !from.Before(to) || to.After(from.AddDate(3, 0, 0)) {
		return fail(fmt.Errorf("CE range must contain 1 to 36 whole months"))
	}
	out := CEMonthlyEvidence{Kind: "aws_ce_monthly_evidence", TimeZone: "UTC", Currency: "USD", Metric: "NetUnblendedCost", SourceSHA256: map[string]string{}, Months: []CEMonthEvidence{}}
	inputs := []struct {
		name, dimension string
		reader          io.Reader
	}{{"services", "SERVICE", services}, {"record_types", "RECORD_TYPE", records}, {"daily", "", daily}}
	maps := make([]map[string]ceResult, 0, 3)
	for _, in := range inputs {
		response, hash, err := readCEEvidence(in.reader)
		if err != nil {
			return fail(fmt.Errorf("%s: %w", in.name, err))
		}
		rows, err := validateCEResponse(response, in.dimension)
		if err != nil {
			return fail(fmt.Errorf("%s: %w", in.name, err))
		}
		maps = append(maps, rows)
		out.SourceSHA256[in.name] = hash
	}
	for month := from; month.Before(to); month = month.AddDate(0, 1, 0) {
		start, end := month.Format("2006-01-02"), month.AddDate(0, 1, 0).Format("2006-01-02")
		s, ok := maps[0][start]
		if !ok {
			return fail(fmt.Errorf("%s: service month missing", start))
		}
		r, ok := maps[1][start]
		if !ok {
			return fail(fmt.Errorf("%s: record-type month missing", start))
		}
		serviceRows, serviceSum, err := ceGroupedAmounts(s)
		if err != nil {
			return fail(err)
		}
		recordRows, recordSum, err := ceGroupedAmounts(r)
		if err != nil {
			return fail(err)
		}
		if serviceSum.Cmp(recordSum) != 0 {
			return fail(fmt.Errorf("%s: service and record-type totals disagree", start))
		}
		dailySum := new(big.Int)
		estimated := *s.Estimated || *r.Estimated
		days := 0
		for day := month; day.Before(month.AddDate(0, 1, 0)); day = day.AddDate(0, 0, 1) {
			date := day.Format("2006-01-02")
			d, ok := maps[2][date]
			if !ok {
				return fail(fmt.Errorf("%s: daily evidence missing", date))
			}
			n, err := ceNetAmount(d.Total)
			if err != nil {
				return fail(err)
			}
			dailySum.Add(dailySum, n)
			estimated = estimated || *d.Estimated
			days++
		}
		if dailySum.Cmp(serviceSum) != 0 {
			return fail(fmt.Errorf("%s: daily and monthly totals disagree", start))
		}
		out.Months = append(out.Months, CEMonthEvidence{Period: month.Format("2006-01"), FromUTC: start, ToUTC: end, NetUSD: ceDecimalString(serviceSum), Estimated: estimated, VerifiedDays: days, Services: serviceRows, RecordTypes: recordRows})
	}
	return out, nil
}

func readCEEvidence(reader io.Reader) (ceResponse, string, error) {
	if reader == nil {
		return ceResponse{}, "", fmt.Errorf("nil CE source")
	}
	b, err := io.ReadAll(io.LimitReader(reader, maxCEEvidenceBytes+1))
	if err != nil {
		return ceResponse{}, "", err
	}
	if len(b) > maxCEEvidenceBytes {
		return ceResponse{}, "", fmt.Errorf("CE source exceeds size limit")
	}
	// JSON decoding otherwise silently accepts duplicate keys and last-wins money.
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := uniqueCEJSONValue(d, 0); err != nil {
		return ceResponse{}, "", err
	}
	if _, err := d.Token(); err != io.EOF {
		return ceResponse{}, "", fmt.Errorf("CE source has trailing data")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var response ceResponse
	if err := d.Decode(&response); err != nil {
		return ceResponse{}, "", fmt.Errorf("invalid CE shape: %w", err)
	}
	if response.NextPageToken != "" {
		return ceResponse{}, "", fmt.Errorf("CE pagination incomplete")
	}
	h := sha256.Sum256(b)
	return response, hex.EncodeToString(h[:]), nil
}

func uniqueCEJSONValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return fmt.Errorf("CE JSON nesting exceeds limit")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			canonical := strings.ToLower(name)
			if !ok || seen[canonical] {
				return fmt.Errorf("duplicate or invalid CE JSON key")
			}
			seen[canonical] = true
			if err := uniqueCEJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueCEJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected CE JSON delimiter")
	}
	_, err = d.Token()
	return err
}

func validateCEResponse(response ceResponse, dimension string) (map[string]ceResult, error) {
	if dimension == "" {
		if len(response.GroupDefinitions) != 0 {
			return nil, fmt.Errorf("daily source must be ungrouped")
		}
	} else if len(response.GroupDefinitions) != 1 || response.GroupDefinitions[0].Type != "DIMENSION" || response.GroupDefinitions[0].Key != dimension {
		return nil, fmt.Errorf("wrong CE grouping")
	}
	if len(response.ResultsByTime) == 0 || len(response.ResultsByTime) > 36*31 {
		return nil, fmt.Errorf("invalid CE period count")
	}
	rows := map[string]ceResult{}
	for _, row := range response.ResultsByTime {
		start, err := time.Parse("2006-01-02", row.TimePeriod.Start)
		if err != nil {
			return nil, fmt.Errorf("invalid CE start")
		}
		end, err := time.Parse("2006-01-02", row.TimePeriod.End)
		if err != nil {
			return nil, fmt.Errorf("invalid CE end")
		}
		if row.Estimated == nil {
			return nil, fmt.Errorf("CE estimated flag missing")
		}
		if _, ok := rows[row.TimePeriod.Start]; ok {
			return nil, fmt.Errorf("duplicate CE period")
		}
		if dimension == "" {
			if !end.Equal(start.AddDate(0, 0, 1)) || len(row.Groups) != 0 {
				return nil, fmt.Errorf("invalid CE daily shape")
			}
			if _, err := ceNetAmount(row.Total); err != nil {
				return nil, err
			}
		} else {
			if start.Day() != 1 || !end.Equal(start.AddDate(0, 1, 0)) || len(row.Total) != 0 {
				return nil, fmt.Errorf("invalid CE monthly shape")
			}
			if _, _, err := ceGroupedAmounts(row); err != nil {
				return nil, err
			}
		}
		rows[row.TimePeriod.Start] = row
	}
	return rows, nil
}

func ceGroupedAmounts(row ceResult) ([]CEMonthAmount, *big.Int, error) {
	if len(row.Groups) == 0 {
		return nil, nil, fmt.Errorf("CE groups missing")
	}
	seen := map[string]bool{}
	sum := new(big.Int)
	out := []CEMonthAmount{}
	for _, g := range row.Groups {
		if len(g.Keys) != 1 || strings.TrimSpace(g.Keys[0]) == "" || seen[g.Keys[0]] {
			return nil, nil, fmt.Errorf("duplicate or invalid CE group")
		}
		seen[g.Keys[0]] = true
		n, err := ceNetAmount(g.Metrics)
		if err != nil {
			return nil, nil, err
		}
		sum.Add(sum, n)
		out = append(out, CEMonthAmount{Name: g.Keys[0], USD: ceDecimalString(n)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, sum, nil
}

var ceAmountPattern = regexp.MustCompile(`^-?[0-9]{1,12}(?:\.[0-9]{1,18})?$`)
var ceScale = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

func ceNetAmount(metrics map[string]ceMetric) (*big.Int, error) {
	m, ok := metrics["NetUnblendedCost"]
	if !ok || m.Unit != "USD" || !ceAmountPattern.MatchString(m.Amount) {
		return nil, fmt.Errorf("missing or invalid USD NetUnblendedCost")
	}
	s := strings.TrimPrefix(m.Amount, "-")
	parts := strings.SplitN(s, ".", 2)
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	n, ok := new(big.Int).SetString(parts[0]+fraction+strings.Repeat("0", 18-len(fraction)), 10)
	if !ok {
		return nil, fmt.Errorf("invalid CE amount")
	}
	if strings.HasPrefix(m.Amount, "-") {
		n.Neg(n)
	}
	return n, nil
}
func ceDecimalString(n *big.Int) string {
	sign := ""
	if n.Sign() < 0 {
		sign = "-"
	}
	abs := new(big.Int).Abs(n)
	whole, rem := new(big.Int), new(big.Int)
	whole.QuoRem(abs, ceScale, rem)
	fraction := strings.TrimRight(strings.Repeat("0", 18-len(rem.String()))+rem.String(), "0")
	if fraction != "" {
		return sign + whole.String() + "." + fraction
	}
	return sign + whole.String()
}
