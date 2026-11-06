package monitor

import (
	"math/big"
	"sort"
)

type monthlyCentScalar struct {
	ID     int64  `json:"id"`
	Amount string `json:"amount"`
}

// One signed column, with exact parent/child originals. This is used for
// presentation of opening balances and money movements, never actual transfers.
// A parent's assigned cents can be passed to its children without rerounding it.
func monthlyCentSplit(total, target string, rows []monthlyCentScalar) ([]monthlyCentScalar, error) {
	if len(rows) > monthlyCreditLimit {
		return nil, errMonthlyCentAllocation
	}
	original, err := monthlyCentParse(total)
	if err != nil {
		return nil, err
	}
	wanted, err := monthlyCentTarget(original, target)
	if err != nil {
		return nil, err
	}
	type part struct {
		id        int64
		floor     *big.Int
		remainder int64
	}
	parts := make([]part, len(rows))
	sum, floors := new(big.Int), new(big.Int)
	seen := make(map[int64]bool, len(rows))
	for i, row := range rows {
		if row.ID < 0 || row.ID > 9007199254740991 || seen[row.ID] {
			return nil, errMonthlyCentAllocation
		}
		seen[row.ID] = true
		value, err := monthlyCentParse(row.Amount)
		if err != nil {
			return nil, err
		}
		floor, remainder := monthlyCentBounds(value)
		parts[i] = part{row.ID, floor, remainder}
		sum.Add(sum, value)
		floors.Add(floors, floor)
	}
	if sum.Cmp(original) != 0 {
		return nil, errMonthlyCentAllocation
	}
	needed := new(big.Int).Sub(wanted, floors)
	if !needed.IsInt64() || needed.Sign() < 0 || needed.Int64() > int64(len(rows)) {
		return nil, errMonthlyCentAllocation
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].remainder != parts[j].remainder {
			return parts[i].remainder > parts[j].remainder
		}
		return parts[i].id < parts[j].id
	})
	for i := range int(needed.Int64()) {
		if parts[i].remainder == 0 {
			return nil, errMonthlyCentAllocation
		}
		parts[i].floor.Add(parts[i].floor, big.NewInt(1))
	}
	result := make([]monthlyCentScalar, len(parts))
	for i, part := range parts {
		result[i] = monthlyCentScalar{ID: part.id, Amount: monthlyDisplayCentsMoney(part.floor)}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func monthlyCentSplitDefault(total string, rows []monthlyCentScalar) ([]monthlyCentScalar, error) {
	n, ok := monthlyRoundedDisplayUnits(total, 2)
	if !ok {
		return nil, errMonthlyCentAllocation
	}
	return monthlyCentSplit(total, monthlyDisplayCentsMoney(n), rows)
}
