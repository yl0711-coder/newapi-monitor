package monitor

import (
	"context"
	"time"

	"gorm.io/gorm"
)

const infraAssetPageSize = 200

type infraAssetPage struct {
	Active, Archived         []InfraAsset
	ActiveNext, ArchivedNext string
}

// Separate keyset cursors keep both active and archived directories operable
// regardless of historical task volume. Removed identities remain tombstones.
func (m *Monitor) infraAssetPage(ctx context.Context, activeAfter, archivedAfter string) (infraAssetPage, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var page infraAssetPage
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		read := func(state, after string) ([]InfraAsset, string, error) {
			var rows []InfraAsset
			query := m.monitorOwnedInfraAssetsQuery(tx)
			if err := query.Where("state = ? AND id > ?", state, after).Order("id").Limit(infraAssetPageSize + 1).Find(&rows).Error; err != nil {
				return nil, "", err
			}
			next := ""
			if len(rows) > infraAssetPageSize {
				rows = rows[:infraAssetPageSize]
				next = rows[len(rows)-1].ID
			}
			scopes := map[string]bool{}
			for _, a := range rows {
				if a.CloudState == "missing" && a.Scope != "" {
					scopes[a.Scope] = true
				}
			}
			if len(scopes) > 0 {
				keys := make([]string, 0, len(scopes))
				for scope := range scopes {
					keys = append(keys, scope)
				}
				var watermarks []InfraAssetScope
				if err := tx.Where("scope IN ?", keys).Find(&watermarks).Error; err != nil {
					return nil, "", err
				}
				checked := map[string]int64{}
				for _, s := range watermarks {
					checked[s.Scope] = s.LastChecked
				}
				for i := range rows {
					if rows[i].CloudState == "missing" {
						rows[i].LastChecked = max(rows[i].LastChecked, checked[rows[i].Scope])
					}
				}
			}
			return rows, next, nil
		}
		var err error
		page.Active, page.ActiveNext, err = read("active", activeAfter)
		if err != nil {
			return err
		}
		page.Archived, page.ArchivedNext, err = read("archived", archivedAfter)
		return err
	})
	return page, err
}
