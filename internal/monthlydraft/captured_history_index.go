package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// PRIVATE server source catalogue, not a bill or a customer DTO. Entries pin
// existing history only; no editable money, wallet flags or approval fields.
type CapturedHistoryIndex struct {
	Version int                         `json:"version"`
	Entries []CapturedHistoryIndexEntry `json:"entries"`
}

type CapturedHistoryIndexEntry struct {
	GroupID int64                                `json:"group_id"`
	Month   string                               `json:"month"`
	History monthlysource.MonthlyObservationFile `json:"history"`
	Scope   monthlysource.MonthlyObservationFile `json:"scope"`
}

// NewCapturedHistoryFileLoader owns no resources and reads nothing until a
// background request. Pin a new catalogue version explicitly when server
// configuration changes; it never silently adopts an edited file or arbitrary
// customer paths. Consumers still verify full authority and original evidence.
func NewCapturedHistoryFileLoader(g *Generation, source monthlysource.MonthlyObservationFile) (CapturedHistoryLoader, error) {
	if g == nil || g.root == nil || !generationHex(source.SHA256, 32) {
		return nil, ErrGenerationInput
	}
	if _, err := g.relativeSource(source.Path); err != nil {
		return nil, err
	}
	return func(parent context.Context, req SourcePreparationRequest) (CapturedHistorySources, error) {
		if parent == nil {
			return CapturedHistorySources{}, ErrGenerationInput
		}
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		raw, err := g.readPrivatePreparationFile(ctx, source, 64<<10)
		if err != nil {
			return CapturedHistorySources{}, err
		}
		var index CapturedHistoryIndex
		if json.Unmarshal(raw, &index) != nil {
			return CapturedHistorySources{}, ErrGenerationInput
		}
		canonical, err := json.Marshal(index)
		if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) || index.Version != 1 || index.Entries == nil || len(index.Entries) > 32 {
			return CapturedHistorySources{}, ErrGenerationInput
		}
		seen := map[GenerationTarget]bool{}
		var selected CapturedHistorySources
		for _, entry := range index.Entries {
			target := GenerationTarget{GroupID: entry.GroupID, Month: entry.Month}
			if !validSelectionScope(entry.GroupID, entry.Month) || seen[target] {
				return CapturedHistorySources{}, ErrGenerationInput
			}
			seen[target] = true
			for _, ref := range []monthlysource.MonthlyObservationFile{entry.History, entry.Scope} {
				if !generationHex(ref.SHA256, 32) {
					return CapturedHistorySources{}, ErrGenerationInput
				}
				if _, err := g.relativeSource(ref.Path); err != nil {
					return CapturedHistorySources{}, err
				}
			}
			if entry.GroupID == req.Owner.GroupID && entry.Month == req.Month {
				selected = CapturedHistorySources{History: entry.History, Scope: entry.Scope}
			}
		}
		if selected.History.SHA256 == "" {
			return CapturedHistorySources{}, ErrGenerationSelectionMissing
		}
		// Do not load unrelated organizations' payloads/files. The chosen files
		// will be re-read and audited by the existing preparation transaction.
		if err := ctx.Err(); err != nil {
			return CapturedHistorySources{}, err
		}
		return selected, nil
	}, nil
}
