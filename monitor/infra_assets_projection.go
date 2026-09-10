package monitor

import (
	"context"
	"errors"
	"time"
)

// Prefer the newest cloud incarnation, not the display name or an old host
// alias. Older incarnations remain visible in the management/archive list.
func currentInfraAssets(assets []InfraAsset) (map[string]InfraAsset, map[string]int) {
	current, counts := map[string]InfraAsset{}, map[string]int{}
	for _, a := range assets {
		if a.Kind == "ecs_task" {
			continue
		}
		if a.Kind != "host" {
			counts[a.Resource]++
		}
		previous, exists := current[a.Resource]
		if !exists || previous.Kind == "host" && a.Kind != "host" || previous.Kind == a.Kind && (a.FirstSeen > previous.FirstSeen || a.FirstSeen == previous.FirstSeen && a.Identity > previous.Identity) {
			current[a.Resource] = a
		}
	}
	return current, counts
}

func (m *Monitor) infraAssetProjection() (map[string]InfraAsset, map[string]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var assets []InfraAsset
	err := m.storeDB.WithContext(ctx).Where("kind <> ? AND state <> ?", "ecs_task", "linked").Order("first_seen DESC, id").Limit(infraAssetLimit + 1).Find(&assets).Error
	if err != nil {
		return nil, nil, err
	}
	if len(assets) > infraAssetLimit {
		return nil, nil, errors.New("resource projection budget exceeded")
	}
	current, counts := currentInfraAssets(assets)
	return current, counts, nil
}

func visibleInfraAsset(a InfraAsset, exists bool) bool { return !exists || a.State == "active" }
