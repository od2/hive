package dedup

import (
	"context"

	"go.od2.network/hive/pkg/items"
)

// SQL implements Dedup using the items table.
type SQL struct {
	Store *items.Store
}

// AddItems does nothing. (Because items are inserted to the items table before Dedup)
func (s *SQL) AddItems(_ context.Context, _ []Item) error {
	return nil
}

// DedupItems returns a copy of the provided items slice except things present in the bitmap.
// The output won't contain any duplicate, even in the case of multiple items.
func (s *SQL) DedupItems(ctx context.Context, items []Item) ([]Item, error) {
	itemIDs := make([]string, len(items))
	itemMap := make(map[string]Item)
	for i, item := range items {
		key := item.DedupKey()
		itemIDs[i] = key
		itemMap[key] = item
	}
	newItemIDs, err := s.Store.FilterNewPointers(ctx, itemIDs)
	if err != nil {
		return nil, err
	}
	deduped := make([]Item, len(newItemIDs))
	for i, itemID := range newItemIDs {
		deduped[i] = itemMap[itemID]
	}
	return deduped, nil
}
