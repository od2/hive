// Package redisdedup implements set membership checks
// for deduplicating large sets of items.
package redisdedup

import "context"

// Set provides set membership checks.
//
// AddItems inserts items into the set.
// DedupItems returns a copy of the items slice without already seen.
type Set interface {
	AddItems(context.Context, []string) error
	DedupItems(context.Context, []string) ([]string, error)
}
