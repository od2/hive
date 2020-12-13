// Package redisdedup implements set membership checks
// for deduplicating large sets of items.
package redisdedup

import "context"

// Dedup provides set membership checks.
//
// AddItems inserts items into the set.
// DedupItems returns a copy of the items slice without already seen.
type Dedup interface {
	AddItems(context.Context, []Item) error
	DedupItems(context.Context, []Item) ([]Item, error)
}

// Item is an element in a relation that can be deduped.
type Item interface {
	DedupKey() string
}

// StringItem wraps string for use in Dedup.
type StringItem string

// DedupKey returns the string held by StrignItem.
func (s StringItem) DedupKey() string {
	return string(s)
}
