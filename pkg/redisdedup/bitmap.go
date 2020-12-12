package redisdedup

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/go-redis/redis/v8"
)

// BitMap uses Redis BITFIELD to implement Set.
//
// Items must be numbers, or else operations fail.
//
// This works best with incremental IDs.
// The number of bits required is equal to the highest ID.
//
// To work with large bitmaps, the set is split across multiple keys.
// The Exp parameter controls the size of a bitmap.
// Each bitmap holds (2^Exp) items.
type BitMap struct {
	Redis  *redis.Client
	Prefix string
	Exp    uint
}

// AddItems flips bits in the set to one.
func (b *BitMap) AddItems(ctx context.Context, items []string) error {
	keys := make([]uint64, 0, len(items))
	for _, item := range items {
		num, err := strconv.ParseUint(item, 10, 64)
		if err != nil {
			return fmt.Errorf("item is not a number: \"%s\"", item)
		}
		keys = append(keys, num)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	pipe := b.Redis.Pipeline()
	defer pipe.Close()
	insert := bitMapInsert{
		b:    b,
		ctx:  ctx,
		pipe: pipe,
	}
	for _, key := range keys {
		if err := insert.insert(key); err != nil {
			return err
		}
	}
	if err := insert.flush(); err != nil {
		return err
	}
	_, err := pipe.Exec(ctx)
	return err
}

type bitMapInsert struct {
	b      *BitMap
	ctx    context.Context
	pipe   redis.Pipeliner
	bucket uint64
	batch  []interface{}
}

func (i *bitMapInsert) insert(key uint64) error {
	bucket := key >> i.b.Exp
	if bucket != i.bucket {
		if err := i.flush(); err != nil {
			return err
		}
		i.bucket = bucket
	}
	mask := (uint64(1) << i.b.Exp) - 1
	i.batch = append(i.batch, "SET", "u1", key&mask, "1")
	return nil
}

func (i *bitMapInsert) flush() error {
	redisKey := fmt.Sprintf("%s-%x", i.b.Prefix, i.bucket)
	i.b.Redis.BitField(i.ctx, redisKey, i.batch...)
	i.bucket = 0
	i.batch = nil
	return nil
}

// DedupItems returns a copy of the provided items slice except things present in the bitmap.
// The output won't contain any duplicate, even in the case of multiple items.
func (b *BitMap) DedupItems(ctx context.Context, items []string) ([]string, error) {
	// Parse the keys from strings.
	sortedKeys := make([]uint64, len(items))
	for i, item := range items {
		num, err := strconv.ParseUint(item, 10, 64)
		if err != nil {
			return items, fmt.Errorf("item is not a number: \"%s\"", item)
		}
		sortedKeys[i] = num
	}
	// Sort the keys.
	sort.Slice(sortedKeys, func(i, j int) bool {
		return sortedKeys[i] < sortedKeys[j]
	})
	// Deduplicate the keys.
	var lastKey uint64
	keys := make([]uint64, 0, len(items))
	for i, k := range sortedKeys {
		if i > 0 && lastKey == k {
			continue
		}
		keys = append(keys, k)
		lastKey = k
	}
	// Execute batch lookups against Redis in a pipeline.
	pipe := b.Redis.Pipeline()
	defer pipe.Close()
	lookup := bitMapLookup{
		b:    b,
		ctx:  ctx,
		pipe: pipe,
	}
	for _, key := range keys {
		if err := lookup.lookup(key); err != nil {
			return nil, err
		}
	}
	if err := lookup.flush(); err != nil {
		return nil, err
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	// Filter the keys based on the result by Redis.
	i := int64(0)
	var filtered []string
	for _, cmd := range lookup.cmds {
		bits, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		for _, bit := range bits {
			if bit == 0 {
				filtered = append(filtered, strconv.FormatUint(keys[i], 10))
			}
			i++
		}
	}
	return filtered, nil
}

type bitMapLookup struct {
	b      *BitMap
	ctx    context.Context
	pipe   redis.Pipeliner
	bucket uint64
	batch  []interface{}
	cmds   []*redis.IntSliceCmd
}

func (i *bitMapLookup) lookup(key uint64) error {
	bucket := key >> i.b.Exp
	if bucket != i.bucket {
		if err := i.flush(); err != nil {
			return err
		}
		i.bucket = bucket
	}
	mask := (uint64(1) << i.b.Exp) - 1
	i.batch = append(i.batch, "GET", "u1", key&mask)
	return nil
}

func (i *bitMapLookup) flush() error {
	redisKey := fmt.Sprintf("%s-%x", i.b.Prefix, i.bucket)
	i.cmds = append(i.cmds, i.b.Redis.BitField(i.ctx, redisKey, i.batch...))
	i.bucket = 0
	i.batch = nil
	return nil
}
