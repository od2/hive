// Package types defines Protobuf types and gRPC interfaces.
package types

import "fmt"

//go:generate ./generate.sh

// Check verifies all required fields are set.
func (m *ItemPointer) Check() error {
	if m == nil {
		return fmt.Errorf("empty pointer")
	}
	if m.Dst == nil {
		return fmt.Errorf("empty dst")
	}
	if m.Timestamp == nil {
		return fmt.Errorf("empty timestamp")
	}
	return nil
}

// DedupKey returns the ID of the item the pointer is pointing to.
func (m *ItemPointer) DedupKey() string {
	return m.Dst.Id
}
