// Package types defines Protobuf types and gRPC interfaces.
package types

//go:generate protoc -I=. --go_out=plugins:. --go_opt=paths=source_relative models.proto

// Check verifies all required fields are set.
func (m *ItemPointer) Check() bool {
	if m.Dst == nil {
		return false
	}
	if m.Timestamp == nil {
		return false
	}
	return true
}

// DedupKey returns the ID of the item the pointer is pointing to.
func (m *ItemPointer) DedupKey() string {
	return m.Dst.Id
}
