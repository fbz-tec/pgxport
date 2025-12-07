package encoders

// DataParams holds a value and its PostgreSQL type OID for encoding.
type DataParams struct {
	Value     any
	ValueType uint32
}
