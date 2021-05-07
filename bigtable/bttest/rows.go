package bttest

import (
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

type RowsFactory interface {
	NewRows(parent string, name string) Rows
}

type keyType = []byte

type Rows interface {
	// Ascend calls the iterator for every row in the table within the range
	// [first, last], until iterator returns false.
	Ascend(iterator RowIterator)

	// AscendRange calls the iterator for every row in the table within the range
	// [greaterOrEqual, lessThan), until iterator returns false.
	AscendRange(greaterOrEqual, lessThan keyType, iterator RowIterator)

	// AscendLessThan calls the iterator for every row in the table within the range
	// [first, pivot), until iterator returns false.
	AscendLessThan(pivot keyType, iterator RowIterator)

	// AscendGreaterOrEqual calls the iterator for every row in the table within
	// the range [pivot, last], until iterator returns false.
	AscendGreaterOrEqual(pivot keyType, iterator RowIterator)

	// Clear removes all rows from the table.
	Clear()

	// Delete removes a row whose key is equal to given key, returning it.
	// Returns nil if unable to find that row.
	Delete(key keyType) *btpb.Row

	// Get looks for a row whose key is equal to the given key, returning it.
	// Returns nil if unable to find that row.
	Get(key keyType) *btpb.Row

	// ReplaceOrInsert adds the given row to the table.  If a row in the table
	// already equals the given one, it is removed from the table and returned.
	// Otherwise, nil is returned.
	//
	// nil cannot be added to the table (will panic).
	ReplaceOrInsert(r *btpb.Row) *btpb.Row
}

type RowIterator = func(r *btpb.Row) bool
