package bttest

type RowsFactory interface {
	NewRows(parent string, name string) Rows
}

type Rows interface {
	// Ascend calls the iterator for every row in the table within the range
	// [first, last], until iterator returns false.
	Ascend(iterator RowIterator)

	// AscendRange calls the iterator for every row in the table within the range
	// [greaterOrEqual, lessThan), until iterator returns false.
	AscendRange(greaterOrEqual, lessThan string, iterator RowIterator)

	// AscendLessThan calls the iterator for every row in the table within the range
	// [first, pivot), until iterator returns false.
	AscendLessThan(pivot string, iterator RowIterator)

	// AscendGreaterOrEqual calls the iterator for every row in the table within
	// the range [pivot, last], until iterator returns false.
	AscendGreaterOrEqual(pivot string, iterator RowIterator)

	// Clear removes all rows from the table.
	Clear()

	// Delete removes a row whose key is equal to given key, returning it.
	// Returns nil if unable to find that row.
	Delete(key string) *row

	// Get looks for a row whose key is equal to the given key, returning it.
	// Returns nil if unable to find that row.
	Get(key string) *row

	// ReplaceOrInsert adds the given row to the table.  If a row in the table
	// already equals the given one, it is removed from the table and returned.
	// Otherwise, nil is returned.
	//
	// nil cannot be added to the table (will panic).
	ReplaceOrInsert(r *row) *row
}

type RowIterator = func(r *row) bool
