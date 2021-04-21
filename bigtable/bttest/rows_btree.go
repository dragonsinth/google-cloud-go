package bttest

import (
	"github.com/google/btree"
)

const btreeDegree = 16

type btreeFactory struct {
}

var _ RowsFactory = btreeFactory{}

func (btreeFactory) NewRows(_ string, _ string) Rows {
	return btreeRows{btree.New(btreeDegree)}
}

type btreeRows struct {
	tree *btree.BTree
}

var _ Rows = btreeRows{}

func (b btreeRows) Ascend(iterator RowIterator) {
	b.tree.Ascend(b.adaptIterator(iterator))
}

func (b btreeRows) AscendRange(greaterOrEqual, lessThan string, iterator RowIterator) {
	b.tree.AscendRange(b.key(greaterOrEqual), b.key(lessThan), b.adaptIterator(iterator))
}

func (b btreeRows) AscendLessThan(pivot string, iterator RowIterator) {
	b.tree.AscendLessThan(b.key(pivot), b.adaptIterator(iterator))
}

func (b btreeRows) AscendGreaterOrEqual(pivot string, iterator RowIterator) {
	b.tree.AscendGreaterOrEqual(b.key(pivot), b.adaptIterator(iterator))
}

func (b btreeRows) Delete(key string) *row {
	item := b.tree.Delete(b.key(key))
	if item == nil {
		return nil
	}
	return item.(*row)
}

func (b btreeRows) Get(key string) *row {
	item := b.tree.Get(b.key(key))
	if item == nil {
		return nil
	}
	return item.(*row)
}

func (b btreeRows) ReplaceOrInsert(r *row) *row {
	item := b.tree.ReplaceOrInsert(r)
	if item == nil {
		return nil
	}
	return item.(*row)
}

func (b btreeRows) Clear() {
	b.tree.Clear(false)
}

func (b btreeRows) key(key string) *row {
	return &row{key: key}
}

func (b btreeRows) adaptIterator(iterator RowIterator) btree.ItemIterator {
	return func(i btree.Item) bool {
		return iterator(i.(*row))
	}
}

var _ btree.Item = (*row)(nil)

// Less implements btree.Item.Less.
func (r *row) Less(i btree.Item) bool {
	return r.key < i.(*row).key
}
