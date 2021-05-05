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
	return fromProto(item.(protoItem).buf)
}

func (b btreeRows) Get(key string) *row {
	item := b.tree.Get(b.key(key))
	if item == nil {
		return nil
	}
	return fromProto(item.(protoItem).buf)
}

func (b btreeRows) ReplaceOrInsert(r *row) *row {
	item := b.tree.ReplaceOrInsert(protoItem{
		key: r.key,
		buf: toProto(r),
	})
	if item == nil {
		return nil
	}
	return fromProto(item.(protoItem).buf)
}

func (b btreeRows) Clear() {
	b.tree.Clear(false)
}

func (b btreeRows) key(key string) protoItem {
	return protoItem{key: key}
}

func (b btreeRows) adaptIterator(iterator RowIterator) btree.ItemIterator {
	return func(i btree.Item) bool {
		r := fromProto(i.(protoItem).buf)
		return iterator(r)
	}
}

type protoItem struct {
	key string
	buf []byte
}

var _ btree.Item = protoItem{}

// Less implements btree.Item.
func (bi protoItem) Less(i btree.Item) bool {
	return bi.key < i.(protoItem).key
}
