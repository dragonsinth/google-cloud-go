package bttest

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	"github.com/google/btree"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
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

func (b btreeRows) AscendRange(greaterOrEqual, lessThan keyType, iterator RowIterator) {
	b.tree.AscendRange(b.key(greaterOrEqual), b.key(lessThan), b.adaptIterator(iterator))
}

func (b btreeRows) AscendLessThan(pivot keyType, iterator RowIterator) {
	b.tree.AscendLessThan(b.key(pivot), b.adaptIterator(iterator))
}

func (b btreeRows) AscendGreaterOrEqual(pivot keyType, iterator RowIterator) {
	b.tree.AscendGreaterOrEqual(b.key(pivot), b.adaptIterator(iterator))
}

func (b btreeRows) Delete(key keyType) *btpb.Row {
	item := b.tree.Delete(b.key(key))
	if item == nil {
		return nil
	}
	return fromProto(item.(protoItem).buf)
}

func (b btreeRows) Get(key keyType) *btpb.Row {
	item := b.tree.Get(b.key(key))
	if item == nil {
		return nil
	}
	return fromProto(item.(protoItem).buf)
}

func (b btreeRows) ReplaceOrInsert(r *btpb.Row) *btpb.Row {
	item := b.tree.ReplaceOrInsert(protoItem{
		key: r.Key,
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

func (b btreeRows) key(key keyType) protoItem {
	return protoItem{key: key}
}

func (b btreeRows) adaptIterator(iterator RowIterator) btree.ItemIterator {
	return func(i btree.Item) bool {
		r := fromProto(i.(protoItem).buf)
		return iterator(r)
	}
}

func fromProto(buf []byte) *btpb.Row {
	var p btpb.Row
	if err := proto.Unmarshal(buf, &p); err != nil {
		panic(err)
	}
	return &p
}

func toProto(r *btpb.Row) []byte {
	if buf, err := proto.Marshal(r); err != nil {
		panic(err)
	} else {
		return buf
	}
}

type protoItem struct {
	key keyType
	buf []byte
}

var _ btree.Item = protoItem{}

// Less implements btree.Item.
func (bi protoItem) Less(i btree.Item) bool {
	return bytes.Compare(bi.key, i.(protoItem).key) < 0
}
