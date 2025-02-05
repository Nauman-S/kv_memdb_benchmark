package badger

import (
	"context"
	"dbbenchmarking/erigon_wrapper/kv"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"unsafe"
)

type BadgerTx struct {
	tx       *badger.Txn
	db       *BadgerKV
	readOnly bool
	ctx      context.Context
	id       uint64
}

func (bTx BadgerTx) Put(table string, k, v []byte) error {
	return bTx.tx.Set(badgerTxKey(table, k), v)
}

func (bTx BadgerTx) Delete(table string, k []byte) error {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) IncrementSequence(table string, amount uint64) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) Append(table string, k, v []byte) error {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) AppendDup(table string, k, v []byte) error {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) DropBucket(s string) error {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) CreateBucket(s string) error {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) ExistsBucket(s string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) ClearBucket(s string) error {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) RwCursor(table string) (kv.RwCursor, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) RwCursorDupSort(table string) (kv.RwCursorDupSort, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) CollectMetrics() {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) Has(table string, key []byte) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) GetOne(table string, key []byte) (val []byte, err error) {
	fetchKey := badgerTxKey(table, key)
	item, err := bTx.tx.Get(fetchKey)

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	item.Value(func(v []byte) error {
		val = make([]byte, len(v))
		copy(val, v)
		return nil
	})
	return val, nil
}

func (bTx BadgerTx) ForEach(table string, fromPrefix []byte, walker func(k []byte, v []byte) error) error {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) ForPrefix(table string, prefix []byte, walker func(k []byte, v []byte) error) error {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) ForAmount(table string, prefix []byte, amount uint32, walker func(k []byte, v []byte) error) error {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) Commit() error {
	if bTx.tx == nil {
		return nil
	}

	defer func() {
		bTx.tx = nil
		bTx.db.trackTxEnd()
		if bTx.readOnly {
			bTx.db.readTxLimiter.Release(1)
		} else {
			bTx.db.writeTxLimiter.Release(1)
		}
		bTx.db.leakDetector.Del(bTx.id)
	}()

	err := bTx.tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (bTx BadgerTx) Rollback() {
	if bTx.tx == nil {
		return
	}
	defer func() {
		bTx.tx = nil
		bTx.db.trackTxEnd()
		if bTx.readOnly {
			bTx.db.readTxLimiter.Release(1)
		} else {
			bTx.db.writeTxLimiter.Release(1)
		}
		bTx.db.leakDetector.Del(bTx.id)
	}()
	bTx.tx.Discard()
}

func (bTx BadgerTx) ReadSequence(table string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) ListBuckets() ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) ViewID() uint64 {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) Cursor(table string) (kv.Cursor, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) CursorDupSort(table string) (kv.CursorDupSort, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) DBSize() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) Range(table string, fromPrefix, toPrefix []byte) (iter.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) RangeAscend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) RangeDescend(table string, fromPrefix, toPrefix []byte, limit int) (iter.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) Prefix(table string, prefix []byte) (iter.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (iter.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) CHandle() unsafe.Pointer {
	//TODO implement me
	panic("implement me")
}

func (bTx BadgerTx) BucketSize(table string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func badgerTxKey(table string, key []byte) []byte {
	fetchKey := make([]byte, len(table)+1+len(key))

	copy(fetchKey, table)
	fetchKey[len(table)] = ':'
	copy(fetchKey[len(table)+1:], key)
	return fetchKey
}
