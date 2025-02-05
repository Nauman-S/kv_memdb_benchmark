package badger

import (
	"context"
	"dbbenchmarking/erigon_wrapper/kv"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"golang.org/x/sync/semaphore"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type BadgerKV struct {
	store          *badger.DB
	readTxLimiter  *semaphore.Weighted
	writeTxLimiter *semaphore.Weighted
	opts           BadgerOpts
	path           string

	txsCountMutex         sync.Mutex
	txsCount              int32
	closed                atomic.Bool
	txsAllDoneOnCloseCond *sync.Cond
	leakDetector          *dbg.LeakDetector
}

func (db *BadgerKV) Close() {
	if ok := db.closed.CompareAndSwap(false, true); !ok {
		return
	}

	if db.store != nil {
		if err := db.store.Close(); err != nil {

		}
		db.store = nil
	}
}

func (db *BadgerKV) ReadOnly() bool {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerKV) View(ctx context.Context, f func(tx kv.Tx) error) error {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerKV) BeginRo(ctx context.Context) (txn kv.Tx, err error) {
	if !db.trackTxBegin() {
		return nil, fmt.Errorf("db closed")
	}

	if semErr := db.readTxLimiter.Acquire(ctx, 1); semErr != nil {
		db.trackTxEnd()
		return nil, fmt.Errorf("mdbx.MdbxKV.BeginRo: roTxsLimiter error %w", semErr)
	}

	defer func() {
		if txn == nil {
			db.readTxLimiter.Release(1)
			db.trackTxEnd()
		}
	}()

	badgerTxn := db.store.NewTransaction(false)

	return &BadgerTx{
		ctx:      ctx,
		readOnly: true,
		tx:       badgerTxn,
		db:       db,
		id:       db.leakDetector.Add(),
	}, nil
}

func (db *BadgerKV) AllTables() kv.TableCfg {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerKV) PageSize() uint64 {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerKV) CHandle() unsafe.Pointer {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerKV) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerKV) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) error {
	//TODO implement me
	panic("implement me")
}

func (db *BadgerKV) BeginRw(ctx context.Context) (txn kv.RwTx, err error) {
	if db.closed.Load() {
		return nil, fmt.Errorf("db closed")
	}

	if semErr := db.writeTxLimiter.Acquire(ctx, 1); semErr != nil {
		return nil, semErr
	}
	if !db.trackTxBegin() {
		return nil, fmt.Errorf("db closed")
	}
	tx := db.store.NewTransaction(true)

	defer func() {
		if txn == nil {
			db.writeTxLimiter.Release(1)
			db.trackTxEnd()
		}
	}()
	return &BadgerTx{
		tx:  tx,
		db:  db,
		ctx: ctx,
		id:  db.leakDetector.Add(),
	}, nil
}

func (db *BadgerKV) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	//TODO implement me
	panic("implement me")
}

func (opts BadgerOpts) Open() (kv.RwDB, error) {

	if opts.readTxLimiter == nil {
		targetSemCount := int64(runtime.GOMAXPROCS(-1) * 16)
		opts.readTxLimiter = semaphore.NewWeighted(targetSemCount) // 1 less than max to allow unlocking to happen
	}

	if opts.writeTxLimiter == nil {
		targetSemCount := int64(runtime.GOMAXPROCS(-1)) - 1
		opts.writeTxLimiter = semaphore.NewWeighted(targetSemCount) // 1 less than max to allow unlocking to happen
	}

	if len(opts.path) > 0 {
		opts.opt.Dir = opts.path
	}

	if numCompactors := opts.numCompactors; numCompactors > 0 {
		opts.opt.NumCompactors = numCompactors
	}

	if numGoRoutines := opts.numGoRoutines; numGoRoutines > 0 {
		opts.opt.NumGoroutines = numGoRoutines
	}

	if numMemTables := opts.numMemTables; numMemTables > 0 {
		opts.opt.NumMemtables = numMemTables
	}

	if blockSize := opts.blockSize; blockSize > 0 {
		opts.opt.BlockSize = blockSize
	}

	store, err := badger.Open(opts.opt)
	if err != nil {
		return nil, err
	}

	db := &BadgerKV{
		store:          store,
		opts:           opts,
		readTxLimiter:  opts.readTxLimiter,
		writeTxLimiter: opts.writeTxLimiter,
		path:           opts.path,
	}
	return db, nil
}

func (db *BadgerKV) trackTxBegin() bool {
	db.txsCountMutex.Lock()
	defer db.txsCountMutex.Unlock()

	isOpen := !db.closed.Load()

	if isOpen {
		db.txsCount++
	}
	return isOpen
}

func (db *BadgerKV) trackTxEnd() {
	db.txsCountMutex.Lock()
	defer db.txsCountMutex.Unlock()

	if db.txsCount > 0 {
		db.txsCount--
	} else {
		panic("MdbxKV: unmatched trackTxEnd")
	}

	if (db.txsCount == 0) && db.closed.Load() {
		db.txsAllDoneOnCloseCond.Signal()
	}
}
