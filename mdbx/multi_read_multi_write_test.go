package mdbx

import (
	"dbbenchmarking/util"
	"github.com/erigontech/mdbx-go/mdbx"
	"runtime"
	"sync"
	"testing"
)

func init() {
	util.Init()
}
func BenchmarkMDBXMultiReadMultiWrite(b *testing.B) {
	env, dbi := initDB(b)
	defer env.Close()
	numWriters := 10
	numWrites := 1000
	numReaders := 10
	numReads := 1000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var wg sync.WaitGroup
		start := make(chan struct{})
		initializeMultipleWriters(&wg, start, numWrites, numWriters, env, dbi, b)
		initializeReaders(numReaders, numReads, &wg, start, env, dbi, b, numWrites)
		b.StartTimer()
		close(start)
		wg.Wait()
	}
}

func initializeMultipleWriters(wg *sync.WaitGroup, start <-chan struct{}, numWrites int, numWriters int, env *mdbx.Env, dbi mdbx.DBI, b *testing.B) {

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			it := util.NewDataIterator()
			for i := 0; i < numWrites; i++ {
				runtime.LockOSThread()
				txn, err := env.BeginTxn(nil, 0)
				if err != nil {
					b.Fatal(err)
					continue
				}

				key, value, _ := it.Next()
				if err = txn.Put(dbi, key, value, 0); err != nil {
					b.Logf("writer: Failed to put key-value pair: %v\n", err)
				}

				if _, err = txn.Commit(); err != nil {
					b.Fatal(err)
				}
				runtime.UnlockOSThread()
			}
		}()
	}
}
