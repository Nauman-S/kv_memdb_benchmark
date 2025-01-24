package badgerdb

import (
	"dbbenchmarking/util"
	"github.com/dgraph-io/badger/v4"
	"sync"
	"testing"
)

func init() {
	util.Init()
}

func BenchmarkBadgerMultiReadMultiWrite(b *testing.B) {
	opts := badger.DefaultOptions(util.PathBadger).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	numWriters := 10
	numWrites := 1000
	numReaders := 10
	numReads := 1000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var wg sync.WaitGroup
		start := make(chan struct{})
		initializeMultiWriters(&wg, start, numWrites, numWriters, db, b)
		initializeReaders(numReaders, numReads, &wg, start, db, b, numWrites)
		b.StartTimer()
		close(start)
		wg.Wait()
	}
}

func initializeMultiWriters(wg *sync.WaitGroup, start <-chan struct{}, numWrites int, numWriters int, db *badger.DB, b *testing.B) {
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			it := util.NewDataIterator()

			for i := 0; i < numWrites; i++ {
				key, val, _ := it.Next()
				txn := db.NewTransaction(true)

				err := txn.SetEntry(badger.NewEntry(key, val))
				if err != nil {
					b.Fatal(err)
				}
				txn.Commit()
			}
		}()

	}
}
