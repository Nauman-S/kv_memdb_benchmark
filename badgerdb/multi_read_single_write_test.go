package badgerdb

import (
	"dbbenchmarking/util"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"sync"
	"testing"
)

func init() {
	util.Init()
}

func BenchmarkBadgerMultiReadSingleWrite(b *testing.B) {
	opts := badger.DefaultOptions(util.PathBadger).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	numWrites := 1000
	numReaders := 10
	numReads := 1000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var wg sync.WaitGroup
		start := make(chan struct{})
		initializeWriters(&wg, start, numWrites, db, b)
		initializeReaders(numReaders, numReads, &wg, start, db, b, numWrites)
		b.StartTimer()
		close(start)
		wg.Wait()
	}
}

func initializeReaders(numReaders int, numReads int, wg *sync.WaitGroup, start <-chan struct{}, db *badger.DB, b *testing.B, numWrites int) {
	randomIndices := make([]*[]int, numReaders)
	for i := 0; i < numReaders; i++ {
		randomArray := make([]int, numWrites)
		for j := 0; j < numWrites; j++ {
			randomArray[j] = j
		}
		util.ShuffleSlice(randomArray)
		randomIndices[i] = &randomArray
	}

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			<-start
			for j := 0; j < numReads; j++ {
				txn := db.NewTransaction(false)
				defer txn.Discard()

				key, expectedVal := util.GetKeyValueAtIndex((*randomIndices[readerID])[j])
				item, err := txn.Get(key)
				//actualValue := make([]byte, len(expectedVal))
				if err != nil {
					if !errors.Is(err, badger.ErrKeyNotFound) {
						b.Fatal(err)
					}

				} else {
					//item.ValueCopy(actualValue)

					err = item.Value(func(val []byte) error {
						if string(expectedVal) != string(val) {
							return errors.New("value mismatch")
						}
						return nil
					})

					if err != nil {
						b.Fatalf(
							"Reader %d: Expected value %s, value mismatch",
							readerID,
							string(expectedVal),
						)
					}
				}

			}
		}(i)
	}
}

func initializeWriters(wg *sync.WaitGroup, start <-chan struct{}, numWrites int, db *badger.DB, b *testing.B) {
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
