package badgerdb

import (
	"dbbenchmarking/util"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"math/rand"
	"testing"
)

func init() {
	util.Init()
}
func BenchmarkBadgerGet(b *testing.B) {
	opts := badger.DefaultOptions(util.PathBadger).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, numEntries := range util.GetTotalEntries() {
		b.Run(fmt.Sprintf("entries=%d", numEntries), func(b *testing.B) {
			runReadForTotalEntries(numEntries, b, db)
		})
	}
}

func runReadForTotalEntries(numEntries int, b *testing.B, db *badger.DB) {
	keys := make([][]byte, numEntries)
	randomReadIndices := make([]int, numEntries)
	for i := 0; i < numEntries; i++ {
		randomReadIndices[i] = rand.Intn(numEntries)
	}

	for i := 0; i < numEntries; i++ {
		key, val := util.GetKeyValue()
		keys[i] = key
		err := db.Update(func(txn *badger.Txn) error {
			err := txn.Set(key, val)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db.View(func(txn *badger.Txn) error {
			index := randomReadIndices[i%numEntries]
			_, err := txn.Get(keys[index])
			if err != nil {
				b.Fatal(err)
				return err
			}
			return nil
		})
	}
}
