package badgerdb

import (
	"dbbenchmarking/util"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"testing"
)

func init() {
	util.Init()
}

// Batching multiple operations in a single TX
func BenchmarkBadgerDBBatchTx(b *testing.B) {
	opts := badger.DefaultOptions(util.PathBadger).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, batchSize := range util.GetBatchSize() {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runForTxSize(batchSize, b, db)
		})
	}

}

func runForTxSize(batchSize int, b *testing.B, db *badger.DB) {

	txn := db.NewTransaction(true) // true = writable tx, false = read only
	defer txn.Discard()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, val := util.GetKeyValue()
		err := txn.SetEntry(badger.NewEntry(key, val))
		if err != nil {
			b.Fatal(err)
		}

		if i%batchSize == 0 {
			err = txn.Commit()
			if err != nil {
				b.Fatal(err)
			}
			txn = db.NewTransaction(true)
		}
	}

	if err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}
