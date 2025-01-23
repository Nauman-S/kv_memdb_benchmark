package badgerdb

import (
	"dbbenchmarking/util"
	"github.com/dgraph-io/badger/v4"
	"testing"
)

// Batching multiple operations in a single TX
func BenchmarkBadgerDBBatchTx(b *testing.B) {
	opts := badger.DefaultOptions(path).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	txn := db.NewTransaction(true) // true = writable tx, false = read only
	defer txn.Discard()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, val := util.GenerateRandomData()
		err := txn.SetEntry(badger.NewEntry(key, val))
		if err != nil {
			b.Fatal(err)
		}

		if i%100 == 0 {
			err = txn.Commit()
			if err != nil {
				b.Fatal(err)
			}
			txn = db.NewTransaction(true)
		}
	}

	// Commit remaining entries (if any)
	if err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}
