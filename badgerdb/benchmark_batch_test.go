package badgerdb

import (
	"dbbenchmarking/util"
	"github.com/dgraph-io/badger/v4"
	"testing"
)

// Batching multiple operations in a single TX
func BenchmarkBadgerDBBatchTx(b *testing.B) {
	opts := badger.DefaultOptions(path).WithLoggingLevel(badger.ERROR) // Stops the info logs
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	txn := db.NewTransaction(true) // true = writable tx, false = read only
	defer txn.Discard()            // Ensure the transaction is discarded if not committed

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, val := util.GenerateRandomData()
		err := txn.SetEntry(badger.NewEntry(key, val))
		if err != nil {
			b.Fatal(err)
		}

		// Commit and start a new transaction after a certain batch size
		if i%100 == 0 {
			err = txn.Commit()
			if err != nil {
				b.Fatal(err)
			}
			txn = db.NewTransaction(true) // Start a new transaction
		}
	}

	// Commit remaining entries (if any) that were not part of the last batch
	if err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}
