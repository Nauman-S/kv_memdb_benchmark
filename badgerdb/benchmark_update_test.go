package badgerdb

import (
	"dbbenchmarking/util"
	"github.com/dgraph-io/badger/v4"
	"testing"
)

func BenchmarkBadgerDBUpdateTest(b *testing.B) {
	opts := badger.DefaultOptions(path).WithLoggingLevel(badger.ERROR) // Stops the info logs
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, val := util.GenerateRandomData()
		err = db.Update(func(txn *badger.Txn) error { //Update = read-write transaction
			return txn.Set(key, val)
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
