package badgerdb

import (
	"dbbenchmarking/util"
	"github.com/dgraph-io/badger/v4"
	"testing"
)

func init() {
	util.Init()
}
func BenchmarkBadgerDBUpdateTest(b *testing.B) {
	opts := badger.DefaultOptions(util.PathBadger).WithLoggingLevel(badger.ERROR) // Stops the info logs
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, val := util.GetTestData()
		err = db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, val)
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
