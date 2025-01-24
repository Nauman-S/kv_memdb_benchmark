package badgerdb

import (
	"dbbenchmarking/util"
	"github.com/dgraph-io/badger/v4"
	"testing"
)

func init() {
	util.Init()
}

func BenchmarkBadgerDBManualTx(b *testing.B) {
	opts := badger.DefaultOptions(util.PathBadger).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txn := db.NewTransaction(true) // true = writable tx, false = read only
		key, val := util.GetKeyValue()
		err := txn.SetEntry(badger.NewEntry(key, val))
		if err != nil {
			b.Fatal(err)
		}
		err = txn.Commit() //Waits till entry is in LSM and valuelog file i.e. Written to disk
		if err != nil {
			b.Fatal(err)
		}
	}
}
