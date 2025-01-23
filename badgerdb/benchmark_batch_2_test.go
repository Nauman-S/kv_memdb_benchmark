package badgerdb

import (
	"dbbenchmarking/util"
	"github.com/dgraph-io/badger/v4"
	"testing"
)

const path = "/tmp/badger"

func BenchmarkBadgerDBBatchWriteBatch(b *testing.B) {
	opts := badger.DefaultOptions(path).WithLoggingLevel(badger.ERROR) // Stops the info logs
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	wb := db.NewWriteBatch()
	defer wb.Cancel()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, val := util.GenerateRandomData()
		err := wb.Set(key, val)
		//key, val is set in memory using tx.SetEntry as in bennchmark batch 1
		// If tx is reached limit it calls CommitWith which is a go routine commit call with a callback
		if err != nil {
			b.Fatal(err)
		}

		if i%100 == 0 {
			err = wb.Flush() //Waits for all 100 CommitWith calls to complete. Basically waits for write to disk
			if err != nil {
				b.Fatal(err)
			}
			wb = db.NewWriteBatch()
		}
	}

	if err := wb.Flush(); err != nil {
		b.Fatal(err)
	}
}
