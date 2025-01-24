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
func BenchmarkBadgerDBBatchWriteBatch(b *testing.B) {
	opts := badger.DefaultOptions(util.PathBadger).WithLoggingLevel(badger.ERROR) // Stops the info logs
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, batchSize := range util.GetBatchSize() {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runForBatchSize(batchSize, b, db)
		})
	}

}

func runForBatchSize(batchSize int, b *testing.B, db *badger.DB) {

	b.ResetTimer()
	wb := db.NewWriteBatch()
	for i := 0; i < b.N; i++ {

		if i%batchSize == 0 {
			if err := wb.Flush(); err != nil { //Flush is Blocking call that makes sure everything is flushed to disk
				b.Fatal(err)
			}
			wb = db.NewWriteBatch()
		}
		key, val := util.GetKeyValue()
		//key, val is set in memory using tx.SetEntry similar to the other batch badgerDB benchmarking
		// If tx memory limit is reached it calls asynchronous Commit i.e. CommitWith
		err := wb.Set(key, val)
		if err != nil {
			b.Fatal(err)
		}
	}
	if err := wb.Flush(); err != nil {
		b.Fatal(err)
	}
}
