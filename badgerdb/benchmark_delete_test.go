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

func BenchmarkBadgerDelete(b *testing.B) {
	opts := badger.DefaultOptions(util.PathBadger).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, numEntries := range util.GetTotalEntries() {
		b.Run(fmt.Sprintf("entries=%d", numEntries), func(b *testing.B) {
			runDeleteForTotalEntries(numEntries, b, db)
		})
	}
}

func runDeleteForTotalEntries(numEntries int, b *testing.B, db *badger.DB) {
	keys := make([][]byte, numEntries)
	randomReadIndices := make([]int, numEntries)
	for i := 0; i < numEntries; i++ {
		randomReadIndices[i] = rand.Intn(numEntries)
	}

	wb := db.NewWriteBatch()

	for i := 0; i < numEntries; i++ {
		key, val := util.GetKeyValue()
		keys[i] = key
		wb.Set(key, val)
	}

	err := wb.Flush()
	if err != nil {
		b.Fatal(err)
	}

	wb = db.NewWriteBatch()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := randomReadIndices[i%numEntries]
		err = wb.Delete(keys[index])
		if err != nil {
			b.Fatal(err)
			return
		}

	}
	wb.Flush()
}
