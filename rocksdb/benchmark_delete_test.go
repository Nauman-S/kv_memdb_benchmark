package rocksdb

import (
	"dbbenchmarking/util"
	"fmt"
	"github.com/linxGnu/grocksdb"
	"math/rand"
	"testing"
)

func BenchmarkRocksDBDelete(b *testing.B) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	db, err := grocksdb.OpenDb(opts, util.PathRocksDB)

	if err != nil {
		b.Fatalf("Failed to open RocksDB: %v", err)
	}

	defer db.Close()
	defer opts.Destroy()

	for _, numEntries := range util.GetTotalEntries() {
		b.Run(fmt.Sprintf("entries=%d", numEntries), func(b *testing.B) {
			runDeleteForTotalEntries(numEntries, b, db)
		})
	}
}

func runDeleteForTotalEntries(numEntries int, b *testing.B, db *grocksdb.DB) {
	keys := make([][]byte, numEntries)
	randomReadIndices := make([]int, numEntries)
	for i := 0; i < numEntries; i++ {
		randomReadIndices[i] = rand.Intn(numEntries)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)
	flushOptions := grocksdb.NewDefaultFlushOptions()

	defer wo.Destroy()
	defer flushOptions.Destroy()

	for i := 0; i < numEntries; i++ {
		key, val := util.GetKeyValue()
		keys[i] = key
		db.Put(wo, key, val)
	}

	db.Flush(flushOptions)
	wb := grocksdb.NewWriteBatch()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		index := randomReadIndices[i%numEntries]
		wb.Delete(keys[index])
	}
	db.Write(wo, wb)
}
