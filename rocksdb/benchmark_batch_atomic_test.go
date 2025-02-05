package rocksdb

import (
	"dbbenchmarking/util"
	"fmt"
	"testing"

	"github.com/linxGnu/grocksdb"
)

func init() {
	util.Init()
}

func BenchmarkRocksDBBatch(b *testing.B) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	db, err := grocksdb.OpenDb(opts, util.PathRocksDB)

	if err != nil {
		b.Fatalf("Failed to open RocksDB: %v", err)
	}

	defer db.Close()
	defer opts.Destroy()

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for _, batchSize := range util.GetBatchSize() {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runForBatchSize(batchSize, b, db, wo)
		})
	}
}

func runForBatchSize(batchSize int, b *testing.B, db *grocksdb.DB, wo *grocksdb.WriteOptions) {
	b.ResetTimer()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	for i := 0; i < b.N; i++ {
		key, val := util.GetKeyValue()
		batch.Put(key, val)
		if i%batchSize == 0 {
			if err := db.Write(wo, batch); err != nil {
				b.Fatalf("Failed to write batch: %v", err)
			}
			batch.Clear()
		}
	}

	if batch.Count() > 0 {
		if err := db.Write(wo, batch); err != nil {
			b.Fatalf("Failed to write final batch: %v", err)
		}
	}

}
