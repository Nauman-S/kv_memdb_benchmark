package rocksdb

import (
	"dbbenchmarking/util"
	"github.com/linxGnu/grocksdb"
	"testing"
)

func init() {
	util.Init()
}

func BenchmarkRocksDBPut(b *testing.B) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	// Open the database
	db, err := grocksdb.OpenDb(opts, util.PathRocksDB)

	if err != nil {
		b.Fatalf("Failed to open RocksDB: %v", err)
	}

	defer db.Close()
	defer opts.Destroy()

	wo := grocksdb.NewDefaultWriteOptions()
	flushOptions := grocksdb.NewDefaultFlushOptions()

	defer wo.Destroy()
	defer flushOptions.Destroy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, value := util.GetKeyValue()
		err := db.Put(wo, key, value)
		if err != nil {
			b.Fatalf("Write error: %v", err)
		}
	}
	db.Flush(flushOptions)
}
