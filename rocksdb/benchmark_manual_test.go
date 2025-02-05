package rocksdb

import (
	"dbbenchmarking/util"
	"github.com/linxGnu/grocksdb"
	"testing"
)

func init() {
	util.Init()
}

func BenchmarkRocksDBTx(b *testing.B) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	tdbo := grocksdb.NewDefaultTransactionDBOptions()
	defer opts.Destroy()
	defer tdbo.Destroy()

	// ACID guarantees
	db, err := grocksdb.OpenTransactionDb(opts, tdbo, util.PathRocksDB)
	if err != nil {
		b.Fatalf("Failed to open RocksDB: %v", err)
	}

	defer db.Close()

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	to := grocksdb.NewDefaultTransactionOptions()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tx := db.TransactionBegin(wo, to, nil)
		key, val := util.GetKeyValue()
		err = tx.Put(key, val)
		if err != nil {
			b.Fatalf("Write error: %v", err)
		}
		err = tx.Commit()
		if err != nil {
			b.Fatalf("Commit error: %v", err)
		}
	}
}
