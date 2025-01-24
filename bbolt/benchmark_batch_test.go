package bbolt

import (
	"dbbenchmarking/util"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"testing"
)

func init() {
	util.Init()
}
func BenchmarkBboltBatchTx(b *testing.B) {
	db, err := bolt.Open(util.PathBbolt, 0600, nil)
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

func runForBatchSize(batchSize int, b *testing.B, db *bolt.DB) {
	err := db.Update(func(txn *bolt.Tx) error {
		_, err := txn.CreateBucketIfNotExists([]byte(b.Name()))
		return err
	})

	if err != nil {
		b.Fatal(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte(b.Name()))
	if bucket == nil {
		b.Fatal("Bucket is nil")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i%batchSize == 0 {
			if tx != nil {
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
			tx, err = db.Begin(true)
			if err != nil {
				b.Fatal(err)
			}
			bucket = tx.Bucket([]byte(b.Name()))
			if bucket == nil {
				b.Fatal("Bucket is nil")
			}
		}
		key, val := util.GetKeyValue()
		err := bucket.Put(key, val)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Commit the remaining records
	if tx != nil {
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
