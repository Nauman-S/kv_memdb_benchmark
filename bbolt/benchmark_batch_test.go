package bbolt

import (
	"dbbenchmarking/util"
	bolt "go.etcd.io/bbolt"
	"testing"
)

func BenchmarkBboltBatchTx(b *testing.B) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *bolt.Tx) error {
		_, err := txn.CreateBucketIfNotExists([]byte("test-bucket"))
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

	bucket := tx.Bucket([]byte("test-bucket"))
	if bucket == nil {
		b.Fatal("Bucket is nil")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Start a transaction at the beginning of each batch
		if i%100 == 0 {
			// Commit the previous transaction when batch size is reached
			if tx != nil {
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
			// Start a new writable transaction
			tx, err = db.Begin(true)
			if err != nil {
				b.Fatal(err)
			}
			bucket = tx.Bucket([]byte("test-bucket"))
			if bucket == nil {
				b.Fatal("Bucket is nil")
			}
		}

		// Add entries to the bucket within the current transaction
		key, val := util.GenerateRandomData()
		err := bucket.Put(key, val)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Commit the remaining records in the final batch
	if tx != nil {
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
