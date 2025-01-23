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
		if i%100 == 0 {
			if tx != nil {
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
			tx, err = db.Begin(true)
			if err != nil {
				b.Fatal(err)
			}
			bucket = tx.Bucket([]byte("test-bucket"))
			if bucket == nil {
				b.Fatal("Bucket is nil")
			}
		}
		key, val := util.GenerateRandomData()
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
