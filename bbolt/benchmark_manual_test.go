package bbolt

import (
	"dbbenchmarking/util"
	bolt "go.etcd.io/bbolt"
	"testing"
)

func init() {
	util.Init()
}
func BenchmarkBboltDBManualTx(b *testing.B) {
	db, err := bolt.Open(util.PathBbolt, 0600, nil)
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

	for i := 0; i < b.N; i++ {
		tx, err := db.Begin(true)
		if err != nil {
			b.Fatal(err)
		}
		bucket := tx.Bucket([]byte("test-bucket"))
		if bucket == nil {
			b.Fatalf("Bucket not found")
		}
		key, val := util.GetTestData()

		err = bucket.Put(key, val)
		if err != nil {
			b.Fatalf("Failed to put key-value: %v", err)
		}

		if err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit transaction: %v", err)
		}
	}
}
