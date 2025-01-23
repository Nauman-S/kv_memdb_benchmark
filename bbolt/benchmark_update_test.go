package bbolt

import (
	"dbbenchmarking/util"
	bolt "go.etcd.io/bbolt"
	"testing"
)

const path = "/tmp/bolt"

func BenchmarkBboltDBUpdateTest(b *testing.B) {
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = db.Update(func(txn *bolt.Tx) error {
			key, val := util.GenerateRandomData()
			return txn.Bucket([]byte("test-bucket")).Put(key, val)
		})
	}
}
