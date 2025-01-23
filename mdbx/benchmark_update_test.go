package mdbx

import (
	"dbbenchmarking/util"
	"fmt"
	"github.com/erigontech/mdbx-go/mdbx"
	"testing"
)

func BenchmarkMDBXDBUpdateTest(b *testing.B) {
	env, err := mdbx.NewEnv()
	if err != nil {
		b.Fatal(err)
	}
	env.SetOption(mdbx.OptMaxDB, 1)
	if err != nil {
		b.Fatal(err)
	}

	err = env.Open(path, mdbx.Create, 0664)
	if err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.Update(func(txn *mdbx.Txn) error {
			dbi, err := txn.OpenDBI("mydb", mdbx.Create, nil, nil)
			if err != nil {
				return fmt.Errorf("open DBI: %w", err)
			}

			key, val := util.GenerateRandomData()
			if err := txn.Put(dbi, key, val, 0); err != nil {
				return fmt.Errorf("put: %w", err)
			}
			return nil
		})
	}

}
