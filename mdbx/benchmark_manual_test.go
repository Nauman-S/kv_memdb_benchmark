package mdbx

import (
	"dbbenchmarking/util"
	"github.com/erigontech/mdbx-go/mdbx"
	"runtime"
	"testing"
)

const path = "/tmp/mdbx"

func BenchmarkMDBXDBManualTx(b *testing.B) {
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

	var dbi mdbx.DBI
	err = env.Update(func(txn *mdbx.Txn) error {
		var err error
		dbi, err = txn.OpenDBI("mydb", mdbx.Create, nil, nil)
		return err
	})
	if err != nil {
		b.Fatalf("Failed to create/open database: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.LockOSThread()
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			b.Fatalf("Failed to begin transaction: %v", err)
		}
		key, val := util.GenerateRandomData()
		err = txn.Put(dbi, key, val, 0)

		if err != nil {
			txn.Abort()
			b.Fatalf("Failed to put key-value: %v", err)
		}

		txn.Commit()
		if err != nil {
			b.Fatalf("Failed to commit transaction: %v", err)
		}
		runtime.UnlockOSThread()
	}
}
