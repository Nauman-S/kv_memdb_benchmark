package mdbx

import (
	"dbbenchmarking/util"
	"github.com/erigontech/mdbx-go/mdbx"
	"runtime"
	"testing"
)

func BenchmarkMDBXDBBatchTx(b *testing.B) {
	env, err := mdbx.NewEnv()
	if err != nil {
		b.Fatal(err)
	}
	env.SetOption(mdbx.OptMaxDB, 1)
	err = env.SetGeometry(16*1024*1024, /* Lower size: 16 MiB */
		1<<30, /* Current size: 1 GiB */
		4<<30, /* Upper size: 4 GiB */
		0, 0, 0)
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
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	txn, err := env.BeginTxn(nil, 0)
	for i := 0; i < b.N; i++ {

		//If the Go scheduler moves the goroutine (and its transaction) between different OS threads during the lifetime of that transaction, MDBX will raise an error i think
		//Basically doesnt work without runtime.LockOSThread
		// Start a write transaction
		if err != nil {
			b.Fatalf("Failed to begin transaction: %v", err)
		}

		key, val := util.GenerateRandomData()
		err = txn.Put(dbi, key, val, 0)
		if err != nil {
			txn.Abort() // Ensure transaction cleanup on error
			b.Fatalf("Failed to put key-value: %v", err)
		}

		if i%100 == 0 { // Batch size of 100 operations
			txn.Commit()
			txn, err = env.BeginTxn(nil, 0)
		}

	}
	txn.Commit()

}
