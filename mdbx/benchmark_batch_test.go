package mdbx

import (
	"dbbenchmarking/util"
	"fmt"
	"github.com/erigontech/mdbx-go/mdbx"
	"runtime"
	"testing"
)

func init() {
	util.Init()
}
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

	err = env.Open(util.PathMDBX, mdbx.Create, 0664)
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

	for _, batchSize := range util.GetBatchSize() {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runForBatchSize(batchSize, b, env, dbi)
		})
	}

}

func runForBatchSize(batchSize int, b *testing.B, env *mdbx.Env, dbi mdbx.DBI) {
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

		key, val := util.GetTestData()
		err = txn.Put(dbi, key, val, 0)
		if err != nil {
			txn.Abort()
			b.Fatalf("Failed to put key-value: %v", err)
		}

		if i%batchSize == 0 {
			txn.Commit()
			txn, err = env.BeginTxn(nil, 0)
		}

	}

	txn.Commit()
}
