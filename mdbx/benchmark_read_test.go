package mdbx

import (
	"dbbenchmarking/util"
	"fmt"
	"github.com/erigontech/mdbx-go/mdbx"
	"math/rand"
	"runtime"
	"testing"
)

func init() {
	util.Init()
}

func BenchmarkMDBXGet(b *testing.B) {
	env, err := mdbx.NewEnv()
	if err != nil {
		b.Fatal(err)
	}
	env.SetOption(mdbx.OptMaxDB, 1)
	if err != nil {
		b.Fatal(err)
	}
	//err = env.SetGeometry(16*1024*1024, /* Lower size: 16 MiB */
	//	1<<30, /* Current size: 1 GiB */
	//	4<<30, /* Upper size: 4 GiB */
	//	0, 0, 0)
	err = env.SetGeometry(1024, /* Lower size: 1 KB */
		1024,  /* Current size: 1 KB */
		4<<30, /* Upper size: 4 GiB */
		1024, 0, 0)

	err = env.Open(util.PathMDBX, mdbx.Create, 0664)
	if err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBI("mydb", mdbx.Create, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	_, err = txn.Commit()
	if err != nil {
		b.Fatal(err)
	}

	for _, entries := range util.GetTotalEntries() {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {

			runForTotalEntries(entries, b, dbi, env)
		})
	}

}

func runForTotalEntries(numEntries int, b *testing.B, dbi mdbx.DBI, env *mdbx.Env) {

	keys := make([][]byte, numEntries)
	randomReadIndices := make([]int, numEntries)
	for i := 0; i < numEntries; i++ {
		randomReadIndices[i] = rand.Intn(numEntries)
	}
	var txn *mdbx.Txn
	var err error

	runtime.LockOSThread()
	txn, err = env.BeginTxn(nil, 0)
	for i := 0; i < numEntries; i++ {

		key, val := util.GetKeyValue()
		keys[i] = key
		err = txn.Put(dbi, key, val, 0)
		if err != nil {
			txn.Abort()
			b.Fatalf("Failed to put key-value: %v", err)
		}

		if i%100000 == 0 {
			txn.Commit()
			txn, err = env.BeginTxn(nil, 0)
		}
	}
	txn.Commit()
	runtime.UnlockOSThread()

	txn, err = env.BeginTxn(nil, mdbx.Readonly)

	if err != nil {
		b.Fatalf("Failed to begin transaction: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		index := randomReadIndices[i%numEntries]
		_, err = txn.Get(dbi, keys[index])
		if err != nil {
			b.Fatalf("Failed to get key-value: %v", err)
		}
	}
	txn.Abort()

}
