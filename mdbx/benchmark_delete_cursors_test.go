package mdbx

import (
	"dbbenchmarking/util"
	"fmt"
	"github.com/erigontech/mdbx-go/mdbx"
	"log"
	"math/rand"
	"runtime"
	"testing"
)

func init() {
	util.Init()
}
func BenchmarkMDBXDeleteCursors(b *testing.B) {
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

	for _, totalEntries := range util.GetTotalEntries() {
		b.Run(fmt.Sprintf("totalEntries=%d", totalEntries), func(b *testing.B) {
			runDeleteUsingCursorsForTotalEntries(totalEntries, b, env, dbi)
		})
	}

}

func runDeleteUsingCursorsForTotalEntries(numEntries int, b *testing.B, env *mdbx.Env, dbi mdbx.DBI) {
	keys := make([][]byte, numEntries)
	randomReadIndices := make([]int, numEntries)
	for i := 0; i < numEntries; i++ {
		randomReadIndices[i] = rand.Intn(numEntries)
	}
	runtime.LockOSThread()
	tx, err := env.BeginTxn(nil, 0)
	if err != nil {
		log.Fatal(err)
	}
	tx.Drop(dbi, false)
	for i := 0; i < numEntries; i++ {
		key, val := util.GetKeyValue()
		keys[i] = key
		err = tx.Put(dbi, key, val, 0)
		if err != nil {
			tx.Abort()
			b.Fatalf("Failed to put key-value: %v", err)
		}
	}
	tx.Commit()
	runtime.UnlockOSThread()

	runtime.LockOSThread()
	tx, err = env.BeginTxn(nil, 0)
	var c *mdbx.Cursor
	c, err = tx.OpenCursor(dbi)

	c.Get(nil, nil, mdbx.First)

	if err != nil {
		b.Fatalf("Failed to open cursor: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := randomReadIndices[i%numEntries]
		_, _, err = c.Get(keys[index], nil, mdbx.Set)
		if err != nil && !mdbx.IsNotFound(err) {
			b.Fatalf("Failed to get key-value: %v", err)
		}
		c.Del(mdbx.Current)
	}
	c.Close()
	tx.Commit()

	runtime.UnlockOSThread()
}

func getRecordCount(env *mdbx.Env, dbi mdbx.DBI) (uint64, error) {
	var stat *mdbx.Stat
	err := env.View(func(txn *mdbx.Txn) error {
		var err error
		stat, err = txn.StatDBI(dbi)
		return err
	})
	if err != nil {
		return 0, err
	}
	return stat.Entries, nil
}
