package mdbx

import (
	"dbbenchmarking/util"
	"fmt"
	"github.com/erigontech/mdbx-go/mdbx"
	"testing"
)

func init() {
	util.Init()
}

func BenchmarkMDBXDBUpdateTest(b *testing.B) {
	env, err := mdbx.NewEnv()
	if err != nil {
		b.Fatal(err)
	}
	env.SetOption(mdbx.OptMaxDB, 1)
	if err != nil {
		b.Fatal(err)
	}
	err = env.SetGeometry(16*1024*1024, /* Lower size: 16 MiB */
		1<<30, /* Current size: 1 GiB */
		4<<30, /* Upper size: 4 GiB */
		0, 0, 0)

	err = env.Open(util.PathMDBX, mdbx.Create, 0664)
	if err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, val := util.GetKeyValue()
		env.Update(func(txn *mdbx.Txn) error {
			dbi, err := txn.OpenDBI("mydb", mdbx.Create, nil, nil)
			if err != nil {
				return fmt.Errorf("open DBI: %w", err)
			}
			if err := txn.Put(dbi, key, val, 0); err != nil {
				return fmt.Errorf("put: %w", err)
			}
			return nil
		})
	}

}
