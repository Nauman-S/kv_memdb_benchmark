package mdbx

import (
	"dbbenchmarking/util"
	"github.com/erigontech/mdbx-go/mdbx"
	"runtime"
	"sync"
	"testing"
)

func init() {
	util.Init()
}
func BenchmarkMDBXMultiReadSingleWrite(b *testing.B) {
	env, dbi := initDB(b)
	defer env.Close()
	numWrites := 1000
	numReaders := 10
	numReads := 1000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var wg sync.WaitGroup
		start := make(chan struct{})
		initializeWriters(&wg, start, numWrites, env, dbi, b)
		initializeReaders(numReaders, numReads, &wg, start, env, dbi, b, numWrites)
		b.StartTimer()
		close(start)
		wg.Wait()
	}
}

func initializeReaders(numReaders int, numReads int, wg *sync.WaitGroup, start <-chan struct{}, env *mdbx.Env, dbi mdbx.DBI, b *testing.B, numWrites int) {
	randomIndices := make([]*[]int, numReaders)
	for i := 0; i < numReaders; i++ {
		randomArray := make([]int, numWrites)
		for j := 0; j < numWrites; j++ {
			randomArray[j] = j
		}
		util.ShuffleSlice(randomArray)
		randomIndices[i] = &randomArray
	}
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			<-start
			for j := 0; j < numReads; j++ {
				txn, err := env.BeginTxn(nil, mdbx.Readonly)
				if err != nil {
					b.Fatal(err)
				}

				cursor, err := txn.OpenCursor(dbi)
				if err != nil {
					b.Logf("Reader %d: Failed to open cursor: %v\n", readerID, err)
					txn.Abort()
					continue
				}

				key, expectedVal := util.GetKeyValueAtIndex((*randomIndices[readerID])[j])

				var value []byte
				_, value, err = cursor.Get(key, nil, mdbx.Set)

				if err != nil {

					if !mdbx.IsErrno(err, mdbx.NotFound) {
						b.Log(err)
					}
				} else {
					if err == nil && string(value) != string(expectedVal) {
						b.Fatalf(
							"Reader %d: Expected value %s, got %s",
							readerID,
							string(expectedVal),
							string(value))
					}
				}

				cursor.Close()
				txn.Abort()
			}
		}(i)
	}
}

func initializeWriters(wg *sync.WaitGroup, start <-chan struct{}, numWrites int, env *mdbx.Env, dbi mdbx.DBI, b *testing.B) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start

		it := util.NewDataIterator()

		for i := 0; i < numWrites; i++ {
			runtime.LockOSThread()
			txn, err := env.BeginTxn(nil, 0)
			if err != nil {
				b.Fatal(err)
				continue
			}

			key, value, _ := it.Next()
			if err = txn.Put(dbi, key, value, 0); err != nil {
				b.Logf("writer: Failed to put key-value pair: %v\n", err)
			}

			if _, err = txn.Commit(); err != nil {
				b.Fatal(err)
			}
			runtime.UnlockOSThread()
		}
	}()
}

func initDB(b *testing.B) (*mdbx.Env, mdbx.DBI) {
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

	var dbi mdbx.DBI
	err = env.Update(func(txn *mdbx.Txn) error {
		var err error
		dbi, err = txn.OpenDBI("mydb", mdbx.Create, nil, nil)
		return err
	})
	if err != nil {
		b.Fatalf("Failed to create/open database: %v", err)
	}

	return env, dbi
}
