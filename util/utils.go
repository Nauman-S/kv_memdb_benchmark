package util

import (
	"crypto/rand"
	"os"
)

const PathBadger = "/tmp/badger"
const PathBbolt = "/tmp/bolt"
const PathMDBX = "/tmp/mdbx"

type KeyValue struct {
	key []byte
	val []byte
}

var t *testData

type testData struct {
	data []KeyValue
	i    int
}

func Init() {
	t = &testData{
		make([]KeyValue, 1000000),
		0,
	}
	for i := 0; i < 1000000; i++ {
		key, val := generateRandomData()
		t.data[i] = KeyValue{key, val}
	}

	panicIfErr(os.RemoveAll(PathBadger))
	panicIfErr(os.RemoveAll(PathBbolt))
	panicIfErr(os.RemoveAll(PathMDBX))
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func GetTestData() ([]byte, []byte) {
	t.i++
	if t.i == 1000000 {
		t.i = 0
	}
	return t.data[t.i].key, t.data[t.i].val
}

func GetBatchSize() []int {
	return []int{1, 10, 100, 1000, 10000}
}

func generateRandomData() ([]byte, []byte) {
	key := make([]byte, 32)
	val := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		panic(err) // handle error as needed
	}
	_, err = rand.Read(val)
	if err != nil {
		panic(err) // handle error as needed
	}
	return key, val
}
