package util

import (
	"crypto/rand"
	"math/big"
	"os"
)

const PathBadger = "/tmp/badger"
const PathBbolt = "/tmp/bolt"
const PathMDBX = "/tmp/mdbx"

type KeyValue struct {
	Key []byte
	Val []byte
}

var t *testData

type testData struct {
	Data []KeyValue
	i    int
}

func Init() {
	t = &testData{
		make([]KeyValue, 1000000),
		0,
	}
	for i := 0; i < 1000000; i++ {
		key, val := generateRandomData()
		t.Data[i] = KeyValue{key, val}
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

func GetKeyValue() ([]byte, []byte) {
	t.i++
	if t.i == 1000000 {
		t.i = 0
	}
	return t.Data[t.i].Key, t.Data[t.i].Val
}

func GetKeyValueAtIndex(index int) ([]byte, []byte) {
	return t.Data[index].Key, t.Data[index].Val
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

func ShuffleSlice(slice []int) error {
	n := len(slice)

	for i := n - 1; i > 0; i-- {
		randomIndex, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			return err
		}
		j := int(randomIndex.Int64())
		slice[i], slice[j] = slice[j], slice[i]
	}

	return nil
}

type DataIterator struct {
	index int
}

func (it *DataIterator) Reset() {
	it.index = 0
}

func NewDataIterator() *DataIterator {
	return &DataIterator{index: 0}
}

func (it *DataIterator) Next() ([]byte, []byte, bool) {
	if it.index >= len(t.Data) {
		return nil, nil, false
	}
	kv := t.Data[it.index]
	it.index++
	return kv.Key, kv.Val, true
}
