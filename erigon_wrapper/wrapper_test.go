package erigon_wrapper

import (
	"dbbenchmarking/util"
	"testing"
)

func init() {
	util.Init()
}

func BenchmarkMDBXWrapped(b *testing.B) {
	rwDB := InitMDBXChainData()
	defer rwDB.Close()
	for i := 0; i < b.N; i++ {
		//fmt.Println("test")
	}
}
