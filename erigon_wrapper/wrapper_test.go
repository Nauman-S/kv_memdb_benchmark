package erigon_wrapper

import (
	"context"
	"dbbenchmarking/erigon_wrapper/badger"
	"dbbenchmarking/erigon_wrapper/kv"
	"dbbenchmarking/erigon_wrapper/mdbx"
	"dbbenchmarking/erigon_wrapper/stages"
	"dbbenchmarking/util"
	"testing"
)

func init() {
	util.Init()
}

func BenchmarkMDBXWrapped(b *testing.B) {
	db := mdbx.InitMDBXChainData()
	defer db.Close()
	var roTx kv.Tx
	var rwTx kv.RwTx
	var err error
	ctx := context.Background()

	var val uint64 = 0

	for i := 0; i < b.N; i++ {
		roTx, err = db.BeginRo(ctx)
		if err != nil {
			panic(err)
		}
		stages.GetStageProgress(roTx, stages.Execution)
		roTx.Rollback()

		if rwTx, err = db.BeginRw(ctx); err != nil {
			panic(err)
		}
		_, err = stages.GetStageProgress(rwTx, stages.L1InfoTree)
		if err != nil {
			panic(err)
		}
		stages.SaveStageProgress(rwTx, stages.Execution, val)
		stages.GetStageProgress(rwTx, stages.HighestSeenBatchNumber)
		rwTx.GetOne(kv.Headers, []byte("key"))
		stages.SaveStageProgress(rwTx, stages.DataStream, val)

		stages.SaveStageProgress(rwTx, stages.Headers, val)
		stages.SaveStageProgress(rwTx, stages.HighestSeenBatchNumber, val)
		stages.SaveStageProgress(rwTx, stages.IntermediateHashes, val)
		stages.SaveStageProgress(rwTx, stages.AccountHistoryIndex, val)
		stages.SaveStageProgress(rwTx, stages.StorageHistoryIndex, val)
		val++
		rwTx.Commit()
	}
}

func BenchmarkBadgerWrapped(b *testing.B) {
	db := badger.InitBadgerChainData()
	defer db.Close()
	var roTx kv.Tx
	var rwTx kv.RwTx
	var err error
	ctx := context.Background()

	var val uint64 = 0

	for i := 0; i < b.N; i++ {
		roTx, err = db.BeginRo(ctx)
		if err != nil {
			panic(err)
		}
		stages.GetStageProgress(roTx, stages.Execution)
		roTx.Rollback()

		if rwTx, err = db.BeginRw(ctx); err != nil {
			panic(err)
		}
		_, err = stages.GetStageProgress(rwTx, stages.L1InfoTree)
		if err != nil {
			panic(err)
		}
		stages.SaveStageProgress(rwTx, stages.Execution, val)
		stages.GetStageProgress(rwTx, stages.HighestSeenBatchNumber)
		rwTx.GetOne(kv.Headers, []byte("key"))
		stages.SaveStageProgress(rwTx, stages.DataStream, val)

		stages.SaveStageProgress(rwTx, stages.Headers, val)
		stages.SaveStageProgress(rwTx, stages.HighestSeenBatchNumber, val)
		stages.SaveStageProgress(rwTx, stages.IntermediateHashes, val)
		stages.SaveStageProgress(rwTx, stages.AccountHistoryIndex, val)
		stages.SaveStageProgress(rwTx, stages.StorageHistoryIndex, val)
		val++
		rwTx.Commit()
	}
}
