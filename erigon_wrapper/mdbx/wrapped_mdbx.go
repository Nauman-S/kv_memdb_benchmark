package mdbx

import (
	"context"
	"dbbenchmarking/erigon_wrapper/kv"
	"dbbenchmarking/util"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
)

func InitMDBXChainData() kv.RwDB {

	var logger = log.Root()
	DBReadConcurrency := 20000 //cmp.Min(cmp.Max(10, runtime.GOMAXPROCS(-1)*64), 9_000)
	roTxLimit := int64(DBReadConcurrency)
	roTxsLimiter := semaphore.NewWeighted(roTxLimit)
	logger.SetHandler(log.DiscardHandler())
	opts := NewMDBX(logger).Label(kv.ChainDB).Path(util.PathMDBX).
		GrowthStep(16 * datasize.MB).
		DBVerbosity(2).RoTxsLimiter(roTxsLimiter).MapSize(12 * datasize.TB).PageSize((8 * datasize.KB).Bytes()) //Erigon defaults
	ctx := context.Background()
	opts = opts.DirtySpace(uint64(512 * datasize.MB))

	rwDB, err := opts.Open(ctx)
	if err != nil {
		panic(err)
	}
	return rwDB
}
