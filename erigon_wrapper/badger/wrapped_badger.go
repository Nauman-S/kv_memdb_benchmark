package badger

import (
	"dbbenchmarking/erigon_wrapper/kv"
	"dbbenchmarking/util"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
)

func InitBadgerChainData() kv.RwDB {

	var logger = log.Root()
	DBReadConcurrency := 20000 //cmp.Min(cmp.Max(10, runtime.GOMAXPROCS(-1)*64), 9_000)
	roTxLimit := int64(DBReadConcurrency)
	roTxsLimiter := semaphore.NewWeighted(roTxLimit)
	logger.SetHandler(log.DiscardHandler())

	opts := NewBadger().Label(kv.ChainDB).Path(util.PathBadger).RoTxLimiter(roTxsLimiter)

	rwDB, err := opts.Open()
	if err != nil {
		panic(err)
	}
	return rwDB
}
