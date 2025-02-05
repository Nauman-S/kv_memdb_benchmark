package badger

import (
	"dbbenchmarking/erigon_wrapper/kv"
	"dbbenchmarking/util"
	"github.com/dgraph-io/badger/v4"
	"golang.org/x/sync/semaphore"
)

type BadgerOpts struct {
	opt            badger.Options
	label          kv.Label
	path           string
	numCompactors  int
	numGoRoutines  int
	numMemTables   int
	blockSize      int
	readTxLimiter  *semaphore.Weighted
	writeTxLimiter *semaphore.Weighted
}

func NewBadger() BadgerOpts {
	badger.DefaultOptions(util.PathBadger)
	return BadgerOpts{
		opt: badger.DefaultOptions(util.PathBadger).WithLoggingLevel(badger.ERROR),
	}
}

func (opts BadgerOpts) Label(label kv.Label) BadgerOpts {
	opts.label = label
	return opts
}

func (opts BadgerOpts) Path(path string) BadgerOpts {
	opts.path = path
	return opts
}

// Performance critical Tweakable options for LSM Tree
func (opts BadgerOpts) NumCompactors(numCompactors int) BadgerOpts {
	opts.numCompactors = numCompactors
	return opts
}

func (opts BadgerOpts) NumGoRoutines(numGoRoutines int) BadgerOpts {
	opts.numGoRoutines = numGoRoutines
	return opts
}

func (opts BadgerOpts) NumMemTables(numMemtables int) BadgerOpts {
	opts.numMemTables = numMemtables
	return opts
}

func (opts BadgerOpts) BlockSize(blockSize int) BadgerOpts {
	opts.blockSize = blockSize
	return opts
}

func (opts BadgerOpts) RoTxLimiter(readTxLimiter *semaphore.Weighted) BadgerOpts {
	opts.readTxLimiter = readTxLimiter
	return opts
}
