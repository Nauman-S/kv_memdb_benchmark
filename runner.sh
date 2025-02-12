#! /bin/zsh
alias remove_system_info="grep -v -e '^goos:' -e '^goarch:' -e '^pkg:' -e '^cpu:'"
alias get_system_info="grep -e '^goos:' -e '^goarch:' -e '^pkg:' -e '^cpu:'"


go test -bench=BenchmarkBadgerDBUpdateTest ./badgerdb -benchmem | get_system_info

echo "\n\nBenchmark Read functions\n\n"
go test -bench=BenchmarkRocksDBGet ./rocksdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkBadgerGet ./badgerdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkMDBXGet ./mdbx -benchmem | remove_system_info
sleep 1


echo "\n\nBenchmark Write functions\n\n"

go test -bench=BenchmarkRocksDBPut ./rocksdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkBadgerDBUpdateTest ./badgerdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkBboltDBUpdateTest ./bbolt -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkMDBXDBUpdateTest ./mdbx -benchmem | remove_system_info
sleep 1

echo "\n\nBenchmark Delete functions\n\n"

go test -bench=BenchmarkRocksDBDelete ./rocksdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkBadgerDelete ./badgerdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkMDBXDeleteCursors ./mdbx -benchmem | remove_system_info
sleep 1

echo "\n\nBenchmark tx Create + tx Commit manually \n\n"

go test -bench=BenchmarkRocksDBTx ./rocksdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkBadgerDBManualTx ./badgerdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkBboltDBManualTx ./bbolt -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkMDBXDBManualTx ./mdbx -benchmem | remove_system_info
sleep 1

echo "\n\nBenchmark different batch sizes \n\n"

go test -bench=BenchmarkRocksDBBatch ./rocksdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkBadgerDBBatchTx ./badgerdb -benchmem | remove_system_info
go test -bench=BenchmarkBadgerDBBatchWriteBatch ./badgerdb -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkBboltBatchTx ./bbolt -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkMDBXDBBatchTx ./mdbx -benchmem | remove_system_info

sleep 1


echo "\n\nMultiple Reader Single Writer\n\n"

go test -bench=BenchmarkBadgerMultiReadSingleWrite -benchtime=5s ./badgerdb -benchmem | remove_system_info
echo "\n"

go test -bench=BenchmarkMDBXMultiReadSingleWrite -benchtime=5s ./mdbx -benchmem | remove_system_info


echo "\n\nMultiple Reader Multiple Writer\n\n"

go test -bench=BenchmarkBadgerMultiReadMultiWrite -benchtime=5s ./badgerdb -benchmem | remove_system_info
echo "\n"

go test -bench=BenchmarkMDBXMultiReadMultiWrite -benchtime=5s ./mdbx -benchmem | remove_system_info