#! /bin/zsh
alias remove_system_info="grep -v -e '^goos:' -e '^goarch:' -e '^pkg:' -e '^cpu:'"
alias get_system_info="grep -e '^goos:' -e '^goarch:' -e '^pkg:' -e '^cpu:'"

go mod tidy

go test -bench=BenchmarkBadgerDBUpdateTest ./badgerdb -benchmem | get_system_info

echo "\n\nBenchmark using Update functions\n\n"

go test -bench=BenchmarkBadgerDBUpdateTest ./badgerdb -benchmem | remove_system_info
echo "\n"


go test -bench=BenchmarkBboltDBUpdateTest ./bbolt -benchmem | remove_system_info
echo "\n"

go test -bench=BenchmarkMDBXDBUpdateTest ./mdbx -benchmem | remove_system_info

sleep 1

echo "\n\nBenchmark tx Create + tx Commit manually \n\n"



go test -bench=BenchmarkBadgerDBManualTx ./badgerdb -benchmem | remove_system_info
echo "\n"

go test -bench=BenchmarkBboltDBManualTx ./bbolt -benchmem | remove_system_info
echo "\n"

go test -bench=BenchmarkMDBXDBManualTx ./mdbx -benchmem | remove_system_info


echo "\n\nBenchmark different batch sizes \n\n"


go test -bench=BenchmarkBadgerDBBatchTx ./badgerdb -benchmem | remove_system_info
go test -bench=BenchmarkBadgerDBBatchWriteBatch ./badgerdb -benchmem | remove_system_info
echo "\n"

go test -bench=BenchmarkBboltBatchTx ./bbolt -benchmem | remove_system_info
echo "\n"
go test -bench=BenchmarkMDBXDBBatchTx ./mdbx -benchmem | remove_system_info

sleep 1