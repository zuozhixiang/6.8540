
go build -buildmode=plugin ../mrapps/wc.go
go run mrworker.go wc.so

rm mr-out*
go run mrcoordinator.go pg-*.txt

