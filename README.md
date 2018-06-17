**positive** [![Go Report Card](https://goreportcard.com/badge/github.com/dc0d/positive)](https://goreportcard.com/report/github.com/dc0d/positive) 

**peripheral** [![GoDoc](https://godoc.org/github.com/dc0d/positive/pkg/peripheral?status.svg)](https://godoc.org/github.com/dc0d/positive/pkg/peripheral)

**layer** [![GoDoc](https://godoc.org/github.com/dc0d/positive/pkg/layer?status.svg)](https://godoc.org/github.com/dc0d/positive/pkg/layer)

# peripheral
kv playground 

Currently package `layer` provides a mean to execute a function before commit, which makes it simple to create secondary indexes for [badger](https://github.com/dgraph-io/badger/) key-value store.

Package `peripheral` provides a secondary index layer for badger key-value store - see examples.