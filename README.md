**kvv** [![Go Report Card](https://goreportcard.com/badge/github.com/dc0d/kvv)](https://goreportcard.com/report/github.com/dc0d/kvv) [![GoDoc](https://godoc.org/github.com/dc0d/kvv?status.svg)](https://godoc.org/github.com/dc0d/kvv)

**kvw** [![GoDoc](https://godoc.org/github.com/dc0d/kvv/kvw?status.svg)](https://godoc.org/github.com/dc0d/kvv/kvw)

# kvv
kv playground 

Currently package `kvw` provides a mean to execute a function before commit, which makes it simple to create secondary indexes for [badger](https://github.com/dgraph-io/badger/) key-value store.

Package `kvv` provides a secondary index layer for badger key-value store - see examples.