// Package rebuilder provides a struct which helps with rebuilding
// indices by adding and managing an index.
package rebuilder

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/dc0d/positive/pkg/layer"
	"github.com/dc0d/positive/pkg/peripheral"
)

// Options for *Rebuilder.
type Options struct {
	DB        *layer.DB
	BatchSize int
	DBVersion uint64
	IndexName string
}

// New creates new *Rebuilder.
func New(opt Options) *Rebuilder {
	if opt.IndexName == "" {
		opt.IndexName = "DATABASE_VERSION"
	}
	if opt.BatchSize <= 0 {
		opt.BatchSize = 300
	}
	if opt.DB == nil {
		panic(".DB must be provided")
	}
	res := &Rebuilder{
		db:        opt.DB,
		batchSize: opt.BatchSize,
		dbVersion: opt.DBVersion,
		indexName: opt.IndexName,
	}

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, res.dbVersion)
	res.header = hex.EncodeToString(b)

	res.rebuilderIndex = peripheral.NewIndex(res.indexName, func(key, val []byte) (entries []peripheral.IndexEntry, err error) {
		ix := res.header + ":" + string(key)
		entries = append(entries, peripheral.IndexEntry{Index: []byte(ix)})
		return
	})

	return res
}

// Rebuilder .
type Rebuilder struct {
	db        *layer.DB
	batchSize int
	dbVersion uint64
	indexName string

	rebuilderIndex *peripheral.Index
	header         string
}

// Index shoud be part of indices that are being processed inside
// passed indexBuilder.
func (rr *Rebuilder) Index() *peripheral.Index { return rr.rebuilderIndex }

// Rebuild .
func (rr *Rebuilder) Rebuild(indexBuilder layer.BeforeCommit) error {
	var ver uint64
	for ver = 0; ver < rr.dbVersion; ver++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, ver)
		start := hex.EncodeToString(b)

		cnt := 1
		for cnt > 0 {
			err := rr.db.UpdateWith(func(txn *layer.Txn) error {
				res, _, err := peripheral.QueryIndex(peripheral.Q{
					Index:  rr.indexName,
					Limit:  rr.batchSize,
					Start:  []byte(start),
					Prefix: []byte(start),
					End:    []byte(rr.header + ":\uffff"),
				}, txn)
				if err != nil {
					return err
				}
				cnt = len(res)
				for _, v := range res {
					itm, err := txn.Get(v.Key)
					if err != nil {
						return err
					}
					k := itm.KeyCopy(nil)
					v, err := itm.ValueCopy(nil)
					if err != nil {
						return err
					}
					if err := txn.Set(k, v); err != nil {
						return err
					}
				}
				return nil
			}, indexBuilder)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
