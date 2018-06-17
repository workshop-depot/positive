// Package peripheral provides a secondary index layer for badger key-value store - see examples.
package peripheral

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"hash/fnv"

	"github.com/dc0d/positive/pkg/layer"
)

//-----------------------------------------------------------------------------

// IndexEntry .
type IndexEntry struct {
	Key, Val []byte
}

// IndexFn .
type IndexFn func(key, val []byte) ([]IndexEntry, error)

// Index .
type Index struct {
	name    string
	indexFn IndexFn
	hash    string
}

// NewIndex .
func NewIndex(name string, indexFn IndexFn) (res *Index) {
	if name == "" {
		panic("name must be provided")
	}
	if indexFn == nil {
		panic("indexFn must be provided")
	}
	res = &Index{name: name, indexFn: indexFn}
	res.hash = string(fnvhash([]byte(res.name)))
	return
}

//-----------------------------------------------------------------------------

// Emit .
func Emit(txn *layer.Txn, ix *Index, key, val []byte) (reserr error) {
	partk2x := indexSpace + ix.hash + indexK2X
	partx2k := indexSpace + ix.hash + indexX2K

	markedKey := indexSpace + string(key)
	preppedk := partk2x + markedKey

	opt := layer.DefaultIteratorOptions
	opt.PrefetchValues = false

	// delete previously calculated index for this key
	itr := txn.NewIterator(opt)
	defer itr.Close()
	prefix := []byte(preppedk)
	var toDelete [][]byte
	for itr.Seek(prefix); itr.ValidForPrefix(prefix); itr.Next() {
		item := itr.Item()
		k := item.KeyCopy(nil)
		v, err := item.ValueCopy(nil)
		if err != nil {
			reserr = err
			return
		}
		toDelete = append(toDelete, k)
		toDelete = append(toDelete, v)
	}
	for _, v := range toDelete {
		if err := txn.Delete(v); err != nil {
			if err != layer.ErrEmptyKey {
				reserr = err
				return
			}
		}
	}

	if val == nil {
		return
	}

	var indexEntries []IndexEntry
	indexEntries, reserr = ix.indexFn(key, val)
	if reserr != nil {
		return
	}

	for _, kv := range indexEntries {
		wix := indexSpace + string(kv.Key)
		k2x := preppedk + wix
		x2k := partx2k + wix + markedKey
		if reserr = txn.Set([]byte(k2x), []byte(x2k)); reserr != nil {
			return
		}
		if reserr = txn.Set([]byte(x2k), kv.Val); reserr != nil {
			return
		}
	}

	return
}

const (
	indexSpace = "^"
	indexK2X   = ">"
	indexX2K   = "<"
)

//-----------------------------------------------------------------------------

// error
var (
	ErrNoIndexNameProvided = fmt.Errorf("no index name provided")
)

// QueryIndex .
func QueryIndex(params Q, txn *layer.Txn, forIndexedKeys ...bool) (reslist []Res, rescount int, reserr error) {
	params.init()
	if params.Index == "" {
		reserr = ErrNoIndexNameProvided
		return
	}

	start, end, prefix := stopWords(params, forIndexedKeys...)

	skip, limit, applySkip, applyLimit := getlimits(params)

	body := func(itr interface{ Item() *layer.Item }) error {
		if params.Count {
			rescount++
			skip--
			if applySkip && skip >= 0 {
				return nil
			}
			if applyLimit && limit <= 0 {
				return nil
			}
			limit--
			if len(end) > 0 {
				item := itr.Item()
				k := item.Key()
				if bytes.Compare(k, end) > 0 {
					return nil
				}
			}
			return nil
		}
		item := itr.Item()
		k := item.KeyCopy(nil)
		// if params.Index == "" && bytes.HasPrefix(k, []byte(indexSpace)) {
		// 	return nil
		// }
		skip--
		if applySkip && skip >= 0 {
			return nil
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if applyLimit && limit <= 0 {
			return nil
		}
		limit--
		if len(end) > 0 {
			if bytes.Compare(k, end) > 0 {
				return nil
			}
		}
		var index []byte
		polishedKey := k
		// sppfx := []byte(keysp)
		// if bytes.HasPrefix(polishedKey, sppfx) {
		// 	polishedKey = bytes.TrimPrefix(polishedKey, sppfx)
		// }
		sppfx := []byte(indexSpace)
		if bytes.HasPrefix(polishedKey, sppfx) {
			parts := bytes.Split(polishedKey, sppfx)
			index = parts[2]
			polishedKey = parts[3]
		}
		var rs Res
		rs.Key = polishedKey
		rs.Val = v
		rs.Index = index
		reslist = append(reslist, rs)
		return nil
	}

	qfn := func(txn *layer.Txn) error {
		var opt = layer.DefaultIteratorOptions
		opt.PrefetchValues = true
		opt.PrefetchSize = limit
		return itrFunc(txn, opt, start, prefix, body)
	}
	// if parentTxn == nil {
	// 	reserr = db.db.View(qfn)
	// } else {
	// 	reserr = qfn(parentTxn)
	// }
	reserr = qfn(txn)
	if rescount == 0 {
		rescount = len(reslist)
	}

	return
}

func itrFunc(txn *layer.Txn,
	opt layer.IteratorOptions,
	start, prefix []byte,
	bodyFunc func(itr interface{ Item() *layer.Item }) error) error {
	itr := txn.NewIterator(opt)
	defer itr.Close()
	for itr.Seek(start); itr.ValidForPrefix(prefix); itr.Next() {
		if err := bodyFunc(itr); err != nil {
			return err
		}
	}
	return nil
}

func stopWords(params Q, forIndexedKeys ...bool) (start, end, prefix []byte) {
	name := string(fnvhash([]byte(params.Index)))
	domain := indexX2K
	if len(forIndexedKeys) > 0 && forIndexedKeys[0] {
		domain = indexK2X
	}
	pfx := indexSpace + name + domain
	start = []byte(pfx + indexSpace + string(params.Start))
	if len(params.End) > 0 {
		end = []byte(pfx + indexSpace + string(params.End))
	}
	if len(params.Prefix) > 0 {
		prefix = []byte(pfx + indexSpace + string(params.Prefix))
	} else {
		prefix = []byte(pfx)
	}

	return
}

func getlimits(params Q) (skip, limit int, applySkip, applyLimit bool) {
	skip = params.Skip
	limit = params.Limit
	var ()
	if skip > 0 {
		applySkip = true
	}
	if limit <= 0 && !params.Count {
		limit = 100
	}
	if limit > 0 {
		applyLimit = true
	}
	return
}

// Q query parameters
type Q struct {
	Index              string
	Start, End, Prefix []byte
	Skip, Limit        int
	Count              bool
}

func (q *Q) init() {
	if q.Limit <= 0 {
		q.Limit = 100
	}
}

// Res .
type Res struct {
	Key, Val []byte
	Index    []byte
}

//-----------------------------------------------------------------------------

func fnvhash(v []byte) []byte {
	h := fnv.New64a()
	h.Write(v)
	return []byte(hex.EncodeToString(h.Sum(nil)))
	// return h.Sum(nil)
}

//-----------------------------------------------------------------------------
