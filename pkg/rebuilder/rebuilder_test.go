package rebuilder

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dc0d/positive/pkg/layer"
	"github.com/dc0d/positive/pkg/peripheral"
	"github.com/stretchr/testify/require"
)

func mkdir(d string) {
	if err := os.MkdirAll(d, 0777); err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
}

func createDB(databaseDir string, deleteExisting bool) *layer.DB {
	if databaseDir == "" {
		databaseDir, _ = ioutil.TempDir(os.TempDir(), "database")
	} else {
		databaseDir = filepath.Join(os.TempDir(), databaseDir)
	}
	mkdir(databaseDir)

	if deleteExisting {
		stat, err := os.Stat(databaseDir)
		if err != nil {
			if !os.IsNotExist(err) {
				panic(err)
			}
		}
		if stat != nil {
			if err := os.RemoveAll(databaseDir); err != nil {
				panic(err)
			}
		}
	}

	index := filepath.Join(databaseDir, "index")
	data := filepath.Join(databaseDir, "data")

	mkdir(index)
	mkdir(data)

	var opts = layer.DefaultOptions
	opts.Dir = index
	opts.ValueDir = data
	preppedDB, err := layer.Open(opts)
	if err != nil {
		panic(err)
	}

	return preppedDB
}

type data struct {
	ID   string
	Text string
	At   time.Time
}

func TestEmit_simple(t *testing.T) {
	require := require.New(t)

	db := createDB("", false)
	defer db.Close()

	var (
		dbVersion  uint64 = 1
		indices    []*peripheral.Index
		_rebuilder *Rebuilder
	)

	indexBuilder := func(txn *layer.Txn, entries map[string][]byte) error {
		for k, v := range entries {
			// all indexes must be built here,
			// based on document type (v), etc, etc.

			for _, ix := range indices {
				if err := peripheral.Emit(txn, ix, []byte(k), v); err != nil {
					return err
				}
			}
		}
		return nil
	}

	initIndices := func() {
		_rebuilder = New(Options{
			DB:        db,
			DBVersion: dbVersion,
		})
		indices = append(indices, _rebuilder.Index())
	}
	initIndices()

	fillDB := func(start, count int) {
		var wg sync.WaitGroup
		for i := start; i < start+count; i++ {
			i := i
			var d data
			d.ID = fmt.Sprintf("D:%010d", i)
			d.Text = fmt.Sprintf("%v", i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := db.UpdateWith(func(txn *layer.Txn) error {
					js, err := json.Marshal(&d)
					if err != nil {
						return err
					}
					return txn.Set([]byte(d.ID), js)
				}, indexBuilder)
				require.NoError(err)
			}()
		}
		wg.Wait()
	}
	fillDB(0, 5)

	cnt := 0
	db.View(func(txn *layer.Txn) error {
		opt := layer.DefaultIteratorOptions
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			cnt++
		}
		return nil
	})
	require.Equal(15, cnt)

	dbVersion++
	initIndices = func() {
		_rebuilder = New(Options{
			DB:        db,
			DBVersion: dbVersion,
		})

		indexTime := peripheral.NewIndex("itemat", func(key, val []byte) (entries []peripheral.IndexEntry, err error) {
			var d data
			json.Unmarshal(val, &d)
			if d.At.IsZero() {
				d.At = time.Now()
			}
			ix := d.At.Format(time.RFC3339)
			entries = append(entries, peripheral.IndexEntry{Index: []byte(ix)})
			return
		})

		indices = append(indices, _rebuilder.Index())
		_ = indexTime
		indices = append(indices, indexTime)
	}
	initIndices()

	fillDB(20, 5)

	cnt = 0
	db.View(func(txn *layer.Txn) error {
		opt := layer.DefaultIteratorOptions
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			cnt++
		}
		return nil
	})
	require.Equal(40, cnt)

	require.NoError(_rebuilder.Rebuild(indexBuilder))

	cnt = 0
	db.View(func(txn *layer.Txn) error {
		opt := layer.DefaultIteratorOptions
		opt.PrefetchValues = false
		itr := txn.NewIterator(opt)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			cnt++
		}
		return nil
	})
	require.Equal(50, cnt)
}
