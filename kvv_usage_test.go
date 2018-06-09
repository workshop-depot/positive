package kvv_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dc0d/kvv"
	"github.com/dc0d/kvv/kvw"
	"github.com/stretchr/testify/require"
)

func mkdir(d string) {
	if err := os.MkdirAll(d, 0777); err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
}

func createDB(databaseDir string, deleteExisting bool) *kvw.DB {
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

	var opts = kvw.DefaultOptions
	opts.Dir = index
	opts.ValueDir = data
	preppedDB, err := kvw.Open(opts)
	if err != nil {
		panic(err)
	}

	return preppedDB
}

func TestEmit_simple(t *testing.T) {
	require := require.New(t)

	db := createDB("", false)
	defer db.Close()

	type post struct {
		ID   string    `json:"id"`
		Rev  string    `json:"rev"`
		By   string    `json:"by,omitempty"`
		Text string    `json:"text,omitempty"`
		At   time.Time `json:"at,omitempty"`
		Tags []string  `json:"tags,omitempty"`
	}

	indexTags := kvv.NewIndex("tags", func(key, val []byte) (entries []kvv.IndexEntry, err error) {
		var p post
		json.Unmarshal(val, &p)
		if len(p.Tags) == 0 {
			return
		}
		for _, v := range p.Tags {
			entries = append(entries, kvv.IndexEntry{Key: []byte(v)})
		}
		return
	})

	sampleIndexBuilder := func(txn *kvw.Txn, entries map[string][]byte) error {
		for k, v := range entries {
			// all indexes must be built here,
			// based on document type (v), etc, etc.

			if err := kvv.Emit(txn, indexTags, []byte(k), v); err != nil {
				return err
			}
		}
		return nil
	}

	func() {
		p := &post{
			ID:   "POST:001",
			By:   "Frodo Baggins",
			Text: "Awesome blog post!",
			At:   time.Now(),
			Tags: []string{"golang", "nosql"},
		}
		js, err := json.Marshal(p)
		require.NoError(err)

		txn := db.NewTransaction(true)
		defer txn.Discard()

		require.NoError(txn.Set([]byte(p.ID), js))

		err = txn.CommitWith(sampleIndexBuilder, nil)
		require.NoError(err)
	}()

	func() {
		got := make(map[string]bool)
		err := db.View(func(txn *kvw.Txn) error {
			itr := txn.NewIterator(kvw.DefaultIteratorOptions)
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				got[string(item.Key())] = true
			}
			return nil
		})
		require.NoError(err)
		require.Equal(5, len(got))
		require.True(got["POST:001"])
		for k := range got {
			cond := strings.Contains(k, "<^golang^POST:001") ||
				strings.Contains(k, "<^nosql^POST:001") ||
				strings.Contains(k, ">^POST:001^golang") ||
				strings.Contains(k, ">^POST:001^nosql") ||
				strings.Contains(k, "POST:001")
			require.True(cond)
		}
	}()

	func() {
		got := make(map[string]kvv.Res)
		err := db.View(func(txn *kvw.Txn) error {
			r, _, err := kvv.QueryIndex(kvv.Q{Index: "tags", Start: []byte("nosql"), Prefix: []byte("nosql")}, txn)
			if err != nil {
				return err
			}
			for _, v := range r {
				got[string(v.Key)] = v
			}
			return nil
		})
		require.NoError(err)
		for k, v := range got {
			require.Equal("POST:001", k)
			require.Equal("POST:001", string(v.Key))
			require.Equal("nosql", string(v.Index))
			break
		}
	}()

	func() {
		got := make(map[string]kvv.Res)
		err := db.View(func(txn *kvw.Txn) error {
			r, _, err := kvv.QueryIndex(kvv.Q{Index: "tags", Start: []byte("golang"), Prefix: []byte("golang")}, txn)
			if err != nil {
				return err
			}
			for _, v := range r {
				got[string(v.Key)] = v
			}
			return nil
		})
		require.NoError(err)
		for k, v := range got {
			require.Equal("POST:001", k)
			require.Equal("POST:001", string(v.Key))
			require.Equal("golang", string(v.Index))
			break
		}
	}()
}
