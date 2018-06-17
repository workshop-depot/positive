package peripheral

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/dc0d/positive/pkg/layer"
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

type comment struct {
	ID   string    `json:"id"`
	Rev  string    `json:"rev"`
	By   string    `json:"by,omitempty"`
	Text string    `json:"text,omitempty"`
	At   time.Time `json:"at,omitempty"`
	Tags []string  `json:"tags,omitempty"`
}

func ExampleNewIndex() {
	db := createDB("", false)
	defer db.Close()

	indexTags := NewIndex("tags", func(key, val []byte) (entries []IndexEntry, err error) {
		var p comment
		json.Unmarshal(val, &p)
		if len(p.Tags) == 0 {
			return
		}
		for _, v := range p.Tags {
			entries = append(entries, IndexEntry{Index: []byte(v)})
		}
		return
	})

	indexBy := NewIndex("tags", func(key, val []byte) (entries []IndexEntry, err error) {
		var p comment
		json.Unmarshal(val, &p)
		if p.By == "" {
			return
		}
		entries = append(entries, IndexEntry{Index: []byte(p.By)})
		return
	})

	sampleIndexBuilder := func(txn *layer.Txn, entries map[string][]byte) error {
		for k, v := range entries {
			// all indexes must be built here,
			// based on document type (v), etc, etc.

			if err := Emit(txn, indexTags, []byte(k), v); err != nil {
				return err
			}
			if err := Emit(txn, indexBy, []byte(k), v); err != nil {
				return err
			}
		}
		return nil
	}

	func() {
		cmnt := comment{
			ID:   "CMNT::001",
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"tech", "golang"},
		}
		js, err := json.Marshal(cmnt)
		fmt.Println(err)

		txn := db.NewTransaction(true)
		defer txn.Discard()

		fmt.Println(txn.Set([]byte(cmnt.ID), js))

		err = txn.CommitWith(sampleIndexBuilder, nil)
		fmt.Println(err)
	}()

	// Output:
	// <nil>
	// <nil>
	// <nil>
}

func ExampleQueryIndex() {
	db := createDB("", false)
	defer db.Close()

	check := func(err interface{}) {
		if err == nil {
			return
		}
		panic(err)
	}

	indexTags := NewIndex("tags", func(key, val []byte) (entries []IndexEntry, err error) {
		var p comment
		if err := json.Unmarshal(val, &p); err != nil {
			return nil, err
		}
		if len(p.Tags) == 0 {
			return
		}
		for _, v := range p.Tags {
			entries = append(entries, IndexEntry{Index: []byte(v)})
		}
		return
	})

	indexBy := NewIndex("by", func(key, val []byte) (entries []IndexEntry, err error) {
		var p comment
		if err := json.Unmarshal(val, &p); err != nil {
			return nil, err
		}
		if p.By == "" {
			return
		}
		entries = append(entries, IndexEntry{Index: []byte(p.By)})
		return
	})

	sampleIndexBuilder := func(txn *layer.Txn, entries map[string][]byte) error {
		for k, v := range entries {
			// all indexes must be built here,
			// based on document type (v), etc, etc.

			if err := Emit(txn, indexTags, []byte(k), v); err != nil {
				return err
			}
			if err := Emit(txn, indexBy, []byte(k), v); err != nil {
				return err
			}
		}
		return nil
	}

	func() {
		cmnt := comment{
			ID:   "CMNT::001",
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"nosql"},
		}
		js, err := json.Marshal(cmnt)
		check(err)

		txn := db.NewTransaction(true)
		defer txn.Discard()

		check(txn.Set([]byte(cmnt.ID), js))

		err = txn.CommitWith(sampleIndexBuilder, nil)
		check(err)
	}()

	func() {
		cmnt := comment{
			ID:   "CMNT::002",
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"nosql", "golang"},
		}
		js, err := json.Marshal(cmnt)
		check(err)

		txn := db.NewTransaction(true)
		defer txn.Discard()

		check(txn.Set([]byte(cmnt.ID), js))

		err = txn.CommitWith(sampleIndexBuilder, nil)
		check(err)
	}()

	func() {
		got := make(map[string]Res)
		err := db.View(func(txn *layer.Txn) error {
			r, _, err := QueryIndex(Q{Index: "tags", Start: []byte("nosql"), Prefix: []byte("nosql")}, txn)
			if err != nil {
				return err
			}
			for _, v := range r {
				got[string(v.Key)] = v
			}
			return nil
		})
		check(err)
		fmt.Println(len(got))
		var keys []string
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := got[k]
			fmt.Println(k)
			fmt.Println(string(v.Key))
			fmt.Println(string(v.Index))
		}
	}()

	func() {
		got := make(map[string]Res)
		err := db.View(func(txn *layer.Txn) error {
			r, _, err := QueryIndex(Q{Index: "tags", Start: []byte("golang"), Prefix: []byte("golang")}, txn)
			if err != nil {
				return err
			}
			for _, v := range r {
				got[string(v.Key)] = v
			}
			return nil
		})
		check(err)
		fmt.Println(len(got))
		var keys []string
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := got[k]
			fmt.Println(k)
			fmt.Println(string(v.Key))
			fmt.Println(string(v.Index))
		}
	}()

	func() {
		got := make(map[string]Res)
		err := db.View(func(txn *layer.Txn) error {
			r, _, err := QueryIndex(Q{Index: "by", Start: []byte("Frodo Baggins"), Prefix: []byte("Frodo Baggins")}, txn)
			if err != nil {
				return err
			}
			for _, v := range r {
				got[string(v.Key)] = v
			}
			return nil
		})
		check(err)
		fmt.Println(len(got))
		var keys []string
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := got[k]
			fmt.Println(k)
			fmt.Println(string(v.Key))
			fmt.Println(string(v.Index))
		}
	}()

	// Output:
	// 2
	// CMNT::001
	// CMNT::001
	// nosql
	// CMNT::002
	// CMNT::002
	// nosql
	// 1
	// CMNT::002
	// CMNT::002
	// golang
	// 2
	// CMNT::001
	// CMNT::001
	// Frodo Baggins
	// CMNT::002
	// CMNT::002
	// Frodo Baggins
}

func ExampleQueryIndex_with() {
	db := createDB("", false)
	defer db.Close()

	check := func(err interface{}) {
		if err == nil {
			return
		}
		panic(err)
	}

	indexTags := NewIndex("tags", func(key, val []byte) (entries []IndexEntry, err error) {
		var p comment
		if err := json.Unmarshal(val, &p); err != nil {
			return nil, err
		}
		if len(p.Tags) == 0 {
			return
		}
		for _, v := range p.Tags {
			entries = append(entries, IndexEntry{Index: []byte(v)})
		}
		return
	})

	indexBy := NewIndex("by", func(key, val []byte) (entries []IndexEntry, err error) {
		var p comment
		if err := json.Unmarshal(val, &p); err != nil {
			return nil, err
		}
		if p.By == "" {
			return
		}
		entries = append(entries, IndexEntry{Index: []byte(p.By)})
		return
	})

	sampleIndexBuilder := func(txn *layer.Txn, entries map[string][]byte) error {
		for k, v := range entries {
			// all indexes must be built here,
			// based on document type (v), etc, etc.

			if err := Emit(txn, indexTags, []byte(k), v); err != nil {
				return err
			}
			if err := Emit(txn, indexBy, []byte(k), v); err != nil {
				return err
			}
		}
		return nil
	}

	func() {
		cmnt := comment{
			ID:   "CMNT::001",
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"nosql"},
		}
		js, err := json.Marshal(cmnt)
		check(err)

		err = db.UpdateWith(func(txn *layer.Txn) error {
			return txn.Set([]byte(cmnt.ID), js)
		}, sampleIndexBuilder)
		check(err)
	}()

	func() {
		cmnt := comment{
			ID:   "CMNT::002",
			By:   "Frodo Baggins",
			Text: "Hi!",
			At:   time.Now(),
			Tags: []string{"nosql", "golang"},
		}
		js, err := json.Marshal(cmnt)
		check(err)

		err = db.UpdateWith(func(txn *layer.Txn) error {
			return txn.Set([]byte(cmnt.ID), js)
		}, sampleIndexBuilder)
		check(err)
	}()

	func() {
		got := make(map[string]Res)
		err := db.View(func(txn *layer.Txn) error {
			r, _, err := QueryIndex(Q{Index: "tags", Start: []byte("nosql"), Prefix: []byte("nosql")}, txn)
			if err != nil {
				return err
			}
			for _, v := range r {
				got[string(v.Key)] = v
			}
			return nil
		})
		check(err)
		fmt.Println(len(got))
		var keys []string
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := got[k]
			fmt.Println(k)
			fmt.Println(string(v.Key))
			fmt.Println(string(v.Index))
		}
	}()

	func() {
		got := make(map[string]Res)
		err := db.View(func(txn *layer.Txn) error {
			r, _, err := QueryIndex(Q{Index: "tags", Start: []byte("golang"), Prefix: []byte("golang")}, txn)
			if err != nil {
				return err
			}
			for _, v := range r {
				got[string(v.Key)] = v
			}
			return nil
		})
		check(err)
		fmt.Println(len(got))
		var keys []string
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := got[k]
			fmt.Println(k)
			fmt.Println(string(v.Key))
			fmt.Println(string(v.Index))
		}
	}()

	func() {
		got := make(map[string]Res)
		err := db.View(func(txn *layer.Txn) error {
			r, _, err := QueryIndex(Q{Index: "by", Start: []byte("Frodo Baggins"), Prefix: []byte("Frodo Baggins")}, txn)
			if err != nil {
				return err
			}
			for _, v := range r {
				got[string(v.Key)] = v
			}
			return nil
		})
		check(err)
		fmt.Println(len(got))
		var keys []string
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := got[k]
			fmt.Println(k)
			fmt.Println(string(v.Key))
			fmt.Println(string(v.Index))
		}
	}()

	// Output:
	// 2
	// CMNT::001
	// CMNT::001
	// nosql
	// CMNT::002
	// CMNT::002
	// nosql
	// 1
	// CMNT::002
	// CMNT::002
	// golang
	// 2
	// CMNT::001
	// CMNT::001
	// Frodo Baggins
	// CMNT::002
	// CMNT::002
	// Frodo Baggins
}
