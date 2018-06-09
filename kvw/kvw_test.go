package kvw

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mkdir(d string) {
	if err := os.MkdirAll(d, 0777); err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
}

func createDB(databaseDir string, deleteExisting bool) *DB {
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

	var opts = DefaultOptions
	opts.Dir = index
	opts.ValueDir = data
	preppedDB, err := Open(opts)
	if err != nil {
		panic(err)
	}

	return preppedDB
}

func Test01(t *testing.T) {
	require := require.New(t)

	sampleIndexBuilder := func(txn *Txn, entries map[string][]byte) error {
		for k, v := range entries {
			// all indexes must be built here,
			// based on document type (v), etc, etc.
			ix := "QQ:" + k
			if v == nil {
				require.NoError(txn.Delete([]byte(ix)))
			} else {
				require.NoError(txn.Set([]byte(ix), nil))
			}
		}
		return nil
	}

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
		err := db.View(func(txn *Txn) error {
			itr := txn.NewIterator(DefaultIteratorOptions)
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				got[string(item.Key())] = true
			}
			return nil
		})
		require.NoError(err)
		require.Equal(2, len(got))
		require.True(got["POST:001"])
		require.True(got["QQ:POST:001"])
	}()

	func() {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		require.NoError(txn.Delete([]byte("POST:001")))
		require.NoError(txn.CommitWith(sampleIndexBuilder, nil))
	}()

	func() {
		got := make(map[string]bool)
		err := db.View(func(txn *Txn) error {
			itr := txn.NewIterator(DefaultIteratorOptions)
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				got[string(item.Key())] = true
			}
			return nil
		})
		require.NoError(err)
		require.Equal(0, len(got))
		require.False(got["POST:001"])
		require.False(got["QQ:POST:001"])
	}()
}

func TestUpdateWith(t *testing.T) {
	require := require.New(t)

	sampleIndexBuilder := func(txn *Txn, entries map[string][]byte) error {
		for k, v := range entries {
			// all indexes must be built here,
			// based on document type (v), etc, etc.
			ix := "QQ:" + k
			if v == nil {
				require.NoError(txn.Delete([]byte(ix)))
			} else {
				require.NoError(txn.Set([]byte(ix), nil))
			}
		}
		return nil
	}

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

		err = db.UpdateWith(func(txn *Txn) error {
			return txn.Set([]byte(p.ID), js)
		}, sampleIndexBuilder)
		require.NoError(err)
	}()

	func() {
		got := make(map[string]bool)
		err := db.View(func(txn *Txn) error {
			itr := txn.NewIterator(DefaultIteratorOptions)
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				got[string(item.Key())] = true
			}
			return nil
		})
		require.NoError(err)
		require.Equal(2, len(got))
		require.True(got["POST:001"])
		require.True(got["QQ:POST:001"])
	}()

	func() {
		err := db.UpdateWith(func(txn *Txn) error {
			return txn.Delete([]byte("POST:001"))
		}, sampleIndexBuilder)
		require.NoError(err)

	}()

	func() {
		got := make(map[string]bool)
		err := db.View(func(txn *Txn) error {
			itr := txn.NewIterator(DefaultIteratorOptions)
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				got[string(item.Key())] = true
			}
			return nil
		})
		require.NoError(err)
		require.Equal(0, len(got))
		require.False(got["POST:001"])
		require.False(got["QQ:POST:001"])
	}()
}

func BenchmarkOneSimpleSecondaryIndex(b *testing.B) {
	sampleIndexBuilder := func(txn *Txn, entries map[string][]byte) error {
		for k, v := range entries {
			// all indexes must be built here,
			// based on document type (v), etc, etc.
			ix := "QQ:" + k
			if v == nil {
				txn.Delete([]byte(ix))
			} else {
				txn.Set([]byte(ix), nil)
			}
		}
		return nil
	}

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

	var counter int64
	b.Run("set", func(b *testing.B) {
		wg := &sync.WaitGroup{}
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				wg.Add(1)
				go func() {
					defer wg.Done()
					id := atomic.AddInt64(&counter, 1)
					p := &post{
						ID:   fmt.Sprintf("POST:%010d", id),
						By:   "Frodo Baggins",
						Text: "Awesome blog post!",
						At:   time.Now(),
						Tags: []string{"golang", "nosql"},
					}
					js, _ := json.Marshal(p)

					txn := db.NewTransaction(true)
					defer txn.Discard()

					txn.Set([]byte(p.ID), js)
					txn.CommitWith(sampleIndexBuilder, nil)
				}()
			}
		})
		wg.Wait()
	})

	func() {
		total := 0
		db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			itr := txn.NewIterator(opt)
			for itr.Rewind(); itr.Valid(); itr.Next() {
				total++
			}
			return nil
		})
		if total == 0 {
			b.Error("no set records")
		}
	}()

	counter = 0
	b.Run("delete", func(b *testing.B) {
		wg := &sync.WaitGroup{}
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				wg.Add(1)
				go func() {
					defer wg.Done()
					id := atomic.AddInt64(&counter, 1)
					p := &post{
						ID:   fmt.Sprintf("POST:%010d", id),
						By:   "Frodo Baggins",
						Text: "Awesome blog post!",
						At:   time.Now(),
						Tags: []string{"golang", "nosql"},
					}

					txn := db.NewTransaction(true)
					defer txn.Discard()

					txn.Delete([]byte(p.ID))
					txn.CommitWith(sampleIndexBuilder, nil)
				}()
			}
		})
		wg.Wait()
	})

	func() {
		total := 0
		db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			itr := txn.NewIterator(opt)
			for itr.Rewind(); itr.Valid(); itr.Next() {
				total++
			}
			return nil
		})
		if total != 0 {
			b.Error("no delete records")
		}
	}()
}

func BenchmarkTenSimpleSecondaryIndexes(b *testing.B) {
	b.ReportAllocs()

	sampleIndexBuilder := func(txn *Txn, entries map[string][]byte) error {
		for k, v := range entries {
			// all indexes must be built here,
			// based on document type (v), etc, etc.
			for i := 0; i < 10; i++ {
				ix := fmt.Sprintf("QQ:%03d"+k, i)
				if v == nil {
					txn.Delete([]byte(ix))
				} else {
					txn.Set([]byte(ix), nil)
				}
			}
		}
		return nil
	}

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

	var counter int64
	b.Run("set", func(b *testing.B) {
		wg := &sync.WaitGroup{}
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				wg.Add(1)
				go func() {
					defer wg.Done()
					id := atomic.AddInt64(&counter, 1)
					p := &post{
						ID:   fmt.Sprintf("POST:%010d", id),
						By:   "Frodo Baggins",
						Text: "Awesome blog post!",
						At:   time.Now(),
						Tags: []string{"golang", "nosql"},
					}
					js, _ := json.Marshal(p)

					txn := db.NewTransaction(true)
					defer txn.Discard()

					txn.Set([]byte(p.ID), js)
					txn.CommitWith(sampleIndexBuilder, nil)
				}()
			}
		})
		wg.Wait()
	})

	func() {
		total := 0
		db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			itr := txn.NewIterator(opt)
			for itr.Rewind(); itr.Valid(); itr.Next() {
				total++
			}
			return nil
		})
		if total == 0 {
			b.Error("no set records")
		}
	}()

	counter = 0
	b.Run("delete", func(b *testing.B) {
		wg := &sync.WaitGroup{}
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				wg.Add(1)
				go func() {
					defer wg.Done()
					id := atomic.AddInt64(&counter, 1)
					p := &post{
						ID:   fmt.Sprintf("POST:%010d", id),
						By:   "Frodo Baggins",
						Text: "Awesome blog post!",
						At:   time.Now(),
						Tags: []string{"golang", "nosql"},
					}

					txn := db.NewTransaction(true)
					defer txn.Discard()

					txn.Delete([]byte(p.ID))
					txn.CommitWith(sampleIndexBuilder, nil)
				}()
			}
		})
		wg.Wait()
	})

	func() {
		total := 0
		db.View(func(txn *Txn) error {
			opt := DefaultIteratorOptions
			opt.PrefetchValues = false
			itr := txn.NewIterator(opt)
			for itr.Rewind(); itr.Valid(); itr.Next() {
				total++
			}
			return nil
		})
		if total != 0 {
			b.Error("no delete records")
		}
	}()
}
