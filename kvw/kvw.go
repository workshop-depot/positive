// Package kvw wraps badger kv store.
package kvw

import (
	"io"
	"time"

	"github.com/dgraph-io/badger"
)

//-----------------------------------------------------------------------------

type (
	// IteratorOptions .
	IteratorOptions = badger.IteratorOptions

	// Entry .
	Entry = badger.Entry

	// Options .
	Options = badger.Options

	// MergeFunc .
	MergeFunc = badger.MergeFunc
)

//-----------------------------------------------------------------------------
// ManagedDB

// ManagedDB .
type ManagedDB struct {
	*badger.ManagedDB
}

// OpenManaged .
func OpenManaged(opts Options) (db *ManagedDB, err error) {
	var bdb *badger.ManagedDB
	bdb, err = badger.OpenManaged(opts)
	if err != nil {
		return
	}
	db = &ManagedDB{ManagedDB: bdb}
	return
}

// GetSequence .
func (db *ManagedDB) GetSequence(_ []byte, _ uint64) (*badger.Sequence, error) {
	return db.ManagedDB.GetSequence(nil, 0)
}

// NewTransaction .
func (db *ManagedDB) NewTransaction(update bool) { db.ManagedDB.NewTransaction(update) }

// NewTransactionAt .
func (db *ManagedDB) NewTransactionAt(readTs uint64, update bool) *Txn {
	return &Txn{Txn: db.ManagedDB.NewTransactionAt(readTs, update)}
}

//-----------------------------------------------------------------------------
// DB

// DB .
type DB struct {
	*badger.DB
}

// Open .
func Open(opt Options) (db *DB, err error) {
	var bdb *badger.DB
	bdb, err = badger.Open(opt)
	if err != nil {
		return
	}
	db = &DB{DB: bdb}
	return
}

// Backup .
func (db *DB) Backup(w io.Writer, since uint64) (uint64, error) {
	return db.DB.Backup(w, since)
}

// Close .
func (db *DB) Close() (err error) { return db.DB.Close() }

// GetMergeOperator .
func (db *DB) GetMergeOperator(key []byte, f MergeFunc, dur time.Duration) *badger.MergeOperator {
	return db.DB.GetMergeOperator(key, f, dur)
}

// GetSequence .
func (db *DB) GetSequence(key []byte, bandwidth uint64) (*badger.Sequence, error) {
	return db.DB.GetSequence(key, bandwidth)
}

// Load .
func (db *DB) Load(r io.Reader) error { return db.DB.Load(r) }

// NewTransaction .
func (db *DB) NewTransaction(update bool) *Txn {
	return &Txn{Txn: db.DB.NewTransaction(update)}
}

// RunValueLogGC .
func (db *DB) RunValueLogGC(discardRatio float64) error {
	return db.DB.RunValueLogGC(discardRatio)
}

// Size .
func (db *DB) Size() (lsm int64, vlog int64) { return db.DB.Size() }

// Tables .
func (db *DB) Tables() []badger.TableInfo { return db.DB.Tables() }

// Update .
func (db *DB) Update(fn func(txn *Txn) error) error {
	return db.DB.Update(func(btxn *badger.Txn) error {
		return fn(&Txn{Txn: btxn})
	})
}

// View .
func (db *DB) View(fn func(txn *Txn) error) error {
	return db.DB.View(func(btxn *badger.Txn) error {
		return fn(&Txn{Txn: btxn})
	})
}

//-----------------------------------------------------------------------------
// Txn

// Txn .
type Txn struct {
	*badger.Txn
}

// Commit .
func (txn *Txn) Commit(callback func(error)) error { return txn.Txn.Commit(callback) }

// CommitAt .
func (txn *Txn) CommitAt(commitTs uint64, callback func(error)) error {
	return txn.Txn.CommitAt(commitTs, callback)
}

// Delete .
func (txn *Txn) Delete(key []byte) error { return txn.Txn.Delete(key) }

// Discard .
func (txn *Txn) Discard() { txn.Txn.Discard() }

// Get .
func (txn *Txn) Get(key []byte) (item *badger.Item, rerr error) { return txn.Txn.Get(key) }

// NewIterator .
func (txn *Txn) NewIterator(opt IteratorOptions) *badger.Iterator {
	return txn.Txn.NewIterator(opt)
}

// Set .
func (txn *Txn) Set(key, val []byte) error { return txn.Txn.Set(key, val) }

// SetEntry .
func (txn *Txn) SetEntry(e *Entry) error { return txn.Txn.SetEntry(e) }

// SetWithDiscard .
func (txn *Txn) SetWithDiscard(key, val []byte, meta byte) error {
	return txn.Txn.SetWithDiscard(key, val, meta)
}

// SetWithMeta .
func (txn *Txn) SetWithMeta(key, val []byte, meta byte) error {
	return txn.Txn.SetWithMeta(key, val, meta)
}

// SetWithTTL .
func (txn *Txn) SetWithTTL(key, val []byte, dur time.Duration) error {
	return txn.Txn.SetWithTTL(key, val, dur)
}

//-----------------------------------------------------------------------------
