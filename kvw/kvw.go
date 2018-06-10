// Package kvw wraps badger kv store for adding utilities.
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

	// Item .
	Item = badger.Item
)

var (
	// DefaultOptions .
	DefaultOptions = badger.DefaultOptions

	// DefaultIteratorOptions .
	DefaultIteratorOptions = badger.DefaultIteratorOptions

	// ErrEmptyKey .
	ErrEmptyKey = badger.ErrEmptyKey
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
	return newTxn(db.ManagedDB.NewTransactionAt(readTs, update))
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
	return newTxn(db.DB.NewTransaction(update))
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
		return fn(newTxn(btxn))
	})
}

// View .
func (db *DB) View(fn func(txn *Txn) error) error {
	return db.DB.View(func(btxn *badger.Txn) error {
		return fn(newTxn(btxn))
	})
}

//-----------------------------------------------------------------------------
// Txn

// Txn .
type Txn struct {
	*badger.Txn
	entries map[string][]byte
}

// Commit .
func (txn *Txn) Commit(callback func(error)) error { return txn.Txn.Commit(callback) }

// CommitAt .
func (txn *Txn) CommitAt(commitTs uint64, callback func(error)) error {
	return txn.Txn.CommitAt(commitTs, callback)
}

// Discard .
func (txn *Txn) Discard() { txn.Txn.Discard() }

// Get .
func (txn *Txn) Get(key []byte) (item *badger.Item, rerr error) { return txn.Txn.Get(key) }

// NewIterator .
func (txn *Txn) NewIterator(opt IteratorOptions) *badger.Iterator {
	return txn.Txn.NewIterator(opt)
}

// Delete .
func (txn *Txn) Delete(key []byte) error {
	if err := txn.Txn.Delete(key); err != nil {
		return err
	}
	txn.note(key, nil)
	return nil
}

// Set .
func (txn *Txn) Set(key, val []byte) error {
	if err := txn.Txn.Set(key, val); err != nil {
		return err
	}
	txn.note(key, val)
	return nil
}

// SetEntry .
func (txn *Txn) SetEntry(e *Entry) error {
	if err := txn.Txn.SetEntry(e); err != nil {
		return err
	}
	txn.note(e.Key, e.Value)
	return nil
}

// SetWithDiscard .
func (txn *Txn) SetWithDiscard(key, val []byte, meta byte) error {
	if err := txn.Txn.SetWithDiscard(key, val, meta); err != nil {
		return err
	}
	txn.note(key, val)
	return nil
}

// SetWithMeta .
func (txn *Txn) SetWithMeta(key, val []byte, meta byte) error {
	if err := txn.Txn.SetWithMeta(key, val, meta); err != nil {
		return err
	}
	txn.note(key, val)
	return nil
}

// SetWithTTL .
func (txn *Txn) SetWithTTL(key, val []byte, dur time.Duration) error {
	if err := txn.Txn.SetWithTTL(key, val, dur); err != nil {
		return err
	}
	txn.note(key, val)
	return nil
}

//-----------------------------------------------------------------------------

func newTxn(btxn *badger.Txn) (txn *Txn) {
	txn = &Txn{Txn: btxn, entries: make(map[string][]byte)}
	return
}

func (txn *Txn) note(key, val []byte) {
	if txn.entries == nil {
		return
	}
	txn.entries[string(key)] = val
	return
}

// CommitWith .
func (txn *Txn) CommitWith(beforeCommit BeforeCommit, callback func(error)) error {
	entries := txn.entries
	txn.entries = nil
	if err := beforeCommit(txn, entries); err != nil {
		return err
	}
	return txn.Txn.Commit(callback)
}

// CommitAtWith .
func (txn *Txn) CommitAtWith(commitTs uint64, beforeCommit BeforeCommit, callback func(error)) error {
	entries := txn.entries
	txn.entries = nil
	if err := beforeCommit(txn, entries); err != nil {
		return err
	}
	return txn.Txn.CommitAt(commitTs, callback)
}

//-----------------------------------------------------------------------------

// UpdateWith .
func (db *DB) UpdateWith(fn func(txn *Txn) error, beforeCommit BeforeCommit) error {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	if err := fn(txn); err != nil {
		return err
	}
	return txn.CommitWith(beforeCommit, nil)
}

// BeforeCommit .
type BeforeCommit func(*Txn, map[string][]byte) error

//-----------------------------------------------------------------------------
