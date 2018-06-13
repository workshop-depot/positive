package kvw

import (
	"github.com/dgraph-io/badger"
)

var (
	// ErrValueLogSize is returned when opt.ValueLogFileSize option is not within the valid
	// range.
	ErrValueLogSize = badger.ErrValueLogSize

	// ErrValueThreshold is returned when ValueThreshold is set to a value close to or greater than
	// uint16.
	ErrValueThreshold = badger.ErrValueThreshold

	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = badger.ErrKeyNotFound

	// ErrTxnTooBig is returned if too many writes are fit into a single transaction.
	ErrTxnTooBig = badger.ErrTxnTooBig

	// ErrConflict is returned when a transaction conflicts with another transaction. This can happen if
	// the read rows had been updated concurrently by another transaction.
	ErrConflict = badger.ErrConflict

	// ErrReadOnlyTxn is returned if an update function is called on a read-only transaction.
	ErrReadOnlyTxn = badger.ErrReadOnlyTxn

	// ErrDiscardedTxn is returned if a previously discarded transaction is re-used.
	ErrDiscardedTxn = badger.ErrDiscardedTxn

	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = badger.ErrEmptyKey

	// ErrRetry is returned when a log file containing the value is not found.
	// This usually indicates that it may have been garbage collected, and the
	// operation needs to be retried.
	ErrRetry = badger.ErrRetry

	// ErrThresholdZero is returned if threshold is set to zero, and value log GC is called.
	// In such a case, GC can't be run.
	ErrThresholdZero = badger.ErrThresholdZero

	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = badger.ErrNoRewrite

	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after DB::Close has been called.
	ErrRejected = badger.ErrRejected

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = badger.ErrInvalidRequest

	// ErrManagedTxn is returned if the user tries to use an API which isn't
	// allowed due to external management of transactions, when using ManagedDB.
	ErrManagedTxn = badger.ErrManagedTxn

	// ErrInvalidDump if a data dump made previously cannot be loaded into the database.
	ErrInvalidDump = badger.ErrInvalidDump

	// ErrZeroBandwidth is returned if the user passes in zero bandwidth for sequence.
	ErrZeroBandwidth = badger.ErrZeroBandwidth

	// ErrInvalidLoadingMode is returned when opt.ValueLogLoadingMode option is not
	// within the valid range
	ErrInvalidLoadingMode = badger.ErrInvalidLoadingMode

	// ErrReplayNeeded is returned when opt.ReadOnly is set but the
	// database requires a value log replay.
	ErrReplayNeeded = badger.ErrReplayNeeded

	// ErrWindowsNotSupported is returned when opt.ReadOnly is used on Windows
	ErrWindowsNotSupported = badger.ErrWindowsNotSupported

	// ErrTruncateNeeded is returned when the value log gets corrupt, and requires truncation of
	// corrupt data to allow Badger to run properly.
	ErrTruncateNeeded = badger.ErrTruncateNeeded
)
