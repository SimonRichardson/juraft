package store

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2/txn"
)

// TxnStats provides information about a successfully applied transaction
type TxnStats struct {
	Assertions int
	Insertions int
	Updates    int
	Deletions  int

	Time struct {
		// The time spent waiting while other transactions were executing.
		Wait time.Duration

		// The time spent evaluating the transaction assertions.
		Assert time.Duration

		// The time spent applying the transaction operations.
		Apply time.Duration
	}
}

// String implements fmt.Stringer for the TxnStats type.
func (s TxnStats) String() string {
	return fmt.Sprintf(
		"assertions: %d, insertions: %d, updates: %d, deletions: %d, lock wait time: %s, assert time: %s, apply time: %s, total time: %s",
		s.Assertions, s.Insertions, s.Updates, s.Deletions,
		s.Time.Wait.String(), s.Time.Assert.String(), s.Time.Apply.String(),
		s.TotalTime().String(),
	)
}

// TotalTime returns the total time spent executing a transaction.
func (s TxnStats) TotalTime() time.Duration {
	return s.Time.Wait + s.Time.Assert + s.Time.Apply
}

// Store is implemented by types that can apply transactions and create/restore
// snapshots of their internal state.
type Store interface {
	// Open performs any required initialization logic to make the store
	// available for reads and writes.
	Open() error

	// Close cleanly shuts down the store.
	Close() error

	// Reset the contents of the store.
	Reset() error

	// Apply a transaction to the store.
	Apply([]txn.Op) (*TxnStats, error)

	// SaveSnapshot returns a snapshot of the current store contents.
	SaveSnapshot() ([]byte, error)

	// Restore the contents of the store from a previously captured snapshot.
	RestoreSnapshot([]byte) error
}
