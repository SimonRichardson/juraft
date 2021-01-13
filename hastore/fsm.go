package hastore

import (
	"io"
	"io/ioutil"

	"github.com/achilleasa/juraft/store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/juju/errors"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

var (
	_ raft.FSM = (*fsm)(nil)
)

type txnCmd struct {
	ID  string
	Ops []txn.Op
}

// fsm implements a suitable raft.FSM for applying transactions against a store.
type fsm struct {
	store  store.Store
	logger hclog.Logger
}

// Apply a raft log entry to the FSM.
func (f fsm) Apply(logEntry *raft.Log) interface{} {
	var cmd txnCmd
	if err := bson.Unmarshal(logEntry.Data, &cmd); err != nil {
		return errors.Annotate(err, "hastore fsm: unable to unmarshal txn operation list from raft log")
	}

	stats, err := f.store.Apply(cmd.Ops)
	if err != nil {
		return err
	}
	if f.logger != nil {
		f.logger.Debug("hastore fsm: applied txn", "ID", cmd.ID)
	}
	return stats
}

// Snapshot returns a snapshot of the underlying DB contents.
func (f fsm) Snapshot() (raft.FSMSnapshot, error) {
	snapshotData, err := f.store.SaveSnapshot()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &dbSnapshot{data: snapshotData}, nil
}

// Restore the DB contents from a snapshot.
func (f fsm) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()

	snapshotData, err := ioutil.ReadAll(rc)
	if err != nil {
		return errors.Annotate(err, "unable to read store snapshot")
	}

	if err = f.store.RestoreSnapshot(snapshotData); err != nil {
		return errors.Annotate(err, "unable to restore store snapshot")
	}
	return nil
}

type dbSnapshot struct {
	data []byte
}

func (s *dbSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}

	// Close the sink.
	if err := sink.Close(); err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (*dbSnapshot) Release() {}
