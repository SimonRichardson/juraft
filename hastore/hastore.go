package hastore

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/achilleasa/juraft/store"
	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/juju/errors"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

type RaftStoreEngineType string

const (
	RaftStoreEngineTypeInMemory = RaftStoreEngineType("in-memory")
	RaftStoreEngineTypeBoltDB   = RaftStoreEngineType("boltdb")
)

// Config encapsulates the configuration parameters for the HA store.
type Config struct {
	// A store instance used by the raft FSM for applying transactions.
	BackingStore store.Store

	// The ID of the raft node.
	RaftNodeID string

	// The port to listen for connections for raft nodes. The cluster grpc
	// API for leader discovery and transaction relaying will be exposed to
	// RaftPort+1.
	RaftPort int

	// The folder for persisting both raft snapshot and the raft log (the
	// latter only if not using the in-memory raft store). The actual data
	// will be stored in a subfolder named after the node's ID.
	RaftDataDir string

	// The type of store engine to use for persisting the raft log.
	RaftStoreEngine RaftStoreEngineType

	// A list of cluster API endpoints for known nodes in the raft cluster.
	// If not specified, and no raft log exists, a new raft cluster will be
	// bootstrapped. Otherwise, the store will establish connections to
	// each API endpoint and poll until the raft leader can be discovered.
	//
	// At that point, the leader will add this node as a voting member to
	// the cluster and the local node can begin replicating and applying
	// entries from the raft log.
	ClusterAPIEndpoints []string

	// An optional logger instance.
	Logger hclog.Logger
}

// HAStore converts a store.Store instance into a highly available store by
// using Raft to coordinate transaction application across multiple nodes.
type HAStore struct {
	cfg Config

	mu      sync.Mutex
	cluster *raftCluster
	raft    *raft.Raft
}

// NewHAStore creates a new HA store instance with the provided config.
func NewHAStore(cfg Config) *HAStore {
	return &HAStore{
		cfg: cfg,
	}
}

// Open performs any required initialization logic to make the store available
// for reads and writes. The implementation will also call Open on the
// underlying backing store. Calling Open on an already opened store is a no-op.
func (ha *HAStore) Open() error {
	ha.mu.Lock()
	defer ha.mu.Unlock()

	if ha.cluster != nil {
		return nil
	}

	if err := ha.cfg.BackingStore.Open(); err != nil {
		return errors.Annotate(err, "hastore: opening backing store")
	}

	ra, err := setupRaft(
		fsm{
			store:  ha.cfg.BackingStore,
			logger: ha.cfg.Logger,
		},
		ha.cfg.RaftNodeID, ha.cfg.RaftPort,
		ha.cfg.RaftDataDir, ha.cfg.RaftStoreEngine,
		ha.cfg.ClusterAPIEndpoints,
		ha.cfg.Logger,
	)
	if err != nil {
		return errors.Trace(err)
	}

	rc := newRaftCluster(ha.cfg.RaftNodeID, ha.cfg.RaftPort, ra, ha.cfg.BackingStore, ha.cfg.Logger)
	if err = rc.start(ha.cfg.ClusterAPIEndpoints); err != nil {
		_ = ra.Shutdown().Error()
		return errors.Trace(err)
	}
	ha.raft = ra
	ha.cluster = rc

	// Wait for a full log replay before allowing the store to be used
	_ = ra.Barrier(0).Error()

	if ha.cfg.Logger != nil {
		ha.cfg.Logger.Info("hastore: store has been initialized", "raft port", ha.cfg.RaftPort, "cluster API port", ha.cfg.RaftPort+1)
	}
	return nil
}

// Close cleanly shuts down both the HA store as well as the underlying backing
// store. Calling Close on an already-closed store is a no-op.
func (ha *HAStore) Close() error {
	ha.mu.Lock()
	defer ha.mu.Unlock()

	if ha.cluster != nil {
		if ha.cfg.Logger != nil {
			ha.cfg.Logger.Info("hastore: stopping cluster service")
		}
		ha.cluster.stop()
		if ha.cfg.Logger != nil {
			ha.cfg.Logger.Info("hastore: stopped cluster service")
		}
		ha.cluster = nil
	}

	if ha.raft != nil {
		if ha.cfg.Logger != nil {
			ha.cfg.Logger.Info("hastore: stopping raft subsystem")
		}
		if err := ha.raft.Shutdown().Error(); err != nil {
			return errors.Annotate(err, "hastore: shutting down raft subsystem")
		}
		if ha.cfg.Logger != nil {
			ha.cfg.Logger.Info("hastore: stopped raft subsystem")
		}
		ha.raft = nil
	}

	return ha.cfg.BackingStore.Close()
}

// Apply a transaction to the store.
func (ha *HAStore) Apply(txnOps []txn.Op) (*store.TxnStats, error) {
	return ha.ApplyTxnWithTimeout(txnOps, 0)
}

func (ha *HAStore) ApplyTxnWithTimeout(txnOps []txn.Op, timeout time.Duration) (*store.TxnStats, error) {
	serializedCmd, err := bson.Marshal(txnCmd{
		ID:  uuid.New().String(),
		Ops: txnOps,
	})
	if err != nil {
		return nil, errors.Annotate(err, "hastore: marshaling txn operation list")
	}

	ha.mu.Lock()
	if ha.cluster == nil {
		return nil, errors.Errorf("hastore: store not opened")
	}
	ha.mu.Unlock()

	return ha.cluster.applyTxn(serializedCmd, timeout)
}

// Reset the contents of the store.
func (ha *HAStore) Reset() error { return ha.cfg.BackingStore.Reset() }

// SaveSnapshot returns a snapshot of the current store contents.
func (ha *HAStore) SaveSnapshot() ([]byte, error) { return ha.cfg.BackingStore.SaveSnapshot() }

// Restore the contents of the store from a previously captured snapshot.
func (ha *HAStore) RestoreSnapshot(snapshot []byte) error {
	return ha.cfg.BackingStore.RestoreSnapshot(snapshot)
}

func setupRaft(raftFSM raft.FSM, nodeID string, raftPort int, raftDataDir string, raftStoreType RaftStoreEngineType, clusterAPIEndpoints []string, logger hclog.Logger) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.LogLevel = "error"
	config.Logger = nil
	config.LogOutput = ioutil.Discard

	// Setup Raft communication.
	bindAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)
	advertiseAddr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, errors.Annotate(err, "hastore: resolving raft advertise address")
	}

	transport, err := raft.NewTCPTransport(bindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create the snapshot store
	nodeDataDir := filepath.Join(raftDataDir, nodeID)
	snapshotDir := filepath.Join(nodeDataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0777); err != nil {
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(snapshotDir, 1, os.Stderr)
	if err != nil {
		return nil, errors.Annotate(err, "hastore: initializing snapshot store")
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	switch raftStoreType {
	case RaftStoreEngineTypeInMemory:
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
		if logger != nil {
			logger.Warn("hastore: using in-memory store for raft logs; logs will be purged when the node shuts down")
		}
	case RaftStoreEngineTypeBoltDB:
		logDir := filepath.Join(nodeDataDir, "raft")
		if err := os.MkdirAll(logDir, 0777); err != nil {
			return nil, err
		}

		logFile := filepath.Join(logDir, "raft.db")
		boltDB, err := raftboltdb.NewBoltStore(logFile)
		if err != nil {
			return nil, errors.Annotatef(err, "hastore: initializing boltdb store for storing raft logs")
		}
		logStore = boltDB
		stableStore = boltDB
		if logger != nil {
			logger.Info("hastore: using boltdb store for raft logs", "log file", logFile)
		}
	default:
		return nil, errors.Errorf("hastore: unsupported raft store engine type %q", raftStoreType)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, raftFSM, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, errors.Annotate(err, "hastore: initializing raft")
	}

	if len(clusterAPIEndpoints) == 0 {
		// Check if the cluster is already bootstrapped
		configFuture := ra.GetConfiguration()
		if err := configFuture.Error(); err == nil && len(configFuture.Configuration().Servers) != 0 {
			return ra, nil
		}

		// Try to bootstrap cluster
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		if logger != nil {
			logger.Info("hastore: attempting to bootstrap raft cluster")
		}
		ra.BootstrapCluster(configuration)
	}

	return ra, nil
}
