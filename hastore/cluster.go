package hastore

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/achilleasa/juraft/hastore/clusterproto"
	"github.com/achilleasa/juraft/store"
	"github.com/golang/protobuf/ptypes"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/juju/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Raft defines an API for controlling the raft subsystem.
type Raft interface {
	Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture
	State() raft.RaftState
	Leader() raft.ServerAddress
	GetConfiguration() raft.ConfigurationFuture
	AddVoter(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	RegisterObserver(*raft.Observer)
	DeregisterObserver(*raft.Observer)
}

type raftCluster struct {
	logger   hclog.Logger
	nodeID   string
	raft     Raft
	raftPort int
	store    store.Store

	cancelClusterCtxFn func()
	workerWg           sync.WaitGroup

	mu                       sync.RWMutex
	leaderClusterAPIEndpoint string
	leaderClusterAPIConn     *grpc.ClientConn
	leaderClusterAPIClient   clusterproto.ClusterClient
}

func newRaftCluster(nodeID string, raftPort int, ra Raft, st store.Store, logger hclog.Logger) *raftCluster {
	return &raftCluster{
		logger:   logger,
		nodeID:   nodeID,
		raft:     ra,
		raftPort: raftPort,
		store:    st,
	}
}

func (rc *raftCluster) start(clusterAPIEndpoints []string) error {
	// Start gprc server for the cluster API
	clusterPort := rc.raftPort + 1
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", clusterPort))
	if err != nil {
		return errors.Annotatef(err, "hastore cluster: listening for grpc connections for the cluster service on port %d", clusterPort)
	}
	gSrv := grpc.NewServer()
	clusterproto.RegisterClusterServer(gSrv, rc)

	clusterCtx, cancelClusterCtxFn := context.WithCancel(context.Background())

	rc.workerWg.Add(2)
	go func() {
		defer rc.workerWg.Done()
		_ = gSrv.Serve(l)
	}()
	go func() {
		defer rc.workerWg.Done()
		<-clusterCtx.Done()
		gSrv.GracefulStop()
	}()

	// Watch raft cluster state and ensure that we are always connected to
	// the cluster API of the leader node.
	observerRegisteredCh := make(chan struct{})
	rc.workerWg.Add(1)
	go func() {
		defer rc.workerWg.Done()
		rc.watchRaftClusterStateate(clusterCtx, observerRegisteredCh)
	}()
	<-observerRegisteredCh

	// Now that the observer is registered, try to join the cluster and
	// wait for the leader to be detected.
	if err := rc.joinCluster(clusterCtx, clusterAPIEndpoints); err != nil {
		cancelClusterCtxFn()
		rc.workerWg.Wait()
		return errors.Annotate(err, "hastore cluster: joining raft cluster")
	}

	rc.cancelClusterCtxFn = cancelClusterCtxFn
	return nil
}

func (rc *raftCluster) stop() {
	rc.cancelClusterCtxFn()
	rc.workerWg.Wait()
}

func (rc *raftCluster) watchRaftClusterStateate(ctx context.Context, observerRegisteredCh chan struct{}) {
	// Check if the leader is already known and connect to its cluster API
	if leaderRaftAddr := rc.raft.Leader(); leaderRaftAddr != "" {
		if leaderClusterAPIEndpoint, err := raftAddrToClusterAddr(string(leaderRaftAddr)); err == nil {
			rc.connectToLeader(ctx, leaderClusterAPIEndpoint)
		}
	}

	// Observe cluster for leadership changes and ensure we always have a
	// cluster API connection to the leader so we can relay transactions
	// from follower nodes to the appropriate leader.
	observationCh := make(chan raft.Observation, 1)
	observer := raft.NewObserver(observationCh, true, func(o *raft.Observation) bool {
		_, isLeaderObservation := o.Data.(raft.LeaderObservation)
		return isLeaderObservation
	})
	rc.raft.RegisterObserver(observer)
	close(observerRegisteredCh)
	defer rc.raft.DeregisterObserver(observer)

	for {
		select {
		case <-ctx.Done():
			return
		case o := <-observationCh:
			leaderRaftAddr := string(o.Data.(raft.LeaderObservation).Leader)
			if leaderClusterAPIEndpoint, err := raftAddrToClusterAddr(leaderRaftAddr); err == nil {
				rc.connectToLeader(ctx, leaderClusterAPIEndpoint)
			}
		}
	}
}

func (rc *raftCluster) connectToLeader(ctx context.Context, leaderClusterAPIEndpoint string) {
	rc.mu.Lock()
	if rc.leaderClusterAPIConn != nil {
		if leaderClusterAPIEndpoint == rc.leaderClusterAPIEndpoint {
			rc.mu.Unlock()
			return // already connected to this leader
		}
		_ = rc.leaderClusterAPIConn.Close()
		rc.leaderClusterAPIClient = nil
	}
	rc.mu.Unlock()

	if rc.logger != nil {
		rc.logger.Info("hastore cluster: connecting to raft leader's cluster API", "endpoint", leaderClusterAPIEndpoint)
	}
	grpcConn, err := grpc.DialContext(ctx, leaderClusterAPIEndpoint, grpc.WithInsecure())
	if err != nil {
		// TODO: implement retry
		if rc.logger != nil {
			rc.logger.Error("hastore cluster: unable to connect to leader's cluster API at address: %s", leaderClusterAPIEndpoint)
		}
		return
	}

	rc.mu.Lock()
	rc.leaderClusterAPIEndpoint = leaderClusterAPIEndpoint
	rc.leaderClusterAPIConn = grpcConn
	rc.leaderClusterAPIClient = clusterproto.NewClusterClient(grpcConn)
	rc.mu.Unlock()
}

func (rc *raftCluster) joinCluster(ctx context.Context, clusterAPIEndpoints []string) error {
	if len(clusterAPIEndpoints) != 0 && rc.logger != nil {
		rc.logger.Info("hastore cluster: attempting to discover raft cluster leader via the cluster API", "endpoints", strings.Join(clusterAPIEndpoints, ", "))
	}

	for {
		// Check if we are asked to abort
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		// Check if we have been elected the leader or have connected to
		// the leader after observing its address
		rc.mu.RLock()
		connectedToLeader := rc.leaderClusterAPIConn != nil
		rc.mu.RUnlock()

		if rc.raft.State() == raft.Leader || connectedToLeader {
			return nil
		}

		// Establish a connection to the cluster API of each known peer
		// until we find one that knows the address of the leader. Then,
		// we add the leader to the known peer list and try to connect
		// to it and announce this node so that the leader can add us
		// to the raft cluster as voting members.
		for _, peerAddr := range clusterAPIEndpoints {
			dialCtx, cancelFn := context.WithTimeout(ctx, time.Second)
			grpcConn, err := grpc.DialContext(dialCtx, peerAddr, grpc.WithInsecure(), grpc.WithBlock())
			cancelFn()
			if err != nil {
				continue
			}

			clusterCli := clusterproto.NewClusterClient(grpcConn)
			res, err := clusterCli.GetRaftLeader(ctx, &clusterproto.GetLeaderReq{
				NodeId:   rc.nodeID,
				RaftPort: uint32(rc.raftPort),
			})
			if err != nil || res.LeaderAddr == "" {
				_ = grpcConn.Close()
				continue
			}

			leaderClusterAPIEndpoint, err := raftAddrToClusterAddr(res.LeaderAddr)
			if err != nil {
				_ = grpcConn.Close()
				return errors.Trace(err)
			}

			// If we invoked GetRaftLeader on the leader, it should
			// have added us as a voting member to the raft cluster
			// and the raft subsystem should be able to detect and
			// connect to all other cluster members behind the scenes.
			//
			// Reset the peer list so we poll until the raft cluster
			// settles down and a leader gets detected.
			if leaderClusterAPIEndpoint == peerAddr {
				rc.mu.Lock()
				rc.leaderClusterAPIEndpoint = peerAddr
				rc.leaderClusterAPIConn = grpcConn
				rc.leaderClusterAPIClient = clusterCli
				rc.mu.Unlock()
				return nil
			}

			// Replace the peer list with the leader address so we
			// connect to it on the next attempt.
			clusterAPIEndpoints = []string{leaderClusterAPIEndpoint}
			break
		}

		// Wait a bit and try again
		<-time.After(250 * time.Millisecond)
	}
}

// GetRaftLeader returns the raft address of the cluster leader.
func (r *raftCluster) GetRaftLeader(ctx context.Context, req *clusterproto.GetLeaderReq) (*clusterproto.GetLeaderRes, error) {
	peer, valid := peer.FromContext(ctx)
	if !valid {
		return nil, errors.BadRequestf("hastore cluster: unable to extract peer information from incoming request")
	}

	// If we are the leader, check if this node is a member of the raft
	// cluster and add it if needed.
	if err := r.maybeJoinNodeToRaftCluster(req.NodeId, peer.Addr, int(req.RaftPort)); err != nil {
		return nil, errors.Trace(err)
	}

	return &clusterproto.GetLeaderRes{
		// The returned address will be empty if the leader is not known
		LeaderAddr: string(r.raft.Leader()),
	}, nil
}

func (r *raftCluster) maybeJoinNodeToRaftCluster(nodeID string, clusterAddr net.Addr, raftPort int) error {
	// Only the leader can add nodes to the cluster.
	if r.raft.State() != raft.Leader {
		return nil
	}

	clusterTcpAddr, ok := clusterAddr.(*net.TCPAddr)
	if !ok {
		return errors.Errorf("hastore cluster: expected peer address to be a TCPAddr; got %#T", clusterTcpAddr)
	}

	// The raft port is always one less than the cluster port
	raftTcpAddr := *clusterTcpAddr
	raftTcpAddr.Port = raftPort
	raftAddr := raft.ServerAddress(raftTcpAddr.String())

	// Check whether the node is already known to the cluster.
	configFuture := r.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return errors.Trace(err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		matchedAddr := srv.Address == raftAddr
		matchedID := srv.ID == raft.ServerID(nodeID)
		if !matchedAddr && !matchedID {
			continue
		}

		// If both the address and ID matched the node is already present
		// in the cluster so we don't need to do anything more.
		if matchedAddr && matchedID {
			return nil
		}

		// We got a partial match. Most probably the address of the
		// node has changed. Remove the old node from the configuration
		if err := r.raft.RemoveServer(srv.ID, 0, 0).Error(); err != nil {
			return errors.Annotatef(err, "hastore cluster: removing stale entry for node %q (%s) from cluster configuration", srv.ID, srv.Address)
		}

		// Keep scanning the configuration for other stale entries
	}

	if err := r.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 0).Error(); err != nil {
		return errors.Annotatef(err, "hastore cluster: adding %q (%s) as a voting node", nodeID, raftAddr)
	}

	if r.logger != nil {
		r.logger.Info("hastore cluster: adding new voting node", "id", nodeID, "raft-endpoint", raftAddr)
	}
	return nil
}

// Apply a transaction if the node is the raft leader.
func (r *raftCluster) Apply(_ context.Context, req *clusterproto.ApplyTxnReq) (*clusterproto.ApplyTxnRes, error) {
	timeout, err := ptypes.Duration(req.Timeout)
	if err != nil {
		return nil, errors.Trace(err)
	}

	stats, err := r.applyTxnAsTheLeader(req.TxnData, timeout)
	if err != nil {
		return nil, errors.Annotate(err, "hastore cluster: apply transaction")
	}

	return &clusterproto.ApplyTxnRes{
		Assertions: int32(stats.Assertions),
		Insertions: int32(stats.Insertions),
		Updates:    int32(stats.Updates),
		Deletions:  int32(stats.Deletions),
		WaitTime:   ptypes.DurationProto(stats.Time.Wait),
		AssertTime: ptypes.DurationProto(stats.Time.Assert),
		ApplyTime:  ptypes.DurationProto(stats.Time.Apply),
	}, nil
}

func (c *raftCluster) applyTxn(serializedCmd []byte, timeout time.Duration) (*store.TxnStats, error) {
	// TODO: Catch ErrNotLeader when the cluster changes leadership while we
	// try to apply the txn to the old leader and retry the txn when we
	// connect to the new leader (or to us if we are the new leader).
	if c.raft.State() == raft.Leader {
		return c.applyTxnAsTheLeader(serializedCmd, timeout)
	}

	// Relay txn to leader and wait for response
	c.mu.RLock()
	clusterCli := c.leaderClusterAPIClient
	c.mu.RUnlock()

	res, err := clusterCli.Apply(context.Background(), &clusterproto.ApplyTxnReq{
		TxnData: serializedCmd,
		Timeout: ptypes.DurationProto(timeout),
	})
	if err != nil {
		return nil, err
	}

	var stats = store.TxnStats{
		Assertions: int(res.Assertions),
		Insertions: int(res.Insertions),
		Updates:    int(res.Updates),
		Deletions:  int(res.Deletions),
	}
	if stats.Time.Wait, err = ptypes.Duration(res.WaitTime); err != nil {
		return nil, errors.Annotate(err, "unmarshaling txn wait time field from leader response")
	}
	if stats.Time.Apply, err = ptypes.Duration(res.ApplyTime); err != nil {
		return nil, errors.Annotate(err, "unmarshaling txn apply time field from leader response")
	}
	if stats.Time.Assert, err = ptypes.Duration(res.AssertTime); err != nil {
		return nil, errors.Annotate(err, "unmarshaling txn assert time field from leader response")
	}

	return &stats, nil
}

func (c *raftCluster) applyTxnAsTheLeader(serializedCmd []byte, timeout time.Duration) (*store.TxnStats, error) {
	if c.raft.State() != raft.Leader {
		return nil, raft.ErrNotLeader
	}

	applyFuture := c.raft.Apply(serializedCmd, timeout)
	err := applyFuture.Error()

	// The FSM has attempted to apply the transaction. Check the result.
	if err == nil {
		fsmRes := applyFuture.Response()
		if fsmErr, isErr := fsmRes.(error); isErr {
			return nil, fsmErr
		}

		// Transaction was applied successfully.
		return fsmRes.(*store.TxnStats), nil
	}

	return nil, err
}

func raftAddrToClusterAddr(raftAddr string) (string, error) {
	host, port, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", errors.Annotate(err, "parsing raft addresss")
	}
	portNum, err := strconv.ParseUint(port, 10, 32)
	if err != nil {
		return "", errors.Annotate(err, "parsing port section of raft addresss")
	}

	// The raft port is always one less than the cluster port
	return fmt.Sprintf("%s:%d", host, portNum+1), nil
}
