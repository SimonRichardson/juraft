package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/achilleasa/juraft/hastore"
	"github.com/achilleasa/juraft/mongoimport"
	"github.com/achilleasa/juraft/store"
	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	"github.com/hashicorp/go-hclog"
	"github.com/juju/errors"
	"github.com/urfave/cli/v2"
	"go.mongodb.org/mongo-driver/bson"
	"gopkg.in/yaml.v3"
)

func main() {
	app := &cli.App{
		Name:  "juraft",
		Usage: "A benchmark platform for applying mgo.Txns to HA in-memory store using Raft as the transaction coordinator",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "raft-port", Usage: "the port to listen for incoming raft connections (the cluster relay API will be exposed to raft-port+1 and a web/prom endpoint to raft-port+2)", Required: true},
			&cli.StringFlag{Name: "raft-node-id", Usage: "a unique ID for this raft node", Required: true},
			&cli.StringFlag{Name: "raft-data-dir", Value: "/tmp/juraft/data", Usage: "a folder to use for raft data. Data will be stored in a sub-folder named after the node ID"},
			&cli.StringFlag{Name: "raft-store-engine", Value: "boltdb", Usage: "the store engine to use for persisting raft data (supported values: in-memory, boltdb)"},
			&cli.StringFlag{Name: "cluster-api-endpoints", Usage: "a comma-delimited list of cluster API endpoints to connect to when joining the raft cluster; if not specified, and the node is not already part of a cluster, it will attempt to bootstrap a new cluster"},
		},
		Commands: []*cli.Command{
			{
				Name:  "serve",
				Usage: "expose the HA store",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "backend", Value: "dummy", Usage: "the type of backend to use. Ssupported backends: dummy"},
				},
				Action: exposeStore,
			},
			{
				Name:  "import-juju-db",
				Usage: "import a controller database from a mongo instance into the store and shut down (run inside a controller instance)",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "mongo-host", Value: "127.0.0.1:37017", Usage: "the address of a remote mongod instance to proxy connections to"},
					&cli.StringFlag{Name: "agent-conf", Value: "/var/lib/juju/agents/machine-0/agent.conf", Usage: "path to juju agent conf for extracting mongo credentials"},
				},
				Action: importMongoDB,
			},
		},
		Action: func(*cli.Context) error {
			return errors.New("please specify a command to execute")
		},
	}

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func exposeStore(cliCtx *cli.Context) error {
	logger := hclog.Default()
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGHUP)

	ha, err := openStore(cliCtx)
	if err != nil {
		return err
	}

	logger.Info("press ctrl+c to shutdown")
	for {
		s := <-sigCh
		if s == syscall.SIGINT {
			logger.Info("caught signal; shutting down", "signal", s)
			return ha.Close()
		}

		// Dump a snapshot of the store contents
		logger.Info("acquiring store snapshot")
		snapshotData, err := ha.SaveSnapshot()
		if err != nil {
			return err
		}
		logger.Info("acquired store snapshot", "size", humanize.Bytes(uint64(len(snapshotData))))
		logger.Info("dumping snapshot contents")
		snapshot := struct {
			Collections bson.M `bson:"collections"`
		}{}
		if err := bson.Unmarshal(snapshotData, &snapshot); err != nil {
			return err
		}
		spew.Dump(snapshot.Collections)
	}
}

func importMongoDB(cliCtx *cli.Context) error {
	// Grab mongo credentials
	f, err := os.Open(cliCtx.String("agent-conf"))
	if err != nil {
		return errors.Annotate(err, "unable to open juju agent configuration file")
	}
	defer func() { _ = f.Close() }()

	var creds = struct {
		User string `yaml:"tag"`
		Pass string `yaml:"statepassword"`
	}{}
	if err = yaml.NewDecoder(f).Decode(&creds); err != nil {
		return errors.Annotate(err, "unable to parse juju agent configuration")
	}

	// Open Store
	ha, err := openStore(cliCtx)
	if err != nil {
		return err
	}

	// Import data to our local store
	err = mongoimport.ImportJujuCollections(
		cliCtx.String("mongo-host"),
		creds.User,
		creds.Pass,
		ha,
		hclog.Default(),
	)
	if err != nil {
		_ = ha.Close()
		return errors.Annotate(err, "importing juju collections")
	}

	return ha.Close()
}

func openStore(cliCtx *cli.Context) (*hastore.HAStore, error) {
	raftPort := cliCtx.Int("raft-port")
	raftNodeID := cliCtx.String("raft-node-id")
	raftDataDir := cliCtx.String("raft-data-dir")
	raftStoreEngine := cliCtx.String("raft-store-engine")
	clusterAPIEndpoints := cliCtx.String("cluster-api-endpoints")

	var clusterAPIEndpointList []string
	if trimmedEndpointList := strings.TrimSpace(clusterAPIEndpoints); len(trimmedEndpointList) != 0 {
		clusterAPIEndpointList = strings.Split(trimmedEndpointList, ",")
	}

	ha := hastore.NewHAStore(hastore.Config{
		BackingStore:        store.NewInMemoryStore(),
		RaftNodeID:          raftNodeID,
		RaftPort:            raftPort,
		RaftDataDir:         raftDataDir,
		RaftStoreEngine:     hastore.RaftStoreEngineType(raftStoreEngine),
		ClusterAPIEndpoints: clusterAPIEndpointList,
		Logger:              hclog.Default(),
	})
	if err := ha.Open(); err != nil {
		return nil, errors.Annotate(err, "opening HA store")
	}

	return ha, nil
}
