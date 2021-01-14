package mongoimport

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/achilleasa/juraft/store"
	"github.com/hashicorp/go-hclog"
	"github.com/juju/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"gopkg.in/mgo.v2/txn"
)

func ImportJujuCollections(mongoHost, mongoUser, mongoPass string, st store.Store, logger hclog.Logger) error {
	mongoCli, err := getMongoClient(mongoHost, mongoUser, mongoPass)
	if err != nil {
		return errors.Annotate(err, "connecting to mongo")
	}
	defer mongoCli.Disconnect(context.Background())

	if err := importCollections(mongoCli, st, logger); err != nil {
		return errors.Annotate(err, "importing juju collections from mongo")
	}
	return nil
}

func importCollections(mongoCli *mongo.Client, st store.Store, logger hclog.Logger) error {
	ctx := context.Background()

	jujuDB := mongoCli.Database("juju")
	colNames, err := jujuDB.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return errors.Annotate(err, "listing collections")
	}
	if logger != nil {
		logger.Info("importing juju collections", "count", len(colNames))
	}

	var aggrStats store.TxnStats

nextCollection:
	for _, colName := range colNames {
		if strings.HasPrefix(colName, "txn") {
			if logger != nil {
				logger.Info("skipping blacklisted collection", "collection", colName)

			}
			continue
		}

		col := jujuDB.Collection(colName)

		var txnOps []txn.Op
		colCursor, err := col.Find(ctx, bson.D{})
		if err != nil {
			return errors.Annotatef(err, "fetching docs in collection %q", colName)
		}

		for colCursor.Next(ctx) {
			var doc bson.M
			if err = colCursor.Decode(&doc); err != nil {
				_ = colCursor.Close(ctx)
				return errors.Annotatef(err, "fetching doc in collection %q", colName)
			}

			op := txn.Op{
				C:      colName,
				Assert: txn.DocMissing,
				Insert: doc,
			}

			switch t := doc["_id"].(type) {
			case string:
				op.Id = t
			case primitive.ObjectID:
				op.Id = t.String()
			default:
				if err != nil {
					return errors.Annotatef(err, "skipping collection due to unsupported _id type", "collection", colName, "_id type", fmt.Sprintf("%T", t))
				}
				_ = colCursor.Close(ctx)
				continue nextCollection
			}

			txnOps = append(txnOps, op)
		}

		curErr := colCursor.Err()
		_ = colCursor.Close(ctx)

		if curErr != nil {
			return errors.Annotatef(err, "fetching docs in collection %q", colName)
		}

		if logger != nil {
			logger.Info("   - running insert txn for collection", "collection", colName, "doc-count", len(txnOps))
		}
		txnStats, err := st.Apply(txnOps)
		if err != nil {
			return errors.Annotatef(err, "applying txn for docs in collection %q", colName)
		}

		aggrStats.Assertions += txnStats.Assertions
		aggrStats.Insertions += txnStats.Insertions
		aggrStats.Updates += txnStats.Updates
		aggrStats.Deletions += txnStats.Deletions
		aggrStats.Time.Assert += txnStats.Time.Assert
		aggrStats.Time.Wait += txnStats.Time.Wait
		aggrStats.Time.Apply += txnStats.Time.Apply
	}
	if logger != nil {
		logger.Info("import complete", "stats", aggrStats)
	}
	return nil
}

func getMongoClient(mongoHost, mongoUser, mongoPass string) (*mongo.Client, error) {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
	}

	dialCtx, cancelDialFn := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDialFn()
	mongoCli, err := mongo.Connect(
		dialCtx,
		options.Client().
			SetAppName("juraft").
			SetTLSConfig(tlsConf).
			SetAuth(options.Credential{
				AuthMechanism: "SCRAM-SHA-1",
				AuthSource:    "admin",
				Username:      mongoUser,
				Password:      mongoPass,
			}).
			SetHosts([]string{mongoHost}),
	)
	if err != nil {
		return nil, err
	}

	if err = mongoCli.Ping(dialCtx, readpref.Primary()); err != nil {
		return nil, err
	}

	return mongoCli, nil
}
