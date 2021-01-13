package store

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

var _ Store = (*InMemoryStore)(nil)

type inMemoryCollection struct {
	Name  string
	Items map[string]bson.M
}

type InMemoryStore struct {
	mu          sync.RWMutex
	collections map[string]*inMemoryCollection
}

// NewInMemoryStore returns a Store implementation that maintains its entire
// state in memory.
func NewInMemoryStore() *InMemoryStore {
	// Apply a transaction to the store.
	return &InMemoryStore{
		collections: make(map[string]*inMemoryCollection),
	}
}

// Open performs any required initialization logic to make the store
// available for reads and writes.
func (s *InMemoryStore) Open() error { return nil }

// Close cleanly shuts down the store.
func (s *InMemoryStore) Close() error { return nil }

// Reset the contents of the store.
func (s *InMemoryStore) Reset() error {
	s.mu.Lock()
	s.collections = make(map[string]*inMemoryCollection)
	s.mu.Unlock()
	return nil
}

func (s *InMemoryStore) Apply(txnOps []txn.Op) (*TxnStats, error) {
	var stats TxnStats

	queueStartTime := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	stats.Time.Wait = time.Since(queueStartTime)

	if err := s.validateTransactionOps(txnOps, &stats); err != nil {
		return nil, errors.Annotate(err, "validating transaction")
	}

	s.applyTransactionOps(txnOps, &stats)
	return &stats, nil
}

// validateTransactionOps ensures that all transaction entries are valid and
// that any provided assertions are satisfied before the transaction can be
// applied to the store. This method must be called while holding a write lock
// on the store.
func (s *InMemoryStore) validateTransactionOps(txnOps []txn.Op, stats *TxnStats) error {
	assertStartTime := time.Now()

	for _, txnOp := range txnOps {
		if txnOp.C == "" || txnOp.Id == nil {
			return errors.BadRequestf("txn entry missing collection or document ID field")
		}
		docID, ok := txnOp.Id.(string)
		if !ok {
			return errors.BadRequestf("txn entry includes non-string document ID field")
		}

		var opCount int
		if txnOp.Remove {
			opCount++
		}
		if txnOp.Insert != nil {
			if !isBSON(txnOp.Insert) {
				return errors.BadRequestf("txn entry for document %q specifies an insert operation with a non-bson payload", docID)
			}
			opCount++
		}
		if txnOp.Update != nil {
			existingDoc, exists := s.getCollection(txnOp.C).Items[docID]
			if !exists {
				continue // document missing; this is a no-op
			}

			// Do a dry-run update attempt to check that the update
			// payload can be applied.
			if _, err := applyMongoUpdate(existingDoc, txnOp.Update, true); err != nil {
				return errors.Annotatef(err, "txn entry for document %q specifies an invalid update payload", docID)
			}
			opCount++
		}
		if opCount > 1 {
			return errors.BadRequestf("txn entry for document %q specifies multiple operations", docID)
		} else if opCount == 0 && txnOp.Assert == nil {
			return errors.BadRequestf("txn entry for document %q should either specify an operation or an assertion", docID)
		}

		// If an assertion is specified, make sure it is satisfied.
		if txnOp.Assert != nil {
			if err := s.checkAssertions(txnOp); err != nil {
				return errors.Annotate(err, "checking transaction assertions")
			}
			stats.Assertions++
		}
	}

	stats.Time.Assert = time.Since(assertStartTime)
	return nil
}

// checkAssertions ensures that all provided assertions are satisfied given the
// current snapshot of the store contents. This method must be called while
// holding a write lock on the store.
func (s *InMemoryStore) checkAssertions(txnOp txn.Op) error {
	collection := s.getCollection(txnOp.C)
	switch t := txnOp.Assert.(type) {
	case string: // shorthand assertion for existing/missing documents
		switch t {
		case txn.DocExists:
			docID := txnOp.Id.(string)
			if collection.Items[docID] == nil {
				return errors.Errorf("DocExists assertion failed for document %q", docID)
			}
		case txn.DocMissing:
			docID := txnOp.Id.(string)
			if collection.Items[docID] != nil {
				return errors.Errorf("DocMissing assertion failed for document %q", docID)
			}
		default:
			return errors.NotSupportedf("assertion: %#+v", txnOp.Assert)
		}
	case bson.D:
		docID := txnOp.Id.(string)
		doc := collection.Items[docID]
		if doc == nil {
			return errors.Errorf("assertion failed for document %q; document does not exist", docID)
		} else if err := s.checkDocAssertion(doc, t); err != nil {
			return errors.Annotatef(err, "assertion failed for document %q", docID)
		}
	case bson.M:
		docID := txnOp.Id.(string)
		doc := collection.Items[docID]
		if doc == nil {
			return errors.Errorf("assertion failed for document %q; document does not exist", docID)
		} else if err := s.checkDocAssertion(doc, t); err != nil {
			return errors.Annotatef(err, "assertion failed for document %q", docID)
		}
	default:
		return errors.NotSupportedf("assertion: %#+v", txnOp.Assert)
	}

	// All assertions were satisfied.
	return nil
}

func (s *InMemoryStore) checkDocAssertion(doc bson.M, docAssertion interface{}) error {
	switch t := docAssertion.(type) {
	case bson.D:
		for _, docElem := range t {
			if err := s.checkFieldAssertion(doc, docElem.Name, docElem.Value); err != nil {
				return errors.Annotatef(err, "field %q", docElem.Name)
			}
		}
	case bson.M:
		for assertionKey, assertionValue := range t {
			if err := s.checkFieldAssertion(doc, assertionKey, assertionValue); err != nil {
				return errors.Annotatef(err, "field %q", assertionKey)
			}
		}
	default:
		return errors.Errorf("unsupported doc assertion payload of type %T", t)
	}

	// Doc assertion was satisfied.
	return nil
}

func (s *InMemoryStore) checkFieldAssertion(doc bson.M, assertionKey string, assertion interface{}) error {
	switch assertionKey {
	case "$or":
		clauses, ok := assertion.([]bson.D)
		if !ok {
			return errors.Errorf("$or operator expects a []bson.D value")
		}

		// For the assertion to succeed, at least one of the $or
		// clauses must be satisfied.
		for _, clause := range clauses {
			if err := s.checkDocAssertion(doc, clause); err == nil {
				return nil
			}
		}

		return errors.Errorf("none of the $or clauses were met")
	default:
		val, err := bsonGetPathRecursive(doc, assertionKey)
		if err != nil {
			return err
		}

		switch assertVal := assertion.(type) {
		case bson.D: // assertion uses an operator
			if len(assertVal) != 1 {
				return errors.Errorf("expected assertion value of type bson.D to have a single entry")
			}
			docElem := assertVal[0]

			if err := s.assertOperatorMatch(docElem.Name, docElem.Value, val); err != nil {
				return err
			}
		case bson.M: // assertion uses an operator
			if len(assertVal) != 1 {
				return errors.Errorf("expected assertion value of type bson.M to have a single key")
			}
			for opName, opArg := range assertVal {
				if err := s.assertOperatorMatch(opName, opArg, val); err != nil {
					return err
				}
			}
		default: // assume assertVal is a scalar and compare the interfaces directly
			if val != assertVal {
				return errors.Errorf("value does not match expectation")
			}
		}
	}

	return nil
}

func (s *InMemoryStore) assertOperatorMatch(opName string, opArg, curVal interface{}) error {
	switch opName {
	case "$ne":
		if opArg == curVal {
			return errors.Errorf("value does not match $ne expectation")
		}
	case "$eq":
		if opArg != curVal {
			return errors.Errorf("value does not match $eq expectation")
		}
	default:
		return errors.Errorf("unsupported operator %q", opName)
	}
	return nil
}

// applyTransactionOps atomically applies the provided txn op list entries to
// the store.
func (s *InMemoryStore) applyTransactionOps(txnOps []txn.Op, stats *TxnStats) {
	applyStartTime := time.Now()

	for _, txnOp := range txnOps {
		collection := s.getCollection(txnOp.C)
		docID := txnOp.Id.(string)

		switch {
		case txnOp.Remove == true:
			delete(collection.Items, docID)
			stats.Deletions++
		case txnOp.Insert != nil:
			insertDoc := convertToBSONMap(txnOp.Insert)
			insertDoc["_id"] = docID          // ensure _id is always present
			insertDoc["txn-revno"] = int64(1) // inject a rev-no count
			collection.Items[docID] = insertDoc
			stats.Insertions++
		case txnOp.Update != nil:
			existingDoc, exists := collection.Items[docID]
			if !exists {
				continue // no-op; this should have been prevented through an assertion
			}

			// Fetch current rev-no and increment it. If it does not
			// exist in the doc we are about to update, assume a
			// rev-no value of 0 so we can increment it.
			curRevNo, _ := existingDoc["txn-revno"].(int64)

			// The pre-apply checks will catch malformed update payloads so we
			// don't need to check the error again here.
			existingDoc, _ = applyMongoUpdate(existingDoc, txnOp.Update, false)
			existingDoc["txn-revno"] = curRevNo + 1
			collection.Items[docID] = existingDoc
			stats.Insertions++
		}
	}

	stats.Time.Apply = time.Since(applyStartTime)
}

func (s *InMemoryStore) getCollection(colName string) *inMemoryCollection {
	collection, found := s.collections[colName]
	if !found {
		collection = &inMemoryCollection{
			Name:  colName,
			Items: make(map[string]bson.M),
		}
		s.collections[colName] = collection
	}

	return collection
}

// SaveSnapshot returns a snapshot of the current store contents.
func (s *InMemoryStore) SaveSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := struct {
		CreatedAt   time.Time                      `bson:"created-at"`
		Collections map[string]*inMemoryCollection `bson:"collections"`
	}{
		CreatedAt:   time.Now(),
		Collections: s.collections,
	}
	return bson.Marshal(snapshot)
}

// Restore the contents of the store from a previously captured snapshot.
func (s *InMemoryStore) RestoreSnapshot(snapshot []byte) error {
	snapshotContents := struct {
		Collections map[string]*inMemoryCollection `bson:"collections"`
	}{}

	if err := bson.Unmarshal(snapshot, &snapshotContents); err != nil {
		return errors.Annotate(err, "unmarshaling store snapshot")
	}

	s.mu.Lock()
	s.collections = snapshotContents.Collections
	s.mu.Unlock()
	return nil
}
