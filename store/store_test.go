package store

import (
	"testing"

	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

// StoreSuiteBase defines a re-usable set of store-related tests that can
// be executed against any type that implements the Store interface.
type StoreSuiteBase struct {
	store Store
}

// SetStore configures the test-suite to run all tests against the provided store instance.
func (s *StoreSuiteBase) SetStore(store Store) {
	s.store = store
}

type assertionTest struct {
	descr  string
	txnOps []txn.Op
	expErr string
}

var (
	failingAssertionTests = []assertionTest{
		{
			descr: "check failing assertion for missing document when it is already present",
			txnOps: []txn.Op{
				{
					C:      "galaxies",
					Id:     "milky-way",
					Assert: txn.DocMissing,
				},
			},
			expErr: `.*DocMissing assertion failed for document.*`,
		},
		{
			descr: "check failing assertion for present document when it is missing",
			txnOps: []txn.Op{
				{
					C:      "galaxies",
					Id:     "missing-document",
					Assert: txn.DocExists,
				},
			},
			expErr: `.*DocExists assertion failed for document.*`,
		},
		{
			descr: "check failing field assertion specified as a bson.D",
			txnOps: []txn.Op{
				{
					C:  "galaxies",
					Id: "milky-way",
					Assert: bson.D{
						{Name: "some-key", Value: "some-value"},
						{Name: "object-key.another-key", Value: "bogus"},
					},
				},
			},
			expErr: `.*assertion failed for document \"milky-way\": field \"object-key\.another-key\": value does not match expectation`,
		},
		{
			descr: "check failing field assertion specified as a bson.M",
			txnOps: []txn.Op{
				{
					C:  "galaxies",
					Id: "milky-way",
					Assert: bson.M{
						"object-key.another-key": "bogus",
					},
				},
			},
			expErr: `.*assertion failed for document \"milky-way\": field \"object-key\.another-key\": value does not match expectation`,
		},
		{
			descr: "check failing field assertion using the $ne operator",
			txnOps: []txn.Op{
				{
					C:  "galaxies",
					Id: "milky-way",
					Assert: bson.D{
						// This is an example of juju's IsAlive assertions
						{
							Name: "some-key",
							Value: bson.D{
								{Name: "$ne", Value: "some-value"},
							},
						},
					},
				},
			},
			expErr: `.*assertion failed for document \"milky-way\": field \"some-key\": value does not match \$ne expectation`,
		},
		{
			descr: "check failing field assertion using the $eq operator",
			txnOps: []txn.Op{
				{
					C:  "galaxies",
					Id: "milky-way",
					Assert: bson.D{
						// This is an example of juju's IsAlive assertions
						{
							Name: "some-key",
							Value: bson.D{
								{Name: "$eq", Value: "bogus"},
							},
						},
					},
				},
			},
			expErr: `.*assertion failed for document \"milky-way\": field \"some-key\": value does not match \$eq expectation`,
		},
		{
			descr: "check failing field assertion using the $or operator",
			txnOps: []txn.Op{
				{
					C:  "galaxies",
					Id: "milky-way",
					Assert: bson.D{
						// This is an example of juju's IsAlive assertions
						{
							Name: "$or",
							Value: []bson.D{
								// Clause 1
								{{Name: "some-key", Value: "bogus"}},
								// Clause 2
								{{Name: "some-key", Value: bson.M{"$ne": "some-value"}}},
							},
						},
					},
				},
			},
			expErr: `.*assertion failed for document \"milky-way\": field \"\$or\": none of the \$or clauses were met`,
		},
	}

	passingAssertionTests = []assertionTest{
		{
			descr: "check passing field assertion using the $or operator; first predicate satisfied",
			txnOps: []txn.Op{
				{
					C:  "galaxies",
					Id: "milky-way",
					Assert: bson.D{
						// This is an example of juju's IsAlive assertions
						{
							Name: "$or",
							Value: []bson.D{
								// Clause 1; satisfied
								{{Name: "some-key", Value: "some-value"}},
								// Clause 2; failing
								{{Name: "some-key", Value: bson.M{"$ne": "some-value"}}},
							},
						},
					},
				},
			},
		},
		{
			descr: "check passing field assertion using the $or operator; second predicate satisfied",
			txnOps: []txn.Op{
				{
					C:  "galaxies",
					Id: "milky-way",
					Assert: bson.D{
						// This is an example of juju's IsAlive assertions
						{
							Name: "$or",
							Value: []bson.D{
								// Clause 1; failing
								{{Name: "some-key", Value: "bogus"}},
								// Clause 2; satisfied
								{{Name: "some-key", Value: bson.M{"$eq": "some-value"}}},
							},
						},
					},
				},
			},
		},
	}
)

func (s *StoreSuiteBase) TestAssertions(c *gc.C) {
	// Add a document to the store
	_, err := s.store.Apply([]txn.Op{
		{
			C:  "galaxies",
			Id: "milky-way",
			Insert: bson.M{
				"some-key": "some-value",
				"object-key": bson.M{
					"another-key": "another-value",
				},
			},
		},
	})
	c.Assert(err, gc.IsNil)

	assertionTests := append(
		append([]assertionTest(nil), failingAssertionTests...),
		passingAssertionTests...,
	)

	for i, spec := range assertionTests {
		c.Logf("test %d: %s", i, spec.descr)
		_, err := s.store.Apply(spec.txnOps)
		if spec.expErr == "" {
			c.Assert(err, gc.IsNil)
		} else {
			c.Assert(err, gc.ErrorMatches, spec.expErr)
		}
	}
}

func (s *StoreSuiteBase) TestInsert(c *gc.C) {
	// Add a document to the store
	s.assertDocsInserted(c, []txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocMissing,
			Insert: bson.M{
				"some-key": "some-value",
			},
		},
	})

	// Attempting to insert the same document twice should fail
	_, err := s.store.Apply([]txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocMissing,
			Insert: bson.M{
				"some-key": "some-value",
			},
		},
	})

	c.Assert(err, gc.ErrorMatches, `.*DocMissing assertion failed for document.*`)
}

func (s *StoreSuiteBase) TestUpdate(c *gc.C) {
	// Add a document to the store
	s.assertDocsInserted(c, []txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocMissing,
			Insert: bson.M{
				"some-key": "some-value",
			},
		},
	})

	// Update the document using a bson.M update payload
	_, err := s.store.Apply([]txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocExists,
			Update: bson.M{
				"$set": bson.M{
					"some-key": "another-value",
				},
			},
		},
	})
	c.Assert(err, gc.IsNil)
	_, err = s.store.Apply([]txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocExists,
			Update: bson.M{
				"$set": bson.D{
					{Name: "some-other-key", Value: "another-value"},
				},
			},
		},
	})

	// Update the document using a bson.D update payload
	_, err = s.store.Apply([]txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocExists,
			Update: bson.D{
				{
					Name: "$set",
					Value: bson.M{
						"some-key": "another-value",
					},
				},
			},
		},
	})
	c.Assert(err, gc.IsNil)
	_, err = s.store.Apply([]txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocExists,
			Update: bson.D{
				{
					Name:  "$set",
					Value: bson.D{{Name: "some-other-key", Value: "another-value"}},
				},
			},
		},
	})
	c.Assert(err, gc.IsNil)
}

func (s *StoreSuiteBase) TestDelete(c *gc.C) {
	// Add a document to the store
	s.assertDocsInserted(c, []txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocMissing,
			Insert: bson.M{
				"some-key": "some-value",
			},
		},
	})

	// Now delete the document
	stats, err := s.store.Apply([]txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocExists,
			Remove: true,
		},
	})
	c.Assert(err, gc.IsNil)
	c.Assert(stats.Insertions, gc.Equals, 0)
	c.Assert(stats.Updates, gc.Equals, 0)
	c.Assert(stats.Deletions, gc.Equals, 1)
}

func (s *StoreSuiteBase) TestReset(c *gc.C) {
	txnOps := []txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocMissing,
			Insert: bson.M{
				"some-key": "some-value",
			},
		},
	}

	// Add a document to the store.
	s.assertDocsInserted(c, txnOps)

	// Reset the store contents.
	err := s.store.Reset()
	c.Assert(err, gc.IsNil)

	// We should be able to insert the same document now that the store is empty.
	s.assertDocsInserted(c, txnOps)
}

func (s *StoreSuiteBase) TestSnapshots(c *gc.C) {
	txnOps := []txn.Op{
		{
			C:      "galaxies",
			Id:     "milky-way",
			Assert: txn.DocMissing,
			Insert: bson.M{
				"some-key": "some-value",
			},
		},
	}

	// Add a document to the store
	s.assertDocsInserted(c, txnOps)

	// Grab a snapshot
	snapshotData, err := s.store.SaveSnapshot()
	c.Assert(err, gc.IsNil)

	// Now reset the store and restore the snapshot
	err = s.store.Reset()
	c.Assert(err, gc.IsNil)

	err = s.store.RestoreSnapshot(snapshotData)
	c.Assert(err, gc.IsNil)

	// If we attempt to run the same transaction it should fail as the document
	// should have been restored from the snapshot.
	_, err = s.store.Apply(txnOps)
	c.Assert(err, gc.ErrorMatches, `.*DocMissing assertion failed for document.*`)
}

func (s *StoreSuiteBase) assertDocsInserted(c *gc.C, txnOps []txn.Op) {
	stats, err := s.store.Apply(txnOps)
	c.Assert(err, gc.IsNil)
	c.Assert(stats.Insertions, gc.Equals, len(txnOps))
	c.Assert(stats.Updates, gc.Equals, 0)
	c.Assert(stats.Deletions, gc.Equals, 0)
}

func Test(t *testing.T) { gc.TestingT(t) }
