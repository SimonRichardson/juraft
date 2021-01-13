package store

import (
	gc "gopkg.in/check.v1"
	"gopkg.in/mgo.v2/bson"
)

var _ = gc.Suite(new(BSONUtilsTestSuite))

type BSONUtilsTestSuite struct {
}

func (s *BSONUtilsTestSuite) TestBSONSetPath(c *gc.C) {
	specs := []struct {
		descr  string
		path   string
		value  interface{}
		expDoc bson.M
		expErr string
	}{
		{
			descr: "set a missing key at the top level",
			path:  "new",
			value: "entry",
			expDoc: bson.M{
				"new":   "entry",
				"lorem": "ipsum",
				"foo": bson.M{
					"bar": "42",
					"baz": bson.M{
						"nested": true,
					},
				},
			},
		},
		{
			descr: "set an existing key at the top level",
			path:  "lorem",
			value: "overwrite",
			expDoc: bson.M{
				"lorem": "overwrite",
				"foo": bson.M{
					"bar": "42",
					"baz": bson.M{
						"nested": true,
					},
				},
			},
		},
		{
			descr: "set an existing key at the top level whose original value is a map",
			path:  "foo",
			value: "overwrite",
			expDoc: bson.M{
				"lorem": "ipsum",
				"foo":   "overwrite",
			},
		},
		{
			descr: "set a nested key that exists",
			path:  "foo.baz.nested",
			value: "overwrite",
			expDoc: bson.M{
				"lorem": "ipsum",
				"foo": bson.M{
					"bar": "42",
					"baz": bson.M{
						"nested": "overwrite",
					},
				},
			},
		},
		{
			descr: "set a nested key whose leaf segment does not exist",
			path:  "foo.new",
			value: "created",
			expDoc: bson.M{
				"lorem": "ipsum",
				"foo": bson.M{
					"new": "created",
					"bar": "42",
					"baz": bson.M{
						"nested": true,
					},
				},
			},
		},
		{
			descr: "set a nested key where none of its segments exist",
			path:  "my.new.nested.path",
			value: "created",
			expDoc: bson.M{
				"lorem": "ipsum",
				"foo": bson.M{
					"bar": "42",
					"baz": bson.M{
						"nested": true,
					},
				},
				"my": bson.M{
					"new": bson.M{
						"nested": bson.M{
							"path": "created",
						},
					},
				},
			},
		},
		{
			descr:  "set an nested key where a non-leaf segment is not a map",
			path:   "foo.bar.leaf",
			value:  "created",
			expErr: `cannot create path "leaf" in non-object field "bar"`,
		},
	}

	for i, spec := range specs {
		c.Logf("test %d: %s", i, spec.descr)

		doc := bson.M{
			"lorem": "ipsum",
			"foo": bson.M{
				"bar": "42",
				"baz": bson.M{
					"nested": true,
				},
			},
		}

		got, err := bsonSetPathRecursive(doc, spec.path, spec.value, false)
		if spec.expErr != "" {
			c.Assert(err, gc.ErrorMatches, spec.expErr)
			continue
		}

		c.Assert(err, gc.IsNil)
		c.Assert(got, gc.DeepEquals, spec.expDoc)
	}
}

func (s *BSONUtilsTestSuite) TestBSONGetPath(c *gc.C) {
	specs := []struct {
		descr    string
		path     string
		expValue interface{}
		expErr   string
	}{
		{
			descr:    "get an existing key at the top level",
			path:     "existing",
			expValue: "it exists",
		},
		{
			descr:  "get a missing key at the top level",
			path:   "not-existing",
			expErr: `field "not-existing" in path "not-existing" not found`,
		},
		{
			descr:    "get an existing key at a nested path",
			path:     "level0.level1.existing",
			expValue: "it exists at a nested path",
		},
		{
			descr:  "get a missing key at the the leaf of a path",
			path:   "level0.level1.not-existing",
			expErr: `field "not-existing" in path "level0.level1.not-existing" not found`,
		},
		{
			descr:  "get a missing key at an intermediate node of a path",
			path:   "level0.not-existing.foo",
			expErr: `field "not-existing" in path "level0.not-existing.foo" not found`,
		},
		{
			descr:  "get a key at a nested path where an intermediate node is not an object",
			path:   "level0.fish.sticks",
			expErr: `cannot visit path "sticks" in non-object field "fish" in path "level0.fish.sticks"`,
		},
	}

	for i, spec := range specs {
		c.Logf("test %d: %s", i, spec.descr)

		doc := bson.M{
			"existing": "it exists",
			"level0": bson.M{
				"fish": "sticks",
				"level1": bson.M{
					"existing": "it exists at a nested path",
				},
			},
		}

		got, err := bsonGetPathRecursive(doc, spec.path)
		if spec.expErr != "" {
			c.Assert(err, gc.ErrorMatches, spec.expErr)
			continue
		}

		c.Assert(err, gc.IsNil)
		c.Assert(got, gc.DeepEquals, spec.expValue)
	}
}

func (s *BSONUtilsTestSuite) TestApplyMongoUpdateErrors(c *gc.C) {
	doc := bson.M{
		"lorem": "ipsum",
		"foo": bson.M{
			"bar": "42",
			"baz": bson.M{
				"nested": true,
			},
		},
	}
	specs := []struct {
		descr  string
		update interface{}
		expErr string
	}{
		{
			descr:  "update is not bson.D or bson.M",
			update: "bogus",
			expErr: `.*update argument should be bson\.D or bson\.M`,
		},
		{
			descr: "update is bson.D and contains unknown mongo update operator",
			update: bson.D{{
				Name:  "$bogus",
				Value: "",
			}},
			expErr: `.*unknown/unsupported mongo field update operator "\$bogus"`,
		},
		{
			descr: "update is bson.M and contains unknown mongo update operator",
			update: bson.M{
				"$bogus": "",
			},
			expErr: `.*unknown/unsupported mongo field update operator "\$bogus"`,
		},
		{
			descr: "$set with non-BSON argument",
			update: bson.M{
				"$set": "bogus",
			},
			expErr: `.*\$set argument must be a bson\.M or bson\.D value`,
		},
		{
			descr: "$set a path where a non-leaf segment is not an object",
			update: bson.M{
				"$set": bson.M{
					"foo.bar.baz": "overwrite!",
				},
			},
			expErr: `.*cannot create path "baz" in non-object field "bar"`,
		},
	}

	for i, spec := range specs {
		c.Logf("test %d: %s", i, spec.descr)

		// Run in dry-run mode so we catch errors but don't mutate the actual doc.
		_, err := applyMongoUpdate(doc, spec.update, true)
		c.Assert(err, gc.ErrorMatches, spec.expErr)
	}
}
