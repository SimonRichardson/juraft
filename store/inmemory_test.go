package store

import (
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(InMemoryStoreTestSuite))

type InMemoryStoreTestSuite struct {
	StoreSuiteBase
}

func (s *InMemoryStoreTestSuite) SetUpTest(c *gc.C) {
	st := NewInMemoryStore()
	c.Assert(st.Open(), gc.IsNil)
	s.SetStore(st)
}

func (s *InMemoryStoreTestSuite) TearDownTest(c *gc.C) {
	c.Assert(s.store.Close(), gc.IsNil)
}
