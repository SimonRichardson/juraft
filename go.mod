module github.com/achilleasa/juraft

go 1.14

require (
	github.com/armon/go-metrics v0.3.6 // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dustin/go-humanize v1.0.0
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.1.2
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/raft v1.2.0
	github.com/hashicorp/raft-boltdb v0.0.0-20171010151810-6e5ba93211ea
	github.com/juju/errors v0.0.0-20200330140219-3fe23663418f
	github.com/juju/testing v0.0.0-20201216035041-2be42bba85f3 // indirect
	github.com/urfave/cli/v2 v2.3.0
	go.mongodb.org/mongo-driver v1.4.4
	google.golang.org/grpc v1.34.0
	google.golang.org/protobuf v1.25.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c
)

replace gopkg.in/mgo.v2 => github.com/juju/mgo v2.0.0-20201106044211-d9585b0b0d28+incompatible
