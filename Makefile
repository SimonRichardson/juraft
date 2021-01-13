build:
	go build -o juraft ./cmd

proto: ensure-proto-deps
	@echo "[protoc] generating protos for API"
	@protoc --go_out=plugins=grpc:hastore/clusterproto/ -Ihastore/clusterproto/ cluster.proto

ensure-proto-deps:
	@echo "[go get] ensuring protoc packages are available"
	@go get google.golang.org/grpc
	@go get github.com/golang/protobuf/protoc-gen-go
	
