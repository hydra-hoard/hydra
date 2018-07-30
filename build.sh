#install libraries
go get -d -v ./...
go get -u github.com/golang/protobuf/protoc-gen-go

# generate proto files
cd protobuf
sh proto.sh

cd persistance
mkdir log
mkdir dht

