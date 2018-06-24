package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	pb "protobuf/node"

	"google.golang.org/grpc"
)

var (
	port       = flag.Int("port", 10000, "The server port")
	jsonDBFile = flag.String("json_db_file", "../testdata/closest_nodes.json", "A json file containing a list of features")
)

// NodeServer is the stub for DHT
type NodeServer struct {
	savedNodes *pb.CloserNodes
}

// FindNodes finds closest nodes and returns the results
func (s *NodeServer) FindNodes(ctx context.Context, node *pb.Node) (*pb.CloserNodes, error) {
	// No feature was found,  returns an unnamed feature
	return &pb.CloserNodes{Nodes: s.savedNodes.Nodes}, nil
}

// loadFeatures loads features from a JSON file.
func (s *NodeServer) loadFeatures(filePath string) {
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to load default features: %v", err)
	}
	if err := json.Unmarshal(file, &s.savedNodes); err != nil {
		log.Fatalf("Failed to load default features: %v", err)
	}
}

// Returns a new server with stub data
func newServer() *NodeServer {
	s := &NodeServer{}
	s.loadFeatures(*jsonDBFile)
	return s
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeDiscoveryServer(grpcServer, newServer())
	// determine whether to use tls
	grpcServer.Serve(lis)
}
