package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	dhtUtil "hydra-dht/dht"
	"io/ioutil"
	"log"
	"net"
	"os"
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

// Ping checks whether the node is lively or not
func (s *NodeServer) Ping(ctx context.Context, node *pb.Node) (*pb.PingResponse, error) {

	fmt.Println("I got a Ping !!")
	return &pb.PingResponse{Alive: true}, nil
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

/*
StartServer starts up the server for node and takes in
1. Node key from the CLI
2. Number of nodes per list
3. Number of bits for node key
4. Timeout seconds for ping response
*/
func StartServer() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeDiscoveryServer(grpcServer, newServer())
	// determine whether to use tls
	grpcServer.Serve(lis)
	dhtUtil.InitDHT(5)
}

/*
StartCLI starts up the client CLI, it's functionality includes
1. Ping to check node of port number x is alive
2. Find Nodes for key k
*/
func StartCLI() {
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Enter an option: ")
		option, _ := reader.ReadString('\n')

		switch option {
		case "1":
			fmt.Println("You selected Ping node ")
			fmt.Println("Enter port number ")
			port, _ := reader.ReadString('\n')
			dhtUtil.PingTest("127.0.0.1:"+port, &pb.Node{
				Domain: "127.0.0.1",
				Port:   1000,
				NodeId: "110010",
			})

		}
		dhtUtil.AddNode("127.0.0.1", 1000, "110010")
	}
}

func main() {
	StartServer()
	StartCLI()
}
