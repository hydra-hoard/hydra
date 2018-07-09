package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	dhtUtil "hydra-dht/dht"
	pb "hydra-dht/protobuf/node"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"google.golang.org/grpc"
)

var (
	nodePort   = flag.Int("port", 10000, "The server port")
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
	fmt.Printf("I got a Ping from machine : %v:%d \n", node.Domain, node.Port)
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

// Returns the server data structure with stub data
func getDataStructure() *NodeServer {
	s := &NodeServer{}
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
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *nodePort))
	color.Red("Server listening at port : %d", *nodePort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeDiscoveryServer(grpcServer, getDataStructure())
	// determine whether to use tls
	dhtUtil.InitDHT(2)

	grpcServer.Serve(lis)

}

/*
StartCLI starts up the client CLI, it's functionality includes
1. Ping to check node of port number x is alive
2. Find Nodes for key k
*/
func StartCLI() {
	for {
		reader := bufio.NewReader(os.Stdin)
		color.Green("1. Ping a Node \n2. Add a Node Into HashTable")
		color.Blue("Enter an option: ")
		option, _ := reader.ReadString('\n')
		option = strings.TrimSpace(option)
		switch option {
		case "1":
			color.Blue("You selected Ping node ")
			color.Blue("Enter port number ")
			port, _ := reader.ReadString('\n')
			port = strings.TrimSpace(port)

			// livliness, _ := dhtUtil.PingTest("127.0.0.1:"+port, &pb.Node{
			// 	Domain: "127.0.0.1",
			// 	Port:   int32(*nodePort),
			// 	NodeId: "110010",
			// })
			//
			// color.HiCyan("Response was: %v", livliness.GetAlive())

		case "2":
			color.Blue("You selected Add Node option")
			color.Blue("Enter the node key ")
			nodeId, _ := reader.ReadString('\n')
			nodeId = strings.TrimSpace(nodeId)

			channel, _ := dhtUtil.AddNode("127.0.0.1", 80, nodeId)

			select {
			case actual := <-channel:
				fmt.Println(actual.ListIndex)
				fmt.Println(actual.Ping)
				fmt.Println(actual.Input)
			case <-time.After(time.Second * 1):
				fmt.Println("Time Out error")
			}
		}
	}
}

func main() {
	go StartServer()
	StartCLI()
}
