package main

import (
	"context"
	"flag"
	pb "hydra-dht/protobuf/node"
	"log"
	"time"

	"google.golang.org/grpc"
)

var (
	serverAddress = flag.String("server_address", "127.0.0.1:10000", "The server address in the format of host:port")
)

// FindNodes gets the list of closest nodes.
func FindNodes(client pb.NodeDiscoveryClient, node *pb.Node) {
	log.Printf("Finding Nodes closest to : %v:%v", node.GetDomain(), node.GetPort())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	closerNodes, err := client.FindNodes(ctx, node)
	if err != nil {
		log.Fatalf("%v.FindNodes(_) = _, %v: ", client, err)
	}
	recievedNode := closerNodes.GetNodes()[0]

	log.Printf("Found Node : %v:%v", recievedNode.GetDomain(), recievedNode.GetPort())
}

func main() {

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(*serverAddress, opts...)

	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	defer conn.Close()

	// Testing for Client
	client := pb.NewNodeDiscoveryClient(conn)
	// nodeID := sha256.Sum256([]byte("hello world\n"))
	nodeID := "110010"
	FindNodes(client, &pb.Node{NodeId: nodeID, Port: 1000, Domain: "217.0.0.1"})

}
