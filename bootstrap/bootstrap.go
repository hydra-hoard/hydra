package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/fatih/color"
	"google.golang.org/grpc"
)

var (
	nodePort   = flag.Int("port", 5000, "The server port")
	jsonDBFile = flag.String("json_db_file", "../testdata/closest_nodes.json", "A json file containing a list of features")
)

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
	// pb.RegisterNodeDiscoveryServer(grpcServer, getDataStructure())
	// determine whether to use tls

	// time out for cache is 1 hour
	// dhtUtil.InitDHT(2, 60)

	grpcServer.Serve(lis)

}

func main() {
	StartServer()
	// set up boootstrap server of given ip and port
	// look for possible connections with peers

	// once peer comes in, attach new peer id to peer and add it to your personal DHT
	// This bootstrap server support the find closest nodes call
	// Idea: THe peers can have user generated name to make it easy to viualise

	// get nice graph visualisation for go ?
	// make a website for visualiation of connected peers
}
