package dht

import (
	"context"
	"fmt"
	"hydra-dht/constants"
	nodedetails "hydra-dht/nodedetails"
	pb "hydra-dht/protobuf/node"
	structures "hydra-dht/structures"
	"log"
	"math/bits"
	"strconv"
	"time"

	"google.golang.org/grpc"
)

var (
	dht                structures.DHT
	channels           structures.IndexChannels
	cache              structures.Cache
	bucketSize         = 0
	cacheExpiryMinutes = 1.0
)

// Appends to list of nodes of DHT's row
func addInDHT(n *structures.Node, row int) {
	fmt.Println("Added node into DHT")
	fmt.Println(n)

	updateDHT(row, -1, n)
	updateCache(row, -1, false)
}

// Replace in list of nodes of DHT's row
func replaceInDHT(n *structures.Node, row int, replaced int) {

	updateDHT(row, replaced, n)
	updateCache(row, replaced, false)
}

// get node Client sets up connection
func getNodeClient(serverAddress *string) (pb.NodeDiscoveryClient, *grpc.ClientConn) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(*serverAddress, opts...)

	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	// Testing for Client
	client := pb.NewNodeDiscoveryClient(conn)
	return client, conn
}

//Ping makes a GRPC call to node and gets response
func Ping(n structures.Node) (*pb.PingResponse, error) {
	hostname := n.Domain + ":" + strconv.Itoa(int(n.Port))
	client, conn := getNodeClient(&hostname)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), constants.TIME_DURATION)
	defer cancel()
	livliness, err := client.Ping(ctx, &pb.Node{
		NodeId: nodedetails.MyNode.Key[:],
		Domain: n.Domain,
		Port:   int32(n.Port),
	})

	return livliness, err
}

// pingNode pings the node if no response till 5 seconds, return dead node
func pingNode(n structures.Node, pings chan int, row int, col int) {
	c := make(chan int, 1)

	go func() {
		livliness, err := Ping(n)
		dead := false

		if err != nil || !livliness.Alive {
			dead = true
		}
		updateCache(row, col, dead)
		c <- 1
	}()

	select {
	case <-c:
		pings <- 1

	case <-time.After(constants.TIME_DURATION):
		updateCache(row, col, false)
		pings <- 1
	}
}

// checks response for all nodes and returns after all nodes have responded
func mergeAllPings(final chan int, pings chan int) {
	i := 0
	for {
		i += <-pings
		if i == bucketSize {
			final <- 1
			return
		}
	}
}

// checkForDeadNodes checks if there are any dead nodes from previous pings
func checkForDeadNodes(row int) (bool, int) {
	for i := 0; i < len(dht.Lists[row]); i++ {
		if getCacheVal(row, i).Dead == true {
			return true, i
		}
	}
	return false, -1
}

// isNodeOld checks if node has expired in cache.
func isNodeOld(row int, col int) bool {

	if time.Since(getCacheVal(row, col).LastTime).Minutes() >= cacheExpiryMinutes {
		return true
	}
	return false
}

/*
   checkAndUpdateCache checks cache for dead nodes, if
   not found update pings of all nodes. Then check for
   dead nodes, return index if any. Else return -1
*/
func checkAndUpdateCache(row int) (int, bool) {
	ping := false
	dead, i := checkForDeadNodes(row)

	if dead {
		return i, ping
	}

	ping = true
	final := make(chan int)
	pings := make(chan int)

	go mergeAllPings(final, pings)

	for i := 0; i < len(dht.Lists[row]); i++ {

		if isNodeOld(row, i) {
			go pingNode(dht.Lists[row][i], pings, row, i)
		} else {
			pings <- 1
		}
	}

	//waiting for mergeAllPings to complete
	<-final

	// return index of dead node
	dead, i = checkForDeadNodes(row)
	fmt.Println(dead, i)
	if dead {
		return i, ping
	}
	return -1, ping
}

//Listeners listens for add node requests for a particular i
// i denotes a row of the DHT
func Listeners(i int) {
	for {
		nodePacket := <-channels.WriteChannel[i]
		response := structures.AddNodeResponse{Ping: false, Input: false, ListIndex: i}
		n := &(nodePacket.Node)
		new, j := checkIfNew(n, i)
		if new {
			if len(dht.Lists[i]) < bucketSize {
				addInDHT(n, i)
				response.Input = true
			} else {
				j, ping := checkAndUpdateCache(i)
				response.Ping = ping

				if j != -1 {
					replaceInDHT(n, i, j)
					response.Input = true
				}
			}
		} else {
			fmt.Println("Node exists!!")
			updateCache(i, j, false)
			response = structures.AddNodeResponse{
				ListIndex: -1,
				Ping:      false,
				Input:     false,
			}
		}
		nodePacket.NodeResponse <- response
	}
}

// Sends value of node over to a particular row listener of DHT
func routeToDHTRow(nodePacket *structures.NodePacket, row int) {
	channels.WriteChannel[row] <- nodePacket
}

func checkIfNew(n *structures.Node, row int) (bool, int) {
	for i := 0; i < len(dht.Lists[row]); i++ {
		existing := getDHTVal(row, i)
		if existing.Key == n.Key {
			return false, i
		}
	}
	return true, -1
}

// GetRowNum is used to find the list number where the node is to be stored in the DHT
// Node is the structure for the incoming node
func GetRowNum(n *structures.Node) int {
	for i := 0; i < constants.NUM_BYTES; i++ {
		val := uint8(nodedetails.MyNode.Key[i] ^ n.Key[i])
		if val != 0 {
			return 8*i + bits.LeadingZeros8(val)
		}
	}
	return 1
}

// Updates value in cache to signify nodes livliness status
func updateCache(row int, col int, status bool) {
	c := structures.CacheObject{LastTime: time.Now(), Dead: status}
	if col == -1 {
		cache.Lists[row] = append(cache.Lists[row], c)
	} else {
		cache.Lists[row][col] = c
	}
}

//storeDHT stores value into DHT. If col is -1 , it appends to the list of DHT
func updateDHT(row int, col int, n *structures.Node) {
	if col == -1 {
		dht.Lists[row] = append(dht.Lists[row], *n)
	} else {
		dht.Lists[row][col] = *n
	}
}

func getDHTVal(row int, col int) structures.Node {
	return dht.Lists[row][col]
}

func getCacheVal(row int, col int) structures.CacheObject {
	return cache.Lists[row][col]
}

// compute verifies the node so that it doesn't add an already inserted node
func compute(nodePacket *structures.NodePacket) {
	n := &nodePacket.Node
	row := GetRowNum(n)
	routeToDHTRow(nodePacket, row)
}

func computeByte(key string) (uint8, error) {
	i, err := strconv.ParseUint(key, 2, 8)
	return uint8(i), err
}

// AddNode Adds the node into DHT and return Response indicating status
func AddNode(domain string, port int, firstNodeIDByteString string) (chan structures.AddNodeResponse, error) {

	firstByte, err := computeByte(firstNodeIDByteString)
	if err != nil {
		return nil, err
	}

	nodeResponse := make(chan structures.AddNodeResponse)
	value := structures.NodePacket{
		Node: structures.Node{
			Domain: domain,
			Port:   port,
			Key:    structures.NodeID{firstByte, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124},
		},
		NodeResponse: nodeResponse,
	}

	go compute(&value)

	return nodeResponse, err
}

//InitDHT Initializes the Data structures required by the DHT
//size is the max number of nodes that can be saved in a list
func InitDHT(size int, timeoutForCache float64) {
	cacheExpiryMinutes = timeoutForCache
	bucketSize = size
	// Setting up DHT listeners
	for i := 0; i < constants.HASH_SIZE; i++ {
		channels.WriteChannel[i] = make(chan *structures.NodePacket)
		go Listeners(i)
	}
}
