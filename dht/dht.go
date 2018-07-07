package dht

import (
	"context"
	"fmt"
	. "hydra-dht/constants"
	. "hydra-dht/nodedetails"
	pb "hydra-dht/protobuf/node"
	. "hydra-dht/structures"
	"log"
	"math/bits"
	"strconv"
	"time"

	"google.golang.org/grpc"
)

var (
	dht                DHT
	channels           IndexChannels
	cache              map[NodeID]CacheObject = make(map[NodeID]CacheObject)
	bucketSize         int                    = 0
	cacheExpiryMinutes float64                = 1.0
)

// Appends to list of nodes of DHT's row
func addInDHT(n *Node, row int) {
	fmt.Println("Added node into DHT")
	fmt.Println(n)

	dht.Nodes[n.Key] = *n
	dht.Lists[row] = append(dht.Lists[row], n.Key)
	updateCache(n.Key, false)
}

// Replace in list of nodes of DHT's row
func replaceInDHT(n *Node, row int, replaced int) {
	deleteKey := dht.Lists[row][replaced]

	delete(dht.Nodes, deleteKey)
	delete(cache, deleteKey)

	dht.Nodes[n.Key] = *n
	dht.Lists[row][replaced] = n.Key
	updateCache(n.Key, false)
}

// get node LCinet sets up connection
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

// Ping node to check livliness, if no response till 5 seconds, return dead node
func Ping(n Node, pings chan int) {
	c := make(chan int, 1)

	go func() {
		hostname := n.Domain + ":" + strconv.Itoa(int(n.Port))
		client, conn := getNodeClient(&hostname)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), TIME_DURATION)
		defer cancel()
		livliness, err := client.Ping(ctx, &pb.Node{
			NodeId: MyNode.Key[:],
			Domain: n.Domain,
			Port:   int32(n.Port),
		})
		ob := CacheObject{LastTime: time.Now(), Dead: false}

		if err != nil {
			// log.Fatalf("%v.Ping(_) = _, %v: ", client, err)
			ob.Dead = true
			cache[n.Key] = ob
		} else if livliness.Alive {
			// dead node false
			cache[n.Key] = ob
		} else {
			// dead node true
			ob.Dead = true
			cache[n.Key] = ob
		}
		c <- 1
	}()

	select {
	case <-c:
		pings <- 1

	case <-time.After(TIME_DURATION):
		ob := CacheObject{LastTime: time.Now(), Dead: true}
		ob.Dead = false
		cache[n.Key] = ob
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
		if cache[dht.Lists[row][i]].Dead == true {
			return true, i
		}
	}
	return false, -1
}

// isNodeOld checks if node has expired in cache
func isNodeOld(id NodeID) bool {
	if time.Since(cache[id].LastTime).Minutes() >= cacheExpiryMinutes {
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
		id := dht.Lists[row][i]

		if isNodeOld(id) {
			go Ping(dht.Nodes[id], pings)
		} else {
			pings <- 1
		}
	}

	//waiting for mergeAllPings to complete
	<-final

	// return index of dead node
	dead, i = checkForDeadNodes(row)
	if dead {
		return i, ping
	}
	return -1, ping
}

//DHT listeners listens for add node requests for a particular i
// i denotes a row of the DHT
func DHTListeners(i int) {
	for {
		nodePacket := <-channels.WriteChannel[i]
		response := AddNodeResponse{Ping: false, Input: false, ListIndex: i}

		n := &(nodePacket.Node)

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

		// sends back the response to client
		nodePacket.NodeResponse <- response
	}
}

// Sends value of node over to a particular row listener of DHT
func routeToDHTRow(nodePacket *NodePacket, row int) {
	channels.WriteChannel[row] <- nodePacket
}

func checkIfNew(n *Node) bool {
	_, ok := dht.Nodes[n.Key]
	if ok != true {
		return true
	}
	return false
}

func getRowNum(n *Node) int {
	for i := 0; i < NUM_BYTES; i++ {
		val := uint8(MyNode.Key[i] ^ n.Key[i])
		if val != 0 {
			return 8*i + bits.LeadingZeros8(val)
		}
	}
	return 1
}

// Updates value in cache to signify nodes livliness status
func updateCache(id NodeID, status bool) {
	cache[id] = CacheObject{LastTime: time.Now(), Dead: status}
}

// compute verifies the node so that it doesn't add an already inserted node
func compute(nodePacket *NodePacket) {
	n := &nodePacket.Node
	if checkIfNew(n) {

		row := getRowNum(n)
		routeToDHTRow(nodePacket, row)

	} else {
		updateCache(n.Key, false)
		response := AddNodeResponse{
			ListIndex: -1,
			Ping:      false,
			Input:     false,
		}
		nodePacket.NodeResponse <- response
	}

}

func computeByte(key string) (uint8, error) {
	i, err := strconv.ParseUint(key, 2, 8)
	return uint8(i), err
}

// AddNode Adds the node into DHT and return Response indicating status
func AddNode(domain string, port int, firstNodeIdByteString string) (chan AddNodeResponse, error) {

	firstByte, err := computeByte(firstNodeIdByteString)
	if err != nil {
		return nil, err
	}

	nodeResponse := make(chan AddNodeResponse)
	value := NodePacket{
		Node: Node{
			Domain: domain,
			Port:   port,
			Key:    NodeID{firstByte, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124},
		},
		NodeResponse: nodeResponse,
	}

	compute(&value)

	return nodeResponse, err
}

func InitDHT(size int) {

	bucketSize = size
	// Init the DHT Map to keep track of duplicate nodes
	dht.Nodes = make(map[NodeID]Node)
	// Setting up DHT listeners
	for i := 0; i < HASH_SIZE; i++ {
		channels.WriteChannel[i] = make(chan *NodePacket)
		go DHTListeners(i)
	}
}
