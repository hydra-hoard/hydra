package dht

import (
	"context"
	"fmt"
	pb "hydra-dht/protobuf/node"
	"log"
	"strconv"
	"time"

	"google.golang.org/grpc"
)

var (
	serverKey             = "11000"
	timeDuration          = 5 * time.Second
	timeDurationInMinutes = 5.0
	maxNodesInList        = 2
	keySize               = 5
	dht                   = DHT{
		tableInputs: make([]chan nodePacket, keySize),
		table:       make([][]node, keySize),
	}
	cache = Cache{
		table: make([][]cacheObject, keySize),
	}
)

// AddNodeResponse is the reponse sent from AddNodes call
// it returns the index in which the node was added
// ping returns true if pings were called
// input return true if node is added
type AddNodeResponse struct {
	ListIndex int
	Ping      bool
	Input     bool
}

type node struct {
	domain string
	port   int32
	nodeID string
}

type nodePacket struct {
	node         node
	nodeResponse chan AddNodeResponse
}

type nodeChannel struct {
	channel chan node
}

// DHT is the main Hash Table
type DHT struct {
	table       [][]node
	tableInputs []chan nodePacket
}

type cacheObject struct {
	lastTime time.Time
	dead     bool
}

// Cache stores the dead nodes cache
type Cache struct {
	table [][]cacheObject
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

// PingTest tests the Ping functionality
func PingTest(hostname string, dNode *pb.Node) (*pb.PingResponse, error) {
	client, conn := getNodeClient(&hostname)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeDuration)
	defer cancel()
	livliness, err := client.Ping(ctx, dNode)

	return livliness, err
}

// Ping node to check livliness, if no response till 5 seconds, return dead node
func Ping(dNode node, cacheList *[]cacheObject, i int, pings chan int) {
	c := make(chan int, 1)

	go func() {
		hostname := dNode.domain + ":" + strconv.Itoa(int(dNode.port))
		client, conn := getNodeClient(&hostname)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), timeDuration)
		defer cancel()
		livliness, err := client.Ping(ctx, &pb.Node{
			NodeId: dNode.nodeID,
			Domain: dNode.domain,
			Port:   dNode.port,
		})
		ob := cacheObject{lastTime: time.Now(), dead: false}

		if err != nil {
			// log.Fatalf("%v.Ping(_) = _, %v: ", client, err)
			ob.dead = true
			(*cacheList)[i] = ob
		} else if livliness.Alive {
			// dead node false
			(*cacheList)[i] = ob
		} else {
			// dead node true
			ob.dead = true
			(*cacheList)[i] = ob
		}
		c <- 1
	}()

	select {
	case <-c:
		pings <- 1

	case <-time.After(timeDuration):
		ob := cacheObject{lastTime: time.Now(), dead: true}
		ob.dead = false
		(*cacheList)[i] = ob
		pings <- 1
	}
}

// checks response for all nodes and returns after all nodes have responded
func mergeAllPings(final chan int, pings chan int) {
	i := 0
	for {
		i += <-pings
		if i == maxNodesInList {
			final <- 1
			return
		}
	}
}

// checkForDeadNodes checks for dead nodes in cache
func checkForDeadNodes(cacheList *[]cacheObject) (bool, int) {

	for j, dNode := range *cacheList {
		fmt.Println(dNode.dead)
		if dNode.dead == true {
			// indicate to all nodes to finsh their go functions
			return true, j
		}
	}
	return false, -1
}

/*
   checkAndUpdateCache checks cache for dead nodes, if
   not found update pings of all nodes. Then check for
   dead nodes, return index if any. Else return -1
*/
func checkAndUpdateCache(list *[]node, cacheList *[]cacheObject) (int, bool) {
	ping := false
	dead, i := checkForDeadNodes(cacheList)

	if dead {
		return i, ping
	}
	ping = true

	final := make(chan int)
	pings := make(chan int)

	go mergeAllPings(final, pings)

	for j := range *cacheList {
		go Ping((*list)[j], cacheList, j, pings)
		// if time.Since(dNode.lastTime).Minutes() > timeDurationInMinutes {
		// 	go Ping(list[j], cacheList, j, pings)
		// }
	}

	<-final
	// return index of dead node
	dead, i = checkForDeadNodes(cacheList)
	if dead {
		return i, ping
	}

	return -1, ping
}

// FinalAdd adds nodes into index i of DHT and updates cache
func FinalAdd(list *chan nodePacket, i int) {

	for {
		val := <-*list
		response := AddNodeResponse{Ping: false, Input: false, ListIndex: i}

		// check size
		size := len(dht.table[i])
		// adds if size is good
		if size == maxNodesInList {
			j, ping := checkAndUpdateCache(&dht.table[i], &cache.table[i])

			response.Ping = ping

			if j != -1 {
				add(val.node, i, j)
				response.Input = true
			}
		} else if size < maxNodesInList {
			// just push into list
			push(val.node, i)
			response.Input = true

		} else {
			log.Fatal("Size Is Greater Than Max Number of Nodes !!")
		}

		val.nodeResponse <- response
	}

}

func push(val node, i int) {
	fmt.Println("Node added !")
	cacheVal := cacheObject{lastTime: time.Now(), dead: false}
	dht.table[i] = append(dht.table[i], val)
	cache.table[i] = append(cache.table[i], cacheVal)
}

func add(val node, i int, j int) {
	fmt.Println("Node added !")
	cacheVal := cacheObject{lastTime: time.Now(), dead: false}
	dht.table[i][j] = val
	cache.table[i][j] = cacheVal
}

// getIndex gets index of list of nodes of DHT to get for given key
func getIndex(nodeID string) int {
	//TODO
	for i := 0; i < keySize; i++ {
		if nodeID[i] != serverKey[i] {
			return i
		}
	}
	return -1
}

// InitDHT Initialises the DHT and setups listeners
func InitDHT(bitSpace int) {

	fmt.Println("Setting up listeners ")
	keySize = bitSpace
	// setting up listeners
	for i := 0; i < keySize; i++ {
		dht.tableInputs[i] = make(chan nodePacket)
		go FinalAdd(&dht.tableInputs[i], i)
	}
}

// AddNode adds a new node into DHT
func AddNode(domain string, port int32, nodeID string) chan AddNodeResponse {

	nodeResponse := make(chan AddNodeResponse)
	value := nodePacket{node: node{
		domain: domain,
		port:   port,
		nodeID: nodeID,
	},
		nodeResponse: nodeResponse,
	}
	index := getIndex(value.node.nodeID)
	dht.tableInputs[index] <- value

	return nodeResponse
}
