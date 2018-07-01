package main

import (
	"log"
	"time"
)

var (
	serverKey = "110010"
	timeDuration = 5
)

type node struct {
	domain string
	port   string
	nodeId string
}

type nodeChannel struct {
	channel chan node
}

// DHT is the main Hash Table
type DHT struct {
	table       [][]node
	tableInputs []chan node
}

type cacheObject struct {
	lastTime time.Time
	dead     bool
}

type Cache struct {
	table [][]cacheObject
}

// ping node to check livliness, if no reponse till 5 seconds, return dead node
func ping(d_node node, cacheList *[]cacheObject, i int, pings chan int) {
	
	c := make(chan int, 1)

	go func() {
		var bool := pb.Ping(node)
		ob := cacheObject{lastTime: time.Now(), dead: true}
		if(bool) {
			// dead node false
			cacheList[i] = ob
		} else {
			// dead node true
			ob.dead = false
			cacheList[i] = ob
		}
		c <- 1 
	}()

	select {
		case <- c:
			pings <- 1

		case <- time.After(timeDuration*time.Second):
			ob := cacheObject{lastTime: time.Now(), dead: true}
			ob.dead = false
			cacheList[i] = ob
			pings <- 1
	}
}

// checks response for all nodes and returns after all nodes have responded
func mergeAllPings(final chan int, pings chan int) {
	for {
		i += <- pings

		if(final == 20) {
			final <- 1
			return
		}
	}
}

// checkForDeadNodes checks for dead nodes in cache
func checkForDeadNodes(cacheList *[]cacheObject) (bool,int){
	for j, d_node := range cache.table[i] {
		if d_node.dead == true {
			// indicate to all nodes to finsh their go functions
			return true,j
		}
	}

	return false,-1
}

/* 
   checkAndUpdateCache checks cache for dead nodes, if 
   not found update pings of all nodes. Then check for
   dead nodes, return index if any. Else return -1
*/
func checkAndUpdateCache(list []node, cacheList *[]cacheObject) int {
	
	var dead_nodes []node 
	time_now := time.Now()

	dead, i = checkForDeadNodes(cacheList)
	
	if(dead) {
		return i
	}

	var final chan int
	var pings chan int

	go mergeAllPings(final,pings)

	for j, d_node := range cacheList {
		if(time.Since(d_node.lastTime).Minutes() > timeDuration) {
			go ping(d_node,cacheList,j,pings)
		}
	}

	<- final

	// return index of dead node
	dead, i = checkForDeadNodes(cacheList)
	if(dead) {
		return i
	}

	return -1
}

// FinalAdd adds nodes into index i of DHT and updates cache
func FinalAdd(list chan node, i int, dht *DHT) {
	val := <-list
	// check size
	size := len(dht.table[i])

	// adds if size is good
	if size == 20 {
		j:= checkAndUpdateCache()
		if(j != -1) {
			add(dht,val,i,j)
		}
	} else if size < 20 {
		// just push into list
		push(dht, val, i)
	} else {
		log.Fatal("SIZE IS GREATER THAN 20 !!")
	}
}

func push(dht *DHT, val node, i int) {
	dht.table[i] = append(dht.table[i], val)
}

func add(dht *DHT, val node, i int,j int) {
	dht.table[i][j] = val
}


func getIndex(nodeID string) int {
	return 2
}

func main() {
	totalIndex := 5

	dht := new(DHT)

	for i := 0; i < totalIndex; i++ {
		go FinalAdd(dht.tableInputs[i], i, dht)
	}

	ue := node{domain: "127.0.0.1", port: "1100", nodeId: "11001"}
	index := getIndex(value.nodeId)

	// adds a new node to the DHT
	tableInputs[index] <- ue 

}