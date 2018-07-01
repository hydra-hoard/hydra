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

func ping(d_node node,channel chan node,i int, pings chan int) {
	var bool := pb.Ping()

	if(bool) {
		channel <- d_node
	} else {
		// dead node 
		// update cache
		channel <- d_node
	}

	pings <- 1
}

func mergeAllPings(final chan int,pings chan int) {
	for {
		i += <- pings

		if(final == 20) {
			final <- 1
			return
		}
	}
}

func checkAndUpdateCache(list []node, cache Cache, i int) int {
	
	var dead_nodes []node 
	time_now := time.Now()
	for j, d_node := range cache.table[i] {
		// check for dead nodes
		if d_node.dead == true {
			return j
		}

		if(time.Since(d_node.lastTime).Minutes() > timeDuration) {
			dead_nodes = append(dead_nodes,d_node)
		}
	}

	var final chan int
	var pings chan int

	go mergeAllPings(final,pings)

	for j, d_node := range dead_nodes {
		// ping nodes, get response update table concurrently
		var channel chan node
		go ping(d_node,channel,pings)
		go store(channel,pings,list,j)
	}

	<- final
}

func finalAdd(list chan node, i int, dht *DHT) {
	val := <-list
	// check size
	size := len(dht.table[i])

	// add if size is good
	if size == 20 {
		checkAndUpdateCache()

		// else check cache

		//  update cache if needed

	} else if size < 20 {
		// just add
		add(dht, val, i)
	} else {
		log.Fatal("SIZE IS GREATER THAN 20 !!")
	}

}

func add(dht *DHT, val node, i int) {
	dht.table[i] = append(dht.table[i], val)
}

func getIndex(nodeID string) int {
	return 2
}

func main() {
	totalIndex := 5

	dht := new(DHT)

	for i := 0; i < totalIndex; i++ {
		go finalAdd(dht.tableInputs[i], i, dht)
	}

	value := node{domain: "127.0.0.1", port: "1100", nodeId: "11001"}
	index := getIndex(value.nodeId)

	go add(dht, value, index)

}


3 lions, 3 buffaloes - L

1 lion, | 0 buffulo -r

2 lions , 3 buffaLOES - L

2 LIONS = range

3 BUFFLOES, 1 LION, 

2 BUFFULOES, 2 LIONS 

I LION, I BUFFULO,

1 LION, 1 BUFFULO 






1 BUFFULO ON LION






