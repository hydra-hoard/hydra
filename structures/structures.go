package structures

import (
	. "hydra-dht/constants"
	"time"
)

type NodeID [NUM_BYTES]uint8

type Node struct {
	Key    NodeID
	Domain string
	Port   int
}

type NodePacket struct {
	Node         Node
	NodeResponse chan AddNodeResponse
}

type DHT struct {
	Nodes map[NodeID]Node
	Lists [HASH_SIZE][]NodeID
}

// IndexChannels keeps track of all the channels for the 256 keys of DHT
type IndexChannels struct {
	WriteChannel [HASH_SIZE]chan *NodePacket
}

type CacheObject struct {
	LastTime time.Time
	Dead     bool
}

type AddNodeResponse struct {
	ListIndex int
	Ping      bool
	Input     bool
}
