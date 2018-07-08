package structures

import (
	constants "hydra-dht/constants"
	"time"
)

// NodeID denotes Key of Node
type NodeID [constants.NUM_BYTES]uint8

// Node is the main Node data structure
type Node struct {
	Key    NodeID
	Domain string
	Port   int
}

// NodePacket wraps Node and NodeResponse for data sending
type NodePacket struct {
	Node         Node
	NodeResponse chan AddNodeResponse
}

// DHT is the main DHT data structure. It consists of a Map of all Nodes to check for duplicity.
// Lists are a 2D matrix of size 256*keySize denoteing each nodes location according to Kademlia standard
type DHT struct {
	Lists [constants.HASH_SIZE][]Node
}

// Cache keeps track of nodes livliness
type Cache struct {
	Lists [constants.HASH_SIZE][]CacheObject
}

// IndexChannels keeps track of all the channels for the 256 keys of DHT
type IndexChannels struct {
	WriteChannel [constants.HASH_SIZE]chan *NodePacket
}

// CacheObject is the object stored in DHT cache.
// LastTime checks expiry of the node
// Dead informs whether it is dead or not
type CacheObject struct {
	LastTime time.Time
	Dead     bool
}

// AddNodeResponse is the response after AddNode function of DHT. It tells you if
// the list index the node is inserted in
// whether the ping action was required for it to be inserted i.e if the list was full
// and lastly Input defines if the node was rejected or not
type AddNodeResponse struct {
	ListIndex int
	Ping      bool
	Input     bool
}
