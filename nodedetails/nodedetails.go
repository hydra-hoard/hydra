package nodedetails

import (
	. "hydra-dht/constants"
	. "hydra-dht/structures"
)

//TODO Node id needs to be generated of current Node
var (
	MyNode = &Node{
		Key:    [NUM_BYTES]uint8{255, 4, 67, 24, 12, 34, 234, 24, 12, 34, 234, 24, 12, 34, 234, 24, 12, 34, 234, 24, 12, 34, 234, 24, 12, 34, 234, 24, 12, 34, 234, 24},
		Domain: "127.0.0.1",
		Port:   1200}
)
