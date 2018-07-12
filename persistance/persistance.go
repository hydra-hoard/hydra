package persistance

import "hydra-dht/structures"

func AppendToLog(index int, node structures.Node, replace bool) {
	// write to file in following format
	if replace {

	}
}

func periodicSync() {
	// clear log
}

func clearLog() {
	// clear log till some point
}

func flushDataStructureToDisk() {
	//  gob ?
}

func InitPersistance() {
	//  setup periodic flushing to disk
}
