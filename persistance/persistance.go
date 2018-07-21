package persistance

import (
	"encoding/binary"
	"fmt"
	"hydra-dht/constants"
	pb "hydra-dht/protobuf/node"
	"hydra-dht/structures"
	"os"

	"github.com/golang/protobuf/proto"
)

var (
	logFile      *os.File
	filePosition int64
)

/*

 */
func convertNumberToBytes(i uint64) ([]byte, int) {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, i)

	return buf, constants.LOG_OBJECT_BYTE_SIZE

}

/*
AppendToLog appends a DHT entry into the persistent transaction log.

The function first adds the number of bytes the LogObject is of, then inserts
the LogObject into the log file.

This method helps when we're reading the contents of the log back into memory.
As the Log Object does not have a fixed size.

Arguments:
1. node :  Node object inside the DHT
2. dhtIndex: Bucket Index of DHT table into which to put the node into
3. listIndex: The index in the Bucket List of DHT in which the Node is added to.

Returns:
1. error: Returns error , if no error then error is nil

*/
func AppendToLog(node structures.Node, dhtIndex int32, listIndex int32) error {
	// write to file in following format
	logObject := &pb.LogNode{
		Node: &pb.Node{
			NodeId: node.Key[:],
			Domain: node.Domain,
			Port:   int32(node.Port),
		},
		DhtIndex:  dhtIndex,
		ListIndex: listIndex,
	}

	out, err := proto.Marshal(logObject)

	if err != nil {
		return err
	}
	// the number of bytes consisting of logObject
	i := uint64(len(out))
	buf, _ := convertNumberToBytes(i)
	fmt.Println(buf)
	// writing the size of logObject into file
	n2, err := logFile.Write(buf)
	if err != nil {
		return err
	}
	fmt.Printf("Wrote %d bytes for the size of LogObject\n", n2)

	// writing the logObject into file
	n3, err := logFile.Write(out)
	if err != nil {
		return err
	}
	fmt.Printf("Wrote LogObject into file of size %d bytes\n", n3)
	err = logFile.Sync()

	return err
}

/*
ReadLatestObjectFromLog reads and sends the latest / last log Object inside
the log file.

Arguments:
None
Returns:
1. logNode = The Log node object retrieved from the log file. It will be empty
if there is an error
2. error = Error object. Will be nil if no error.

*/
func ReadLatestObjectFromLog() (*pb.LogNode, error) {

	logFile.Seek(filePosition, 0)

	sizeOfLogObjectBuffer := make([]byte, constants.LOG_OBJECT_BYTE_SIZE)
	logFile.Read(sizeOfLogObjectBuffer)
	filePosition += constants.LOG_OBJECT_BYTE_SIZE

	sizeOfLogObject, _ := binary.Uvarint(sizeOfLogObjectBuffer)
	filePosition += int64(sizeOfLogObject)

	logObjectBuffer := make([]byte, sizeOfLogObject)
	logFile.Read(logObjectBuffer)

	logObject := &pb.LogNode{}
	err := proto.Unmarshal(logObjectBuffer, logObject)

	return logObject, err
}

func LoadDHT() {
	// load objects one by one and insert into DHT

}

func PeriodicSyncDHT() {
	// clear log
}

func clearLog() {
	// clear log till some point
}

func flushDataStructureToDisk() {
	//  gob ?
}

func InitPersistance(filename string) error {
	//  setup periodic flushing to disk
	// open file
	var err error
	logFile, err = os.Create(filename)
	return err
}

func ClosePersistance() {
	logFile.Close()
}

func main() {

	InitPersistance("log_1")

	// do some stuff here

	ClosePersistance()
}
