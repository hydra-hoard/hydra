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
convertNumberToBytes is a helper function to AppendToLog. It converts a uint64 number
to a byte array . The size of byte array is 10. This is required to keep track of size of
logObject in the log as that object can be of any size.

Hence , we first store the size of log Object in bytes, and then store the byte representation
of the Log Object.

Arguments:
1. i = The uint64 number , the size of logObject
Returns
1. byte[] = THe byte array representation of i
2. int = The actual number of bytes , the number i took. The max number of bytes it can take is 10
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

/*
addToDHT is a helper function to the LoadDHT function. It adds the value to the
DHT. If required it appends the value to list, else it inserts the value at the
said position.

Arguments:
1. dht - The DHT in which value is added
2. logObject - The object which contains value to be added, along with dhtIndex
(the bucket in which the value is to be inserted) and bucket list index in
 which the value is to inserted.

Please check proto Log Object defination to know more about the LogNode variable.
*/
func addToDHT(dht *structures.DHT, logObject *pb.LogNode) {
	row := logObject.DhtIndex
	col := logObject.ListIndex

	var nodeID structures.NodeID
	copy(nodeID[:], logObject.Node.NodeId)

	n := &structures.Node{
		Key:    nodeID,
		Port:   int(logObject.Node.Port),
		Domain: logObject.Node.Domain,
	}

	if len(dht.Lists[row]) < int(col+1) {
		dht.Lists[row] = append(dht.Lists[row], *n)
		fmt.Println(dht.Lists[row])
	} else {
		dht.Lists[row][col] = *n
	}
}

/*
LoadDHT loads the DHT by replaying the tra nsaction Log entirely.
It performs all the operations on the DHT that have occurred in the past, hence
getting DHT to the same state as before the crash/shut down

Arguments: None
Returns:
1. DHT - Returns the DHDT loaded from log, empty if error occurs.
2. error - Non nil value if some error occured in between the program.
*/
func LoadDHT() (*structures.DHT, error) {

	var dht structures.DHT
	fi, err := logFile.Stat()
	filePosition = 0
	for {
		if filePosition >= fi.Size() {
			break
		}
		logObject, err := ReadLatestObjectFromLog()
		if err != nil {
			return &dht, err
		}

		addToDHT(&dht, logObject)
	}

	return &dht, err
}

func PeriodicSyncDHT() {
	// clear log
}

func clearLog() {
	// clear log till some point
}

func flushDataStructureToDisk() {
	//  todo
}

/*
InitPersistance starts the persistance module of Hydra. The goals of the persistance
module is to open the log file. It opens the log file and brings the filePosition index
to append mode.

Arguments:
1. filename = Name of the log file
Returns:
1. error = nil if no error else error
*/
func InitPersistance(filename string) error {
	//  setup periodic flushing to disk
	// open file
	var err error
	logFile, err = os.Create(filename)
	return err
}

/*
ClosePersistance closes the log file and shuts down the persisatnce module Gracefully
Arguments:
None
Returns:
error: nil if no error else some error
*/
func ClosePersistance() error {
	return logFile.Close()
}
