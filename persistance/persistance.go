package persistance

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hydra-dht/constants"
	pb "hydra-dht/protobuf/node"
	"hydra-dht/structures"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
)

var (
	logFile      *os.File
	filePosition int64
	logIndex     int = 1
)

type PERSISTANCE_FILE string

const (
	LOG PERSISTANCE_FILE = "log"
	DHT PERSISTANCE_FILE = "dht"
)

// For sorting the log and dht files according to index that are read from the directory
type fileSortObject struct {
	FileInfo os.FileInfo
	Index    int64
}

// LogFileNameError is the struct to aid when there is
// an error for when the name for a log file is faulty.
type LogFileNameError struct {
	fileName string
}

// implements the error for the log file name error
func (e *LogFileNameError) Error() string {
	return fmt.Sprintf("Illegal LogFile name, the format for a log file is 'log-<int>', the file encountered is %s", e.fileName)
}

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

func AppendToLogUtil(logFile *os.File, node structures.Node, dhtIndex int32, listIndex int32) error {
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
	// writing the size of logObject into file
	ob := append(buf, out...)
	_, err = logFile.Write(ob)
	if err != nil {
		return err
	}

	err = logFile.Sync()

	return err
}

/*
AppendToLog is the wrapper function called by the DHT to aappend an object into the log.
Read Documentation for AppendToLogUtil for more information
*/
func AppendToLog(node structures.Node, dhtIndex int32, listIndex int32) error {
	return AppendToLogUtil(logFile, node, dhtIndex, listIndex)
}

/*
ReadObjectFromLog reads and sends the latest / last log Object inside
the log file.

Arguments:
1. Log: the log file in which to read an object from
2. Pointer:  The pointer in the log file at the current seek stage.
Returns:
1. logNode = The Log node object retrieved from the log file. It will be empty
if there is an error
2. error = Error object. Will be nil if no error.

*/
func ReadObjectFromLog(log *os.File, logPosition *int64) (*pb.LogNode, error) {

	log.Seek(*logPosition, 0)

	sizeOfLogObjectBuffer := make([]byte, constants.LOG_OBJECT_BYTE_SIZE)
	log.Read(sizeOfLogObjectBuffer)
	*logPosition += constants.LOG_OBJECT_BYTE_SIZE

	sizeOfLogObject, _ := binary.Uvarint(sizeOfLogObjectBuffer)
	*logPosition += int64(sizeOfLogObject)

	logObjectBuffer := make([]byte, sizeOfLogObject)
	log.Read(logObjectBuffer)

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
	} else {
		dht.Lists[row][col] = *n
	}
}

/*
FlushLog reads the log's objects and applies it to the dht in the argument.

Arguments:
1. dht: The pointer to the DHT on which the operations of log are applied.
2. log: The log file which has the operations stored.
Returns:
1. error: Error if any, nil if no error

The DHT is modified and since it's passed by reference, there is no need to return it.
*/
func FlushLog(dht *structures.DHT, log *os.File) error {
	fi, err := log.Stat()
	var logPosition int64
	logPosition = 0
	for {
		if logPosition >= fi.Size() {
			break
		}
		logObject, err := ReadObjectFromLog(log, &logPosition)
		if err != nil {
			return err
		}

		addToDHT(dht, logObject)
	}

	return err
}

// persists dht at regular time intervas when called from dht.go
func PersistDHT(dht structures.DHT) error {

	logIndex++
	OpenLogFile("log-" + strconv.Itoa(logIndex)) // sets up new log file
	err := flushDataStructureToDisk(&dht)

	if err != nil {
		return err
	}
	logFilename := "log-" + strconv.Itoa(logIndex)
	dhtFilename := "dht-" + strconv.Itoa(logIndex-1)
	clearAllLogsAndDhtsBut(logFilename, dhtFilename)

	return nil
}

// remove old log file,
// remove all log files not of the following name
// clear log till some point
func clearAllLogsAndDhtsBut(logFilename string, dhtFilename string) {

	logFiles := GetPersistanceFileNames(LOG)
	dhtFiles := GetPersistanceFileNames(DHT)

	// crucial operation, if shut down occurs now
	// information is lost.

	for _, l := range logFiles {
		if logFilename != l.Name() {
			os.Remove("log/" + l.Name())
		}
	}
	for _, d := range dhtFiles {
		if dhtFilename != d.Name() {
			os.Remove("dht/" + d.Name())
		}
	}
}

// Saves the dht to Disk
func flushDataStructureToDisk(dht *structures.DHT) error {
	//  todo
	filename := "dht/dht-" + strconv.Itoa(logIndex-1) // save dht of old log.
	err := SaveDHT(filename, dht)

	return err
}

/*
getLogFileName get Latest Log file names

This function gets the filename of the log file. It look into the log directory. If only one log file is found.
It returns that. If more than one log file is found. there can be 2 scenarios

1. The previous program failed in the middle of DHT sync
This failure can be anywhere, so we assume that the latest log file is not the absolute truth.

We look at DHT saved

1. so first switch file name and new file is created, 2 files 1 one dht.
# fail -  then take old log, and old dht, apply operation on it
2. save DHT : 2 files 2 dht.
# fail load DHT, if successful of loading latest DHT , then play back log file instructions of new log file on dht and send. If not successful, then do above thing
3. delete prev file 1 file, 2 DHT
# fail if 2 dht found and one log file. then take latest dht and process with log file and send
4. delete prev DHT so, 1 dht, one file
# fail - so all good
5. all good

Arguments:
1. filename = Name of the log file
Returns:
1. error = nil if no error else error
*/
func GetPersistanceFileNames(fileType PERSISTANCE_FILE) []os.FileInfo {
	files, err := ioutil.ReadDir(string(fileType))
	if err != nil {
		log.Fatal(err)
	}

	var latestLogs []fileSortObject
	for i := len(files) - 1; i >= 0; i-- {
		fileName := files[i].Name()
		j, err := GetFileIndex(fileName, fileType)
		if err != nil {
			// just print the error or not
			fmt.Println(err)
		} else {
			latestLogs = append(latestLogs, fileSortObject{FileInfo: files[i], Index: j})
		}
	}

	sort.Slice(latestLogs, func(i, j int) bool {
		return latestLogs[i].Index > latestLogs[j].Index
	})

	var finalLogs []os.FileInfo
	for _, f := range latestLogs {
		finalLogs = append(finalLogs, f.FileInfo)
	}

	return finalLogs
}

/*
GetFileIndex gets the index of file in the log or dht folders.
For examples if the filename is log-23, it will return 23.
Arguments:
1. name:  The file name
2. fileType: File Type ,whether it was a dht or a log file
Returns:
1. int - The file index
2. err - error , nil if no error
*/
func GetFileIndex(name string, fileType PERSISTANCE_FILE) (int64, error) {
	number := strings.SplitAfter(name, "-")
	number[0] = strings.TrimRight(number[0], "-")
	if len(number) > 2 || number[0] != string(fileType) {
		return -1, &LogFileNameError{name}
	}

	return strconv.ParseInt(number[1], 10, 32)

}

// will always create new log file after recover
// saves dht object and then clears up log.
func OpenLogFile(filename string) (*os.File, *int64, error) {

	// create file
	var err error
	logFile, err = os.Create("log/" + filename)
	filePosition = 0

	return logFile, &filePosition, err
}

/*
InitPersistance starts the persistance module of Hydra. The goals of the persistance
module is to open the log file. It opens the log file and brings the filePosition index
to append mode.

Arguments:
None
Returns:
1. The persistant Dht retrieved by starting up the program.nil if error
2. error = nil if no error else error
*/
func InitPersistance() (*structures.DHT, *os.File, *int64, error) {
	//  setup periodic flushing to disk
	// open file
	var err error
	dht, filename := RecoverDHT()
	logIndex = 1
	_, _, err = OpenLogFile(filename)
	if err != nil {
		return nil, nil, nil, err
	}
	return dht, logFile, &filePosition, nil
}

/* persistanceCleanUp cleans up all logs and dhts and saves new fresh dht sent through
the arguments.
Arguments:

1. dht = The DHT to be saved.structures

Rest every log and dht is deleted.
*/
func persistanceCleanUp(dht *structures.DHT) string {

	logFiles := GetPersistanceFileNames(LOG)
	dhtFiles := GetPersistanceFileNames(DHT)

	// crucial operation, if shut down occurs now
	// information is lost.
	for _, d := range dhtFiles {
		os.Remove("dht/" + d.Name())
	}

	for _, l := range logFiles {
		os.Remove("log/" + l.Name())
	}

	// save dht to disk
	SaveDHT("dht/dht-0", dht)

	// the index of the new log file
	return "log-1"
}

// recovers dht if sudden shut down, this culd be multiple log files and dhts in folder.
// It cleans up the folders and saves new fresh dht in disk and starts new log.
func RecoverDHT() (*structures.DHT, string) {
	logFiles := GetPersistanceFileNames(LOG)
	dhtFiles := GetPersistanceFileNames(DHT)

	// init variables
	var l_ind int64
	var d_ind int64
	var l os.FileInfo
	var d os.FileInfo
	logStack := []os.FileInfo{}

	i := 0 // index to logFiles
	j := 0 // index to dhtFiles

	for i < len(logFiles) && j < len(dhtFiles) {

		l = logFiles[i] // TAKE CURRENT LOG
		d = dhtFiles[j] // TAKE CURRENT DHT

		l_ind, _ = GetFileIndex(l.Name(), LOG) // get log index
		d_ind, _ = GetFileIndex(d.Name(), DHT) // get dht index

		fmt.Println(l_ind, d_ind)

		// if log is ahead than dht , then push to stack
		if l_ind > d_ind {
			fmt.Println("Adding to log stack")
			logStack = append(logStack, l)
			i++
			continue
		}
		// if log is behind the current DHT
		dht, err := LoadDHTFile(d.Name())
		// if error in reading DHT, go to next DHT
		if err != nil {
			fmt.Println(err)
			j++
			continue
		} else {
			return processLogStack(dht, logStack, d_ind)

		}
	}

	// if all log files are processed, but dht isn't so, find one good dht, if found ,then process.
	// else go to empty dht condition
	for j < len(dhtFiles) {
		d = dhtFiles[j] // TAKE CURRENT DHT
		dht, err := LoadDHTFile(d.Name())
		// if error in reading DHT, go to next DHT
		if err != nil {
			fmt.Println(err)
			j++
			continue
		} else {
			return processLogStack(dht, logStack, d_ind)
		}
	}

	// this means either all DHT in memory are corrupted
	// so we start from empty DHT
	// traverse remaining logs into log stack and then process log stack
	var dhtEmpty structures.DHT

	for i < len(logFiles) {
		l = logFiles[i] // TAKE CURRENT LOG
		// if log is ahead than dht , then push to stack
		fmt.Println(l.Name())
		fmt.Println("Adding to log stack")
		logStack = append(logStack, l)
		i++
	}
	if len(logStack) == 0 {
		return &dhtEmpty, "log-1"
	}
	lastLogIndex, _ := GetFileIndex(logStack[len(logStack)-1].Name(), LOG)
	d_ind = lastLogIndex - 1
	return processLogStack(&dhtEmpty, logStack, d_ind)
}

// helper function to recover log.
func processLogStack(dht *structures.DHT, logStack []os.FileInfo, d_ind int64) (*structures.DHT, string) {
	latestLogFileName := ""

	for i := len(logStack) - 1; i >= 0; i-- {
		// get index of file, can't error out
		lind, err := GetFileIndex(logStack[i].Name(), LOG)

		// if error in file index or, the log is far ahead of the state of DHT, return that dht
		// setup new file for logging.
		if err != nil || d_ind != lind-1 {
			fmt.Println("Log dht not compatible !")

			// clear all logs in log folder and other dht's, rename dht and log to new index.
			latestLogFileName = persistanceCleanUp(dht)
			// make new_log-file,
			return dht, latestLogFileName // log dht not compatible
		}

		// open log file
		file, err := os.Open("log/" + logStack[i].Name())

		// if error in opening file, clean up everything as before error scenario, and go with current DHT
		if err != nil {
			fmt.Println(err)
			latestLogFileName = persistanceCleanUp(dht)
			return dht, latestLogFileName // if error, then
		}

		// no error in opening log file
		// now use log and run all the operation of log in DHT
		err = FlushLog(dht, file)

		// if there is an error while flushing, that means problem with the log
		// discard log and clean up operations.
		if err != nil {
			fmt.Println(err)
			latestLogFileName = persistanceCleanUp(dht)
			return dht, latestLogFileName // if error, then
		}

		// if there is no error, then flushing has happened well.
		file.Close()
		// update state of DHT for next log
		d_ind++

	}

	// now clean up redundant log files and make new dht object file
	latestLogFileName = persistanceCleanUp(dht)
	return dht, latestLogFileName // if error, then
}

// Saves dht gob to file
func SaveDHT(path string, dht *structures.DHT) error {
	file, err := os.Create(path)
	if err == nil {
		encoder := gob.NewEncoder(file)
		encoder.Encode(dht)
	}
	file.Close()
	return err
}

// loads dht gob from file to memory
func LoadDHTFile(path string) (*structures.DHT, error) {
	dht := &structures.DHT{}
	file, err := os.Open("dht/" + path)
	if err == nil {
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(dht)
	}
	file.Close()
	return dht, err
}

/*
ClosePersistance closes the log file and shuts down the persiatance module Gracefully
Arguments:
None
Returns:
error: nil if no error else some error
*/
func ClosePersistance() error {
	return logFile.Close()
}
