package persistance_test

import (
	"fmt"
	"hydra-dht/persistance"
	"hydra-dht/structures"
	"os"
	"testing"
)

func TestAppendToFile(t *testing.T) {

	_, log, filePosition, _ := persistance.InitPersistance()

	key := structures.NodeID{5, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124}

	var tests = []struct {
		nodeId    byte
		domain    string
		port      int32
		dhtIndex  int32
		listIndex int32
	}{

		{1, "127.0.0.1", 10, 1, 1},
		{2, "127.0.0.1", 20, 2, 1},
		{3, "127.0.0.1", 30, 3, 2},
		{4, "127.0.0.1", 40, 4, 3},
		{5, "127.0.0.1", 50, 5, 4},
		{6, "127.0.0.1", 60, 6, 4},
		{7, "127.0.0.1", 70, 7, 6},
		{8, "127.0.0.1", 80, 8, 6},
	}
	for _, test := range tests {
		key[0] = test.nodeId

		node := structures.Node{
			Key:    key,
			Domain: test.domain,
			Port:   int(test.port),
		}

		err := persistance.AppendToLog(node, test.dhtIndex, test.listIndex)
		if err != nil {
			t.Errorf("%v", err)
		}

		logObject, err := persistance.ReadObjectFromLog(log, filePosition)
		if err != nil {
			t.Errorf("%v", err)
		}

		if logObject.Node.Domain != test.domain || logObject.DhtIndex != test.dhtIndex || logObject.ListIndex != test.listIndex || logObject.Node.Port != test.port {

			t.Errorf("AppendToLog => ||  (Domain)= %v;want %v | (Port)= %v;want %v | (DHTIndex)= %v;want %v | (ListIndex)= %v;want %v",
				logObject.Node.Domain, test.domain,
				logObject.Node.Port, test.port,
				logObject.DhtIndex, test.dhtIndex,
				logObject.ListIndex, test.listIndex)
		}

	}
	// clean up test log files
	closingError := persistance.ClosePersistance()
	if closingError != nil {
		t.Errorf("%v", closingError)
	}
	cleanUpHelper()
}

func CreateDHTandLog(fileIndex string) *structures.DHT {
	logFile, _, _ := persistance.OpenLogFile("log-" + fileIndex)

	key := structures.NodeID{5, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124}

	var tests = []struct {
		nodeId    byte
		domain    string
		port      int32
		dhtIndex  int32
		listIndex int32
	}{

		{1, "127.0.0.1", 10, 0, 0},
		{2, "127.0.0.1", 20, 0, 1},
		{3, "127.0.0.1", 30, 0, 2},
		{4, "127.0.0.1", 40, 3, 0},
		{5, "127.0.0.1", 50, 4, 0},
		{6, "127.0.0.1", 60, 5, 0},
		{7, "127.0.0.1", 70, 5, 1},
		{8, "127.0.0.1", 80, 5, 2},
	}
	for _, test := range tests {

		key[0] = test.nodeId

		node := structures.Node{
			Key:    key,
			Domain: test.domain,
			Port:   int(test.port),
		}
		persistance.AppendToLogUtil(logFile, node, test.dhtIndex, test.listIndex)
	}
	// replay log on empty dht
	var dht structures.DHT
	persistance.FlushLog(&dht, logFile)

	persistance.ClosePersistance()
	persistance.SaveDHT("dht/dht-"+fileIndex, &dht)
	return &dht
}

func TestLoadDHT(t *testing.T) {

	logFile, _, err := persistance.OpenLogFile("log-1")
	if err != nil {
		t.Errorf("%v", err)
	}
	key := structures.NodeID{5, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124, 234, 4, 67, 124}

	var tests = []struct {
		nodeId    byte
		domain    string
		port      int32
		dhtIndex  int32
		listIndex int32
	}{

		{1, "127.0.0.1", 10, 0, 0},
		{2, "127.0.0.1", 20, 0, 1},
		{3, "127.0.0.1", 30, 0, 2},
		{4, "127.0.0.1", 40, 3, 0},
		{5, "127.0.0.1", 50, 4, 0},
		{6, "127.0.0.1", 60, 5, 0},
		{7, "127.0.0.1", 70, 5, 1},
		{8, "127.0.0.1", 80, 5, 2},
	}
	for _, test := range tests {

		key[0] = test.nodeId

		node := structures.Node{
			Key:    key,
			Domain: test.domain,
			Port:   int(test.port),
		}
		err := persistance.AppendToLog(node, test.dhtIndex, test.listIndex)
		if err != nil {
			t.Errorf("%v", err)
		}

	}

	// replay log on empty dht
	var dht structures.DHT
	persistance.FlushLog(&dht, logFile)

	if err != nil {
		t.Errorf("%v", err)
	}

	for _, test := range tests {

		key[0] = test.nodeId

		node := structures.Node{
			Key:    key,
			Domain: test.domain,
			Port:   int(test.port),
		}

		n := dht.Lists[int(test.dhtIndex)][int(test.listIndex)]

		if n.Domain != node.Domain || n.Port != node.Port || n.Key != node.Key {
			t.Errorf("LoadDHT => ||  (Domain)= %v;want %v | (Port)= %v;want %v ",
				n.Domain, node.Domain,
				n.Port, node.Port)
		}

	}

	// clean up test log files
	closingError := persistance.ClosePersistance()
	if closingError != nil {
		t.Errorf("%v", closingError)
	}
	cleanUpHelper()

}

func TestGetLogFileName(t *testing.T) {
	var tests = []struct {
		fileIndex string
	}{

		{"1"},
	}
	for _, test := range tests {
		CreateDHTandLog(test.fileIndex)
	}
	files := persistance.GetPersistanceFileNames(persistance.LOG)
	if len(files) != 1 {
		t.Errorf("Wanted 1 log file but got %d number of files in Log", len(files))
	}

	cleanUpHelper()

}

func TestGetFileIndex(t *testing.T) {

	i, err := persistance.GetFileIndex("log-1", persistance.LOG)
	if err == nil {
		fmt.Println(i)
	} else {
		t.Errorf("%v", err)
	}

	i, err = persistance.GetFileIndex("logas-1s", persistance.LOG)
	if err == nil {
		t.Errorf("This is an invalid name, test should error out, but err was nil")
	}
}

func TestRecover(t *testing.T) {

	// make different combinations of logs

	var tests = []struct {
		fileIndex string
	}{

		{"1"},
		{"2"},
		{"3"},
	}
	for _, test := range tests {
		CreateDHTandLog(test.fileIndex)
	}

	cleanUpDHTs()

	_, filename := persistance.RecoverDHT()
	fmt.Println(filename)
	if filename != "log-1" {
		t.Errorf("Wanted filename: log-1, but got %v", filename)
	}

	cleanUpHelper()

	tests = []struct {
		fileIndex string
	}{

		{"1"},
		{"2"},
		{"3"},
	}
	for _, test := range tests {
		CreateDHTandLog(test.fileIndex)
	}

	cleanUpLogs()

	_, filename = persistance.RecoverDHT()
	fmt.Println(filename)
	if filename != "log-1" {
		t.Errorf("Wanted filename: log-1, but got %v", filename)
	}

	cleanUpHelper()

	tests = []struct {
		fileIndex string
	}{

		{"1"},
		{"2"},
		{"3"},
	}
	for _, test := range tests {
		CreateDHTandLog(test.fileIndex)
	}

	_, filename = persistance.RecoverDHT()
	fmt.Println(filename)
	if filename != "log-1" {
		t.Errorf("Wanted filename: log-1, but got %v", filename)
	}

	cleanUpHelper()

	tests = []struct {
		fileIndex string
	}{

		{"1"},
		{"2"},
		{"10"},
	}
	for _, test := range tests {
		CreateDHTandLog(test.fileIndex)
	}

	cleanUpDHTsBut("dht-1")

	_, filename = persistance.RecoverDHT()
	fmt.Println(filename)
	if filename != "log-1" {
		t.Errorf("Wanted filename: log-1, but got %v", filename)
	}

	cleanUpHelper()

}

func TestPeriodicSyncDHT(t *testing.T) {

	persistance.InitPersistance()
	var dht *structures.DHT
	var tests = []struct {
		fileIndex string
	}{
		{"121"},
	}
	for _, test := range tests {
		dht = CreateDHTandLog(test.fileIndex)
	}

	persistance.PersistDHT(*dht)
	logfiles := persistance.GetPersistanceFileNames(persistance.LOG)
	dhtfiles := persistance.GetPersistanceFileNames(persistance.DHT)

	logindex, err := persistance.GetFileIndex(string(logfiles[0].Name()), persistance.LOG)
	dhtindex, err := persistance.GetFileIndex(string(dhtfiles[0].Name()), persistance.DHT)
	if len(logfiles) != 1 || len(dhtfiles) != 1 || logindex != 2 || dhtindex != 1 {
		t.Errorf("Wanted 1 log file and 1 dht file but got %d number of files in Log and %d files in dht", len(logfiles), len(dhtfiles))
	} else if err != nil {
		t.Errorf("%v", err)
	}

	cleanUpHelper()
}

func cleanUpHelper() {
	cleanUpLogs()
	cleanUpDHTs()

}

func cleanUpDHTs() {
	dhtFiles := persistance.GetPersistanceFileNames(persistance.DHT)

	for _, d := range dhtFiles {
		os.Remove("dht/" + d.Name())
	}
}

func cleanUpDHTsBut(filename string) {
	dhtFiles := persistance.GetPersistanceFileNames(persistance.DHT)

	for _, d := range dhtFiles {
		if filename != d.Name() {
			os.Remove("dht/" + d.Name())
		}
	}
}

func cleanUpLogs() {
	logFiles := persistance.GetPersistanceFileNames(persistance.LOG)
	for _, l := range logFiles {
		os.Remove("log/" + l.Name())
	}
}
