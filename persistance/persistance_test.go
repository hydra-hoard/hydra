package persistance_test

import (
	"hydra-dht/persistance"
	"hydra-dht/structures"
	"os"
	"testing"
)

func TestAppendToFile(t *testing.T) {
	filename := "log_test"
	persistance.InitPersistance(filename)

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

		logObject, err := persistance.ReadLatestObjectFromLog()
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
	os.Remove(filename)
}

func TestPeriodicSyncDHT(t *testing.T) {
	// set time duration
	// check if DHT exists
	// check after some time
	// DHT object should be saved to disk
}

func TestClearLog(t *testing.T) {
	// clear log
	// check if log cleared
}

func TestLoadDHT(t *testing.T) {
	filename := "log_test"
	persistance.InitPersistance(filename)

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

	dht, err := persistance.LoadDHT()

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
	os.Remove(filename)

}
