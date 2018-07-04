package dht_test

import (
	"hydra-dht/dht"
	"testing"
	"time"
)

func TestAddNode(t *testing.T) {

	dht.InitDHT(5)

	timeDurationInSeconds := 5 * time.Second
	// DHT Parameters

	//nodeKey := "11000"
	//maxNodeInList := 2

	var tests = []struct {
		nodeId    string
		listIndex int
		ping      bool
		input     bool
	}{
		{"01000", 0, false, true},
		{"01100", 0, false, true},
		{"01110", 0, true, true},
		{"11110", 3, false, true},
		{"11100", 3, false, true},
		{"11101", 3, true, true},
		{"01100", 0, false, false},
	}
	for _, test := range tests {
		channel := dht.AddNode("127.0.0.1", 80, test.nodeId)
		var actual dht.AddNodeResponse
		select {
		case actual = <-channel:
			if actual.ListIndex != test.listIndex ||
				actual.Ping != test.ping || actual.Input != test.input {
				t.Errorf("AddNode(%q) = %v;want %v", test.nodeId, actual.ListIndex, test.listIndex)
			}
		case <-time.After(timeDurationInSeconds):
			t.Errorf("Time Out error")
		}
	}

}

func TestInitDHT(t *testing.T) {

}
