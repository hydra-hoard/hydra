package dht_test

import (
	"hydra-dht/dht"
	"testing"
	"time"
)

func TestAddNode(t *testing.T) {

	dht.InitDHT(5)
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
		{"11110", 2, false, true},
		{"11100", 2, false, true},
		{"11101", 2, true, true},
	}
	for _, test := range tests {
		channel := dht.AddNode("127.0.0.1", 80, test.nodeId)
		select {
		case actual := <-channel:
			if actual.ListIndex != test.listIndex ||
				actual.Ping != test.ping || actual.Input != test.input {
				t.Errorf("AddNode(%q) => ||  (INDEX)= %v;want %v | (INPUT)= %v;want %v | (PING)= %v;want %v",
					test.nodeId,
					actual.ListIndex, test.listIndex,
					actual.Input, test.input,
					actual.Ping, test.ping)
			}
		case <-time.After(time.Second * 1):
			t.Errorf("Time Out error")
		}
	}

}

func TestInitDHT(t *testing.T) {

}
