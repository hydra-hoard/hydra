package dht_test

import (
	"hydra-dht/dht"
	"testing"
	"time"
)

func TestAddNode(t *testing.T) {

	//nodeKey := "1111111"
	//maxNodeInList := 2

	dht.InitDHT(2, .01)

	var tests = []struct {
		nodeId    string
		listIndex int
		ping      bool
		input     bool
		sleep     bool
	}{
		// All ping responses will yield dead nodes.
		// TODO mock ping response
		{"01111111", 0, false, true, false}, //testing list index
		{"01111110", 0, false, true, false},
		{"01111101", 0, true, true, true},     // testing list full
		{"01111101", -1, false, false, false}, //testing repeat node
		{"11011111", 2, false, true, false},
		{"11011101", 2, false, true, false},
		{"11011101", -1, false, false, false},     // testing list full
		{"101010101010", -1, false, false, false}, // testing rubbish nodeid
	}
	for _, test := range tests {
		if test.sleep {
			time.Sleep(1 * time.Second)
		}
		channel, err := dht.AddNode("127.0.0.1", 80, test.nodeId)

		if err != nil {

			if test.nodeId == "101010101010" {
				continue
			} else {
				t.Errorf("%v", err)
			}
		}
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

func TestComputeByte(t *testing.T) {

}
