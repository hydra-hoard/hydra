package persistance_test

import (
	"testing"
)

func TestAppendToFile(t *testing.T) {
	filename := "log.txt"
	// init persistance
	// append value to log
	// read text value back up, check latest value
	// clean up
}

func TestPeriodicFlushDHT() {
	// set time duration
	// check if DHT exists
	// check after some time
	// DHT object should be saved to disk
}

func TestClearLog() {
	// clear log
	// check if log cleared
}

func TestLoadDHT() {
	// save to disk
	// read it back
	// check
	// clean up
}
