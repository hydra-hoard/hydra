package constants

import "time"

const (
	NUM_BYTES              = 32
	K_BUCKET_SIZE          = 20
	HASH_SIZE              = NUM_BYTES * 8
	TIME_DURATION          = 5 * time.Second
	LOG_OBJECT_BYTE_SIZE   = 10
	MONGO_SERVER           = "127.0.0.1"
	MONGO_TRACKER_DATABASE = "Tracker"
)
