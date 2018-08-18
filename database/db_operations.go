package database

import (
	"hydra-dht/constants"
	"hydra-dht/structures"
	"log"

	mgo "gopkg.in/mgo.v2"
)

var db *mgo.Database

func Connect() {
	session, err := mgo.Dial(constants.MONGO_SERVER)
	if err != nil {
		log.Fatal(err)
	}
	db = session.DB(constants.MONGO_TRACKER_DATABASE)
}

func MakeCollection(collectionHash string, collectionInfo structures.DatasetInfo, messageChannel chan structures.DatabaseResponse) {
	Connect()
	err := db.C(collectionHash).Insert(&collectionInfo)
	message := structures.DatabaseResponse{Hash: collectionHash, Status: ""}
	if err != nil {
		log.Fatalf("Failed to add the new Dataset: %v", err)
		message.Status = "Failure"
	} else {
		message.Status = "Success"
	}
	messageChannel <- message
}

func AddDocument(collectionHash string, peer structures.DatabasePeer, messageChannel chan structures.DatabaseResponse) {
	Connect()
	err := db.C(collectionHash).Insert(&peer)
	message := structures.DatabaseResponse{Hash: collectionHash, Status: ""}
	if err != nil {
		log.Fatalf("Fail to add new peer: %v", err)
		message.Status = "Failure"
	} else {
		message.Status = "Success"
	}
	messageChannel <- message
}
