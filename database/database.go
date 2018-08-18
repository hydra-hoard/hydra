package database

import (
	"hydra-dht/structures"
)

func RegisterDataset(hash string) chan structures.DatabaseResponse {
	messageChannel := make(chan structures.DatabaseResponse)
	collectionInfo := structures.DatasetInfo{ItemType: "info", Hash: hash, NumPieces: 5}
	go MakeCollection(hash, collectionInfo, messageChannel)
	return messageChannel
}

func AddPeer(hash string, collectionHash string, address string) chan structures.DatabaseResponse {
	messageChannel := make(chan structures.DatabaseResponse)
	peer := structures.DatabasePeer{ItemType: "peer", Hash: hash, Address: address}
	go AddDocument(hash, peer, messageChannel)
	return messageChannel
}
