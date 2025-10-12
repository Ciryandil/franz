package franz_client

import (
	"fmt"
	"sync"
)

type FranzClientConfig struct {
	PartitionUrls []string `json:"partition_urls"`
}

type Payload struct {
	key   string
	value []byte
}

type FranzProducer struct {
	LocalStore sync.Map
}

func (producer *FranzProducer) Produce(payload Payload) error {
	if payload.key == "" {
		return fmt.Errorf("payload with empty key received")
	}
	//TODO
	return nil
}

type FranzClient struct {
}
