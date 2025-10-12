package storage_handler

import (
	"encoding/json"
	"fmt"
	"franz/franz-server/constants"
	"log"
	"os"
	"sync"
)

type ConsumerMetadataHandler struct {
	lock      sync.RWMutex
	offsetMap sync.Map
}

var ConsumerOffsetHandler ConsumerMetadataHandler

func NewConsumerMetadataHandler() {
	file, err := os.Open(constants.CONSUMER_METADATA_FILE)
	if err != nil {
		log.Fatalf("[FRANZ] Error opening Consumer offset file: %v\n", err)
	}
	defer file.Close()
	var dataMap map[string]interface{}
	err = json.NewDecoder(file).Decode(&dataMap)
	if err != nil {
		log.Fatalf("[FRANZ] Error decoding Consumer offset file to map: %v\n", err)
	}
	for key, val := range dataMap {
		ConsumerOffsetHandler.offsetMap.Store(key, val)
	}
}

func (c *ConsumerMetadataHandler) GetConsumerOffset(consumer string) int64 {
	c.lock.RLock()
	val, ok := c.offsetMap.Load(consumer)
	if !ok {
		return 0
	}
	valInt, _ := val.(int64)
	return valInt
}

func (c *ConsumerMetadataHandler) SetConsumerOffset(consumer string, offset int64) {
	c.lock.RLock()
	c.offsetMap.Store(consumer, offset)
}

func (c *ConsumerMetadataHandler) Close() {
	c.lock.Lock()
	currMap := make(map[string]interface{})
	c.offsetMap.Range(func(key any, value any) bool {
		k, ok := key.(string)
		if ok {
			currMap[k] = value
		} else {
			fmt.Println("[FRANZ] Consumer offset map contained non-string key: ", key)
		}
		return true
	})
	dataBytes, err := json.Marshal(currMap)
	if err != nil {
		fmt.Println("[FRANZ] Error marshalling consumer offset map to json: ", err)
		return
	}
	err = os.WriteFile(constants.CONSUMER_METADATA_FILE, dataBytes, 0644)
	if err != nil {
		fmt.Println("[FRANZ] Error writing consumer offset map to file: ", err)
	}
}
