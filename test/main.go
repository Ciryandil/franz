package main

import (
	"fmt"
	"franz/compiled_protos"
	"franz/franz-client/consumer"
	"franz/franz-client/producer"
	"log"
)

func main() {
	franzProducer, err := producer.NewProducer(
		producer.ProducerConfig{
			PartitionAddrs: []string{"localhost:50051"},
			FlushInterval:  5,
			BatchSize:      32,
		},
	)
	if err != nil {
		log.Fatal("Error creating franz producer: ", err)
	}
	franzConsumer, err := consumer.NewConsumer(
		consumer.ConsumerConfig{
			PartitionAddr:  "localhost:50051",
			CommitInterval: 1,
			BatchSize:      1,
			Id:             "franz_test",
		},
	)
	if err != nil {
		log.Fatal("Error creating franz consumer: ", err)
	}
	testMessage := "Hello Franz"

	franzProducer.Produce(&compiled_protos.DataEntry{
		Value: []byte(testMessage),
	})
	go func() {
		for {
			res, isThere := franzConsumer.Consume()
			if isThere {
				fmt.Println("Consumed: ", string(res.Value))
				break
			}
		}
	}()
	franzProducer.Close()
	franzConsumer.Close()
}
