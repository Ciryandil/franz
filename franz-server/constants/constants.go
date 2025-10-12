package constants

import (
	"fmt"
	"os"
)

var QUEUE_FILE string
var METADATA_FILE string
var CONSUMER_METADATA_FILE string
var SERVER_ID string

func InitConstants() {
	SERVER_ID = os.Getenv("FRANZ_SERVER_ID")
	QUEUE_FILE = fmt.Sprintf("queue_file_%s", SERVER_ID)
	METADATA_FILE = fmt.Sprintf("metadata_file_%s", SERVER_ID)
	CONSUMER_METADATA_FILE = fmt.Sprintf("consumer_metadata_file_%s.json", SERVER_ID)
}
