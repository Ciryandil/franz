package storage_handler

func InitializeHandlers() {
	NewConsumerMetadataHandler()
	NewDataHandler()
	NewMetadataHandler()
}
