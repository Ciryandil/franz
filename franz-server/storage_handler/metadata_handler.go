package storage_handler

import (
	"encoding/binary"
	"fmt"
	"franz/franz-server/constants"
	"io"
	"log"
	"os"
)

var MetadataLogFileWrite *os.File
var MetadataLogFileRead *os.File

func NewMetadataHandler() {
	var err error
	MetadataLogFileWrite, err = os.OpenFile(constants.METADATA_FILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("[FRANZ] Unable to open Metadata Log File Writer: %v\n", err)
	}
	MetadataLogFileRead, err = os.OpenFile(constants.METADATA_FILE, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("[FRANZ] Unable to open Metadata Log File Reader: %v\n", err)
	}
}

func WriteOffsetsToFile(offsets []uint64) error {
	fmt.Printf("[FRANZ] Writing %d offsets to file: %v\n", len(offsets), offsets)
	byteBuf := make([]byte, len(offsets)*8)
	for ind, offset := range offsets {
		byteOffset := make([]byte, 8)
		binary.BigEndian.PutUint64(byteOffset, offset)
		copy(byteBuf[ind*8:(ind+1)*8], byteOffset)
	}
	_, err := MetadataLogFileWrite.Write(byteBuf)
	return err
}

func ReadOffsetsFromFile(numOffsets uint64, startOffset int64) ([]uint64, error) {
	buf := make([]byte, 8*numOffsets)
	fmt.Printf("[FRANZ] Trying to read %d bytes from offsets file\n", len(buf))
	n, err := MetadataLogFileRead.ReadAt(buf, startOffset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != len(buf) {
		buf = buf[:n]
	}
	offsets := make([]uint64, numOffsets)
	for itr, _ := range offsets {
		offset := binary.BigEndian.Uint64(buf[itr*8 : (itr+1)*8])
		offsets[itr] = offset
	}
	fmt.Printf("[FRANZ] Read %d offsets from file: %v\n", len(offsets), offsets)

	return offsets, nil
}
