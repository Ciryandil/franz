package storage_handler

import (
	"encoding/binary"
	"franz/franz-server/constants"
	"io"
	"log"
	"os"

	"franz/compiled_protos"

	"google.golang.org/protobuf/proto"
)

var DataLogFileWrite *os.File
var DataLogFileRead *os.File
var DataFileOffset uint64

func NewDataHandler() {
	var err error
	DataLogFileWrite, err = os.OpenFile(constants.QUEUE_FILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("[FRANZ] Unable to open Data Log File Writer: %v\n", err)
	}
	DataLogFileRead, err = os.OpenFile(constants.QUEUE_FILE, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("[FRANZ] Unable to open Data Log File Reader: %v\n", err)
	}
	info, err := DataLogFileRead.Stat()
	if err != nil {
		log.Fatalf("[FRANZ] Unable to load Data Log File Stats: %v\n", err)
	}
	DataFileOffset = uint64(info.Size())

}

func WriteEntriesToFile(dataEntries []*compiled_protos.DataEntry) ([]uint64, error) {
	byteArray := make([]byte, 0)
	offsets := make([]uint64, len(dataEntries))
	var sizeSum uint64
	for itr, dataEntry := range dataEntries {
		serializedEntry, err := proto.Marshal(dataEntry)
		if err != nil {
			return nil, err
		}
		lenBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBuf, uint64(len(serializedEntry)))
		byteArray = append(byteArray, lenBuf...)
		byteArray = append(byteArray, serializedEntry...)
		offsets[itr] = sizeSum + DataFileOffset
		sizeSum += uint64(len(serializedEntry)) + 8
	}
	_, err := DataLogFileWrite.Write(byteArray)
	if err != nil {
		return nil, err
	}
	DataFileOffset += sizeSum
	return offsets, nil
}

func ReadEntriesFromFile(offset int64, numBytes int64) (*compiled_protos.DataEntryArray, error) {
	entries := make([]*compiled_protos.DataEntry, 0)
	DataLogFileRead.Seek(offset, 0)
	lenBuf := make([]byte, numBytes)
	_, err := io.ReadFull(DataLogFileRead, lenBuf)
	if err != nil {
		return nil, err
	}
	var pos int64 = 0
	for pos < numBytes {
		entrySize := binary.BigEndian.Uint64(lenBuf[pos : pos+8])
		pos += 8
		var dataEntry *compiled_protos.DataEntry
		err = proto.Unmarshal(lenBuf[pos:pos+int64(entrySize)], dataEntry)
		if err != nil {
			return nil, err
		}
		entries = append(entries, dataEntry)
		pos += int64(entrySize)
	}

	return &compiled_protos.DataEntryArray{
		Entries: entries,
	}, nil
}
