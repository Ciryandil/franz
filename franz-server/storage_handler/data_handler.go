package storage_handler

import (
	"encoding/binary"
	"fmt"
	"franz/franz-server/constants"
	"log"
	"os"

	"franz/compiled_protos"

	"google.golang.org/protobuf/proto"
)

var DataLogFileWrite *os.File
var DataLogFileRead *os.File
var DataFileOffset uint64

func init() {
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

func ReadEntriesFromFile(offset int64, numEntries int64) (*compiled_protos.DataEntryArray, error) {
	var entriesRead int64 = 0
	entries := make([]*compiled_protos.DataEntry, numEntries)
	for entriesRead < numEntries {
		dataBuf := make([]byte, 64*1024)
		n, err := DataLogFileRead.ReadAt(dataBuf, offset)
		if err != nil {
			return nil, err
		}
	}

	lenBuf := make([]byte, 8)
	n, err := DataLogFileRead.ReadAt(lenBuf, offset)
	if err != nil {
		return nil, err
	}
	if n != 8 {
		return nil, fmt.Errorf("insufficient bytes read")
	}
	entryLength := binary.BigEndian.Uint64(lenBuf)
	dataBuf := make([]byte, entryLength)
	n, err = DataLogFileRead.ReadAt(dataBuf, offset+8)
	if err != nil {
		return nil, err
	}
	if n != int(entryLength) {
		return nil, fmt.Errorf("insufficient bytes read")
	}
	var dataEntry *compiled_protos.DataEntry
	err = proto.Unmarshal(dataBuf, dataEntry)
	if err != nil {
		return nil, err
	}
	return dataEntry, nil
}
