package writer

import (
	"encoding/binary"
	"fmt"
	"franz/franz-server/compiled_protos"
	"franz/franz-server/constants"
	"log"
	"os"

	"google.golang.org/protobuf/proto"
)

var LogFileWrite *os.File
var LogFileRead *os.File

func init() {
	var err error
	LogFileWrite, err = os.OpenFile(constants.QUEUE_FILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("[FRANZ] Unable to open Log File Writer: %v\n", err)
	}
	LogFileRead, err = os.OpenFile(constants.QUEUE_FILE, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("[FRANZ] Unable to open Log File Reader: %v\n", err)
	}
}

func WriteToFile(dataEntries []*compiled_protos.DataEntry) error {
	byteArray := make([]byte, 0)
	for _, dataEntry := range dataEntries {
		serializedEntry, err := proto.Marshal(dataEntry)
		if err != nil {
			return err
		}
		lenBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBuf, uint64(len(serializedEntry)))
		byteArray = append(byteArray, lenBuf...)
		byteArray = append(byteArray, serializedEntry...)
	}
	_, err := LogFileWrite.Write(byteArray)
	return err
}

func ReadFromFile(offset int64) (*compiled_protos.DataEntry, error) {
	lenBuf := make([]byte, 8)
	n, err := LogFileRead.ReadAt(lenBuf, offset)
	if err != nil {
		return nil, err
	}
	if n != 8 {
		return nil, fmt.Errorf("insufficient bytes read")
	}
	entryLength := binary.BigEndian.Uint64(lenBuf)
	dataBuf := make([]byte, entryLength)
	n, err = LogFileRead.ReadAt(dataBuf, offset+8)
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
