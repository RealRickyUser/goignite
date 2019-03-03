package goignite

import (
	"bufio"
	"bytes"
	"encoding/binary"
)

var igniteOrder = binary.LittleEndian

type igniteWriter struct {
	writer *bufio.Writer
	buffer *bytes.Buffer
}

func createNewWriter() igniteWriter {
	i := igniteWriter{buffer: new(bytes.Buffer)}
	i.writer = bufio.NewWriter(i.buffer)
	return i
}

func (i *igniteWriter) write(item interface{}) error {
	return binary.Write(i.writer, igniteOrder, item)
}

func (i *igniteWriter) writeAll(items ...interface{}) error {
	for _, data := range items {
		err := binary.Write(i.writer, igniteOrder, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *igniteWriter) flushAndGet() (result []byte, err error) {
	err = i.writer.Flush()
	result = i.buffer.Bytes()
	return
}
