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

type igniteReader struct {
	reader  *bytes.Reader
	content []byte
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
		err := i.write(data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *igniteWriter) writeField(data interface{}) (err error) {
	if err = i.write(getDataType(data)); err != nil {
		return
	}
	switch data.(type) {
	case string:
		buff := []byte(data.(string))
		if err = i.write(int32(len(buff))); err != nil {
			return
		}
		return i.write(buff)
	}
	return i.write(data)
}

func (i *igniteWriter) flushAndGet() (result []byte, err error) {
	err = i.writer.Flush()
	result = i.buffer.Bytes()
	return
}

func createNewReader(content []byte) igniteReader {
	return igniteReader{content: content, reader: bytes.NewReader(content)}
}

func (r *igniteReader) readAny(data interface{}, dataType byte) (err error) {
	switch dataType {
	case typeString:
		size, err := r.readInt32()
		if err != nil {
			return err
		}
		buff := make([]byte, size)
		if err = binary.Read(r.reader, igniteOrder, &buff); err != nil {
			return err
		}
		result := data.(*string)
		*result = string(buff)
		return nil
	}
	return binary.Read(r.reader, igniteOrder, data)
}

func (r *igniteReader) readByte() (byte, error) {
	return r.reader.ReadByte()
}

func (r *igniteReader) readInt32() (data int32, err error) {
	err = binary.Read(r.reader, igniteOrder, &data)
	return
}

func (r *igniteReader) readUInt32() (data uint32, err error) {
	err = binary.Read(r.reader, igniteOrder, &data)
	return
}

func (r *igniteReader) readString() (result string, err error) {
	size, err := r.readUInt32()
	if err != nil {
		return "", err
	}
	buf := make([]byte, size)
	_, err = r.reader.Read(buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (r *igniteReader) readStringSize(size int) (result string, err error) {
	buf := make([]byte, size)
	_, err = r.reader.Read(buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (r *igniteReader) readUShort() (data uint16, err error) {
	err = binary.Read(r.reader, igniteOrder, &data)
	return
}

func (r *igniteReader) readUInt64() (data uint64, err error) {
	err = binary.Read(r.reader, igniteOrder, &data)
	return
}
