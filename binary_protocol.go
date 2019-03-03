package goignite

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const headerWithoutSize = 10

const (
	typeInt    = byte(3)
	typeString = byte(9)
)

type handshake struct {
	code       byte
	major      uint16
	minor      uint16
	patch      uint16
	clientCode byte
	username   string
	password   string
}

type handshakeError struct {
	major   uint16
	minor   uint16
	patch   uint16
	message string
}

type requestHeader struct {
	code      uint16
	requestId uint64
	content   []byte
}

type responseHeader struct {
	len          uint32
	requestId    uint64
	status       uint32
	errorMessage string
	error        error
	content      []byte
}

func newHandshake() handshake {
	return handshake{code: 1, major: 1, minor: 1, clientCode: 2}
}

func (i *IgniteClient) sendHeader(request requestHeader) error {
	w := createNewWriter()
	err := w.writeAll(uint32(len(request.content))+headerWithoutSize,
		request.code,
		request.requestId,
		request.content)
	if err != nil {
		return err
	}
	buff, err := w.flushAndGet()
	if err != nil {
		return err
	}
	_, err = i.conn.Write(buff)
	return err
}

func (i *IgniteClient) getResponseHeader(cmdId uint16) (r responseHeader) {
	resp := make([]byte, 16)
	_, err := i.conn.Read(resp)
	if err != nil {
		r.error = err
		return
	}
	reader := bytes.NewReader(resp)
	r.len = readUInt32(reader)
	r.requestId = readUInt64(reader)
	r.status = readUInt32(reader)

	if r.status != 0 {
		resp = make([]byte, r.len-12)
		_, err = i.conn.Read(resp)
		msg := string(resp[5:])
		r.errorMessage = fmt.Sprintf("error ignite request: %s %d", msg, cmdId)
		r.error = fmt.Errorf(r.errorMessage)
		return
	} else if r.len > 12 {
		r.content = make([]byte, r.len-12)
		_, err = i.conn.Read(r.content)
	}
	return
}

func readString(r *bytes.Reader) string {
	var size32 uint32
	binary.Read(r, binary.LittleEndian, &size32)
	buf := make([]byte, size32)
	r.Read(buf)
	return string(buf)
}

func readUShort(r *bytes.Reader) (data uint16) {
	binary.Read(r, binary.LittleEndian, &data)
	return
}

func readInt32(r *bytes.Reader) (data int32) {
	binary.Read(r, binary.LittleEndian, &data)
	return
}

func readUInt32(r *bytes.Reader) (data uint32) {
	binary.Read(r, binary.LittleEndian, &data)
	return
}

func readUInt64(r *bytes.Reader) (data uint64) {
	binary.Read(r, binary.LittleEndian, &data)
	return
}

func write(writer io.Writer, data interface{}) {
	binary.Write(writer, binary.LittleEndian, data)
}

// hashCode from java
func hashCode(name string) int32 {
	hash := 0
	var h = hash
	if len(name) > 0 {
		val := []byte(name)
		for i := 0; i < len(name); i++ {
			h = 31*h + int(val[i])
		}
		hash = h
	}
	return int32(h)
}
