package goignite

import (
	"fmt"
)

const (
	headerWithoutSize                  = 10
	minHandshakeRequestSize            = 8
	handshakeResponseHeaderSize        = 12
	handshakeResponseHeaderWithLenSize = 16
)

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

func (i *IgniteClient) getResponseHeader(cmdId uint16) (r responseHeader, err error) {
	resp := make([]byte, handshakeResponseHeaderWithLenSize)
	_, err = i.conn.Read(resp)
	if err != nil {
		r.error = err
		return
	}
	reader := createNewReader(resp)
	if r.len, err = reader.readUInt32(); err != nil {
		return
	}
	if r.requestId, err = reader.readUInt64(); err != nil {
		return
	}
	if r.status, err = reader.readUInt32(); err != nil {
		return
	}

	if r.status != 0 {
		resp = make([]byte, r.len-handshakeResponseHeaderSize)
		_, err = i.conn.Read(resp)
		msg := string(resp[5:]) // first 4 bytes - is a length of string
		r.errorMessage = fmt.Sprintf("error ignite request: %s %d", msg, cmdId)
		r.error = fmt.Errorf(r.errorMessage)
		return
	} else if r.len > handshakeResponseHeaderSize {
		r.content = make([]byte, r.len-handshakeResponseHeaderSize)
		_, err = i.conn.Read(r.content)
	}
	return
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
