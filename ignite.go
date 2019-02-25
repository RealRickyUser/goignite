package goignite

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

const (
	opCacheGetNames            = uint16(1050)
	opCacheCreateWithName      = uint16(1051)
	opCacheGetOrCreateWithName = uint16(1052)
	opCacheDestroy             = uint16(1056)
)

// IgniteClient stores connection data and resources
type IgniteClient struct {
	conn           net.Conn
	requestCounter chan uint64
	// Error stores any operation error
	Error error
	// Address stores connection data (host and port) for client
	Address string
}

// NewClient creates and returns a new structure
func NewClient(address string) IgniteClient {
	return IgniteClient{Address: address, requestCounter: make(chan uint64, 10)}
}

func (i *IgniteClient) createConnection() error {
	conn, err := net.Dial("tcp", i.Address)
	if err != nil {
		return err
	}
	han := newHandshake()

	b := new(bytes.Buffer)
	writer := bufio.NewWriter(b)
	l := 8 + int32(len(han.username)) + int32(len(han.password))
	write(writer, l)
	write(writer, han.code)
	write(writer, han.major)
	write(writer, han.minor)
	write(writer, han.patch)
	write(writer, han.clientCode)
	write(writer, []byte(han.username))
	write(writer, []byte(han.password))
	_ = writer.Flush()
	_, err = conn.Write(b.Bytes())
	if err != nil {
		return err
	}
	resp := make([]byte, 5)
	_, err = conn.Read(resp)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(resp)
	var success byte
	l = readInt32(reader)
	success, _ = reader.ReadByte()
	if success == 1 {
		i.conn = conn
		go makeOperationIds(*i)
		return nil
	}
	defer conn.Close()
	resp = make([]byte, l-1)
	_, err = conn.Read(resp)
	if err != nil {
		return err
	}
	reader = bytes.NewReader(resp)
	serverErr := handshakeError{}
	serverErr.major = readUShort(reader)
	serverErr.minor = readUShort(reader)
	serverErr.patch = readUShort(reader)
	serverErr.message = string(resp[11:])
	return fmt.Errorf("error connecting to ignite [%s]: client [%d.%d.%d], server [%d.%d.%d]: %s",
		i.Address,
		han.major, han.minor, han.patch,
		serverErr.major, serverErr.minor, serverErr.patch, serverErr.message)
}

// makeOperationIds generates request Id's for Ignite
func makeOperationIds(i IgniteClient) {
	var counter uint64
	for {
		counter++
		i.requestCounter <- counter
	}
}

// Connect opens and verifies connection to Apache Ignite
func (i *IgniteClient) Connect() error {
	return i.createConnection()
}

// Close closes connection and chans.
// For reconnect use NewClient and Connect
func (i *IgniteClient) Close() {
	i.conn.Close()
	close(i.requestCounter)
}

// GetCacheNames returns list of cache names
func (i *IgniteClient) GetCacheNames() (result []string, e error) {
	request := requestHeader{requestId: <-i.requestCounter, code: opCacheGetNames}
	err := i.sendHeader(request)
	if err != nil {
		return nil, err
	}
	respHeader := i.getResponseHeader(opCacheGetNames)

	if request.requestId != respHeader.requestId {
		return nil, fmt.Errorf("wrong response id: expected %d, was %d", request.requestId, respHeader.requestId)
	}
	length := respHeader.len
	resp := make([]byte, length-12)
	_, err = i.conn.Read(resp)

	reader := bytes.NewReader(resp)
	err = binary.Read(reader, binary.LittleEndian, &length)
	for x := 0; x < int(length); x++ {
		reader.ReadByte() // pass a string data type
		res := readString(reader)
		result = append(result, res)
	}
	return result, nil
}

// GetOrCreateCache calls Ignite to create cache if not exists
func (i *IgniteClient) GetOrCreateCache(name string) error {
	return i.callIgniteWithStringArg(name, opCacheGetOrCreateWithName)
}

// CreateCache calls Ignite to create a new cache
func (i *IgniteClient) CreateCache(name string) error {
	return i.callIgniteWithStringArg(name, opCacheCreateWithName)
}

// callIgniteWithStringArg calls Ignite to do operation with opCode and sends a param
func (i *IgniteClient) callIgniteWithStringArg(name string, opCode uint16) error {
	request := requestHeader{requestId: <-i.requestCounter, code: opCode}
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	write(writer, typeString)
	write(writer, uint32(len(name)))
	write(writer, []byte(name))
	writer.Flush()
	request.content = buf.Bytes()
	err := i.sendHeader(request)
	if err != nil {
		return err
	}
	respHeader := i.getResponseHeader(opCode)
	if request.requestId != respHeader.requestId {
		return fmt.Errorf("wrong response id: expected %d, was %d", request.requestId, respHeader.requestId)
	}
	return respHeader.error
}

// DeleteCache calls Ignite to delete existing cache
func (i *IgniteClient) DeleteCache(name string) error {
	request := requestHeader{requestId: <-i.requestCounter, code: opCacheDestroy}
	buf := new(bytes.Buffer)
	writer := bufio.NewWriter(buf)
	hash := hashCode(name)
	write(writer, int32(hash))
	writer.Flush()
	request.content = buf.Bytes()
	err := i.sendHeader(request)
	if err != nil {
		return err
	}
	respHeader := i.getResponseHeader(opCacheDestroy)
	if request.requestId != respHeader.requestId {
		return fmt.Errorf("wrong response id: expected %d, was %d", request.requestId, respHeader.requestId)
	}
	return respHeader.error
}
