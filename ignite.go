package goignite

import (
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
	// Error stores any operation error instead of connection error
	Error error
	// Address stores connection data (host and port) for client
	Address string
}

// NewClient creates and returns a new structure
func NewClient(address string) IgniteClient {
	return IgniteClient{Address: address, requestCounter: make(chan uint64, 10)}
}

func (i *IgniteClient) createConnection() (err error) {
	han, err := i.sendHandshakeRequest()
	if err != nil {
		return err
	}

	if err = i.receiveHandshakeResponse(han); err != nil {
		return err
	}
	go makeOperationIds(*i)
	return
}

func (i *IgniteClient) sendHandshakeRequest() (han handshake, err error) {
	i.conn, err = net.Dial("tcp", i.Address)
	if err != nil {
		return
	}

	han = newHandshake()
	writer := createNewWriter()
	rqSize := minHandshakeRequestSize + int32(len(han.username)) + int32(len(han.password))
	err = writer.writeAll(rqSize, han.code, han.major, han.minor, han.patch, han.clientCode, []byte(han.username), []byte(han.password))
	if err != nil {
		return
	}
	buff, err := writer.flushAndGet()
	if err != nil {
		return
	}
	_, err = i.conn.Write(buff)
	if err != nil {
		return
	}
	return
}

func (i *IgniteClient) receiveHandshakeResponse(han handshake) (err error) {
	resp := make([]byte, 5)
	_, err = i.conn.Read(resp)
	if err != nil {
		return
	}
	reader := createNewReader(resp)
	var success byte
	l, err := reader.readInt32()
	if err != nil {
		return
	}
	success, _ = reader.readByte()
	if success == 1 {
		return
	}
	defer i.Close() // close connection on exit
	resp = make([]byte, l-1)
	_, err = i.conn.Read(resp)
	if err != nil {
		return
	}
	reader = createNewReader(resp)
	serverErr := handshakeError{}
	if serverErr.major, err = reader.readUShort(); err != nil {
		return
	}
	if serverErr.minor, err = reader.readUShort(); err != nil {
		return
	}
	if serverErr.patch, err = reader.readUShort(); err != nil {
		return
	}
	if serverErr.message, err = reader.readStringSize(len(resp) - headerWithoutSize - 1); err != nil { // string(resp[headerWithoutSize+1:])
		return
	}
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
	_ = i.conn.Close()
	close(i.requestCounter)
	i.conn = nil
}

// GetCacheNames returns list of cache names
func (i *IgniteClient) GetCacheNames() (result []string, err error) {
	request := requestHeader{requestId: i.getNextOperationId(), code: opCacheGetNames}
	err = i.sendHeader(request)
	if err != nil {
		return nil, err
	}
	respHeader, err := i.getResponseHeader(opCacheGetNames)
	if err != nil {
		return
	}

	if request.requestId != respHeader.requestId {
		return nil, fmt.Errorf("wrong response id: expected %d, was %d", request.requestId, respHeader.requestId)
	}

	reader := createNewReader(respHeader.content)
	cacheCount, err := reader.readUInt32()
	if err != nil {
		return
	}
	for x := uint32(0); x < cacheCount; x++ {
		_, _ = reader.readByte()
		res, err := reader.readString()
		if err != nil {
			return nil, err
		}
		result = append(result, res)
	}
	return result, nil
}

// GetOrCreateCache calls Ignite to create cache if not exists
func (i *IgniteClient) GetOrCreateCache(name string) (cache IgniteCache, err error) {
	return i.callCacheAction(name, opCacheGetOrCreateWithName)
}

// CreateCache calls Ignite to create a new cache
func (i *IgniteClient) CreateCache(name string) (cache IgniteCache, err error) {
	return i.callCacheAction(name, opCacheCreateWithName)
}

func (i *IgniteClient) callCacheAction(name string, opCode uint16) (cache IgniteCache, err error) {
	err = i.callIgniteWithStringArg(name, opCode)
	if err == nil {
		cache = IgniteCache{cacheName: name, cacheHashCode: hashCode(name), client: i}
	}
	return
}

// callIgniteWithStringArg calls Ignite to do operation with opCode and sends a param
func (i *IgniteClient) callIgniteWithStringArg(name string, opCode uint16) (err error) {
	request := requestHeader{requestId: i.getNextOperationId(), code: opCode}
	writer := createNewWriter()
	err = writer.writeAll(typeString, uint32(len(name)), []byte(name))
	if err != nil {
		return err
	}
	buff, err := writer.flushAndGet()
	if err != nil {
		return err
	}
	request.content = buff
	err = i.sendHeader(request)
	if err != nil {
		return err
	}
	respHeader, err := i.getResponseHeader(opCode)
	if err != nil {
		return
	}
	if request.requestId != respHeader.requestId {
		return fmt.Errorf("wrong response id: expected %d, was %d", request.requestId, respHeader.requestId)
	}
	return respHeader.error
}

// DeleteCache calls Ignite to delete existing cache
func (i *IgniteClient) DeleteCache(name string) (err error) {
	request := requestHeader{requestId: i.getNextOperationId(), code: opCacheDestroy}

	writer := createNewWriter()
	err = writer.writeAll(hashCode(name))
	if err != nil {
		return err
	}
	buff, err := writer.flushAndGet()
	if err != nil {
		return err
	}
	request.content = buff

	err = i.sendHeader(request)
	if err != nil {
		return err
	}
	respHeader, err := i.getResponseHeader(opCacheDestroy)
	if err != nil {
		return
	}
	if request.requestId != respHeader.requestId {
		return fmt.Errorf("wrong response id: expected %d, was %d", request.requestId, respHeader.requestId)
	}
	return respHeader.error
}

func (i *IgniteClient) getNextOperationId() uint64 {
	return <-i.requestCounter
}
