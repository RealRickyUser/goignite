package goignite

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

const (
	opCacheGetNames = uint16(1050)
)

type IgniteClient struct {
	conn    net.Conn
	Error   error
	Address string
}

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
}

type responseHeader struct {
	len          uint32
	requestId    uint64
	status       uint32
	errorMessage string
	error        error
}

func newHandshake() handshake {
	return handshake{code: 1, major: 1, minor: 1, clientCode: 2}
}

func NewClient(address string) IgniteClient {
	return IgniteClient{Address: address}
}

func (i *IgniteClient) createConnection() error {
	conn, err := net.Dial("tcp", i.Address)
	if err != nil {
		return err
	}
	//defer conn.Close()
	han := newHandshake()

	b := new(bytes.Buffer)
	writer := bufio.NewWriter(b)
	l := 8 + int32(len(han.username)) + int32(len(han.password))
	_ = binary.Write(writer, binary.LittleEndian, l)
	_ = binary.Write(writer, binary.LittleEndian, han.code)
	_ = binary.Write(writer, binary.LittleEndian, han.major)
	_ = binary.Write(writer, binary.LittleEndian, han.minor)
	_ = binary.Write(writer, binary.LittleEndian, han.patch)
	_ = binary.Write(writer, binary.LittleEndian, han.clientCode)
	_ = binary.Write(writer, binary.LittleEndian, []byte(han.username))
	_ = binary.Write(writer, binary.LittleEndian, []byte(han.password))
	_ = writer.Flush()
	_, err = conn.Write(b.Bytes())
	if err != nil {
		return err
	}
	//fmt.Printf("Sended %d bytes\n", n)
	resp := make([]byte, 5)
	_, err = conn.Read(resp)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(resp)
	var success byte
	err = binary.Read(reader, binary.LittleEndian, &l)
	err = binary.Read(reader, binary.LittleEndian, &success)
	if success == 1 {
		i.conn = conn
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
	_ = binary.Read(reader, binary.LittleEndian, &serverErr.major)
	_ = binary.Read(reader, binary.LittleEndian, &serverErr.minor)
	_ = binary.Read(reader, binary.LittleEndian, &serverErr.patch)
	serverErr.message = string(resp[11:])
	return fmt.Errorf("error connecting to ignite [%s]: client [%d.%d.%d], server [%d.%d.%d]: %s",
		i.Address,
		han.major, han.minor, han.patch,
		serverErr.major, serverErr.minor, serverErr.patch, serverErr.message)
}

func (i *IgniteClient) Connect() error {
	return i.createConnection()
}

func (i *IgniteClient) Close() {
	defer i.conn.Close()
}

func (i *IgniteClient) sendHeader(request requestHeader) error {
	buff := new(bytes.Buffer)
	writer := bufio.NewWriter(buff)
	_ = binary.Write(writer, binary.LittleEndian, uint32(10))
	_ = binary.Write(writer, binary.LittleEndian, request.code)
	_ = binary.Write(writer, binary.LittleEndian, request.requestId)
	_ = writer.Flush()
	fmt.Println(buff.Bytes())
	_, err := i.conn.Write(buff.Bytes())
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
	err = binary.Read(reader, binary.LittleEndian, &r.len)
	err = binary.Read(reader, binary.LittleEndian, &r.requestId)
	err = binary.Read(reader, binary.LittleEndian, &r.status)

	//if rqUid != requestId {
	//	return nil, fmt.Errorf("wrong response id: expected %d, was %d", rqUid, requestId)
	//}
	//fmt.Println(resp)
	if r.status != 0 {
		resp = make([]byte, r.len-12)
		_, err = i.conn.Read(resp)
		msg := string(resp[5:])
		r.errorMessage = fmt.Sprintf("error ignite request: %s %d", msg, cmdId)
		r.error = fmt.Errorf(r.errorMessage)
		return
	}
	return
}

func (i *IgniteClient) GetCacheNames() (result []string, e error) {
	request := requestHeader{requestId: 1, code: opCacheGetNames}
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
	//fmt.Printf("Cache count: %d\n", length)
	var sep byte
	var size uint32
	for x := 0; x < int(length); x++ {
		err = binary.Read(reader, binary.LittleEndian, &sep)
		err = binary.Read(reader, binary.LittleEndian, &size)
		buf := make([]byte, size)
		_, _ = reader.Read(buf)
		result = append(result, string(buf))
	}
	return result, nil
}
