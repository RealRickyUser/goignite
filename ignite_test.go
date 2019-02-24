package goignite

import (
	"bufio"
	"bytes"
	"testing"
)

func TestWriteByte(t *testing.T) {
	testWriteAny(t, byte(0x12), []byte{0x12})
}

func TestWriteShort(t *testing.T) {
	testWriteAny(t, uint16(0x3412), []byte{0x12, 0x34})
}

func TestWriteInt(t *testing.T) {
	testWriteAny(t, uint32(0x78563412), []byte{0x12, 0x34, 0x56, 0x78})
}

func TestWriteLong(t *testing.T) {
	testWriteAny(t, uint64(0x6473829178563412), []byte{0x12, 0x34, 0x56, 0x78, 0x91, 0x82, 0x73, 0x64})
}

func testWriteAny(t *testing.T, data interface{}, expected []byte) {
	buff := new(bytes.Buffer)
	writer := bufio.NewWriter(buff)
	write(writer, data)
	writer.Flush()
	received := buff.Bytes()
	if !bytes.Equal(received, expected) {
		t.Errorf("'write' makes incorrect long value, expected: %d, actual %d", expected, data)
	}
}
