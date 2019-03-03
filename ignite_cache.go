package goignite

import "fmt"

const (
	opCacheGet = uint16(1000)
	opCachePut = uint16(1001)
)

// IgniteCache stores IgniteClient instance and cache name
type IgniteCache struct {
	client        *IgniteClient
	cacheName     string
	cacheHashCode int32
}

// Put puts key&value into cache
func (c *IgniteCache) Put(key interface{}, value interface{}) (err error) {
	if err = validateDataType(key); err != nil {
		return
	}
	if err = validateDataType(value); err != nil {
		return
	}

	request := requestHeader{requestId: c.client.getNextOperationId(), code: opCachePut}

	writer := createNewWriter()
	err = writer.writeAll(c.cacheHashCode, byte(0))
	if err != nil {
		return err
	}
	err = writer.writeField(key)
	if err != nil {
		return
	}
	err = writer.writeField(value)
	if err != nil {
		return
	}

	buff, err := writer.flushAndGet()
	if err != nil {
		return err
	}
	request.content = buff

	err = c.client.sendHeader(request)
	if err != nil {
		return err
	}
	respHeader, err := c.client.getResponseHeader(opCachePut)
	if err != nil {
		return
	}
	if request.requestId != respHeader.requestId {
		return fmt.Errorf("wrong response id: expected %d, was %d", request.requestId, respHeader.requestId)
	}
	return respHeader.error
}

// Get return value from cache by key
func (c *IgniteCache) Get(key interface{}, value interface{}) (err error) {
	if err = validateDataType(key); err != nil {
		return
	}
	if err = validateDataType(value); err != nil {
		return
	}

	request := requestHeader{requestId: c.client.getNextOperationId(), code: opCacheGet}

	writer := createNewWriter()
	if err = writer.writeAll(c.cacheHashCode, byte(0)); err != nil {
		return
	}
	err = writer.writeField(key)
	if err != nil {
		return
	}
	buff, err := writer.flushAndGet()
	if err != nil {
		return
	}
	request.content = buff

	err = c.client.sendHeader(request)
	if err != nil {
		return
	}
	respHeader, err := c.client.getResponseHeader(opCacheGet)
	if err != nil {
		return
	}
	if request.requestId != respHeader.requestId {
		return fmt.Errorf("wrong response id: expected %d, was %d", request.requestId, respHeader.requestId)
	}

	reader := createNewReader(respHeader.content)
	expectedValueType := getDataType(value)
	actual, err := reader.readByte()
	if actual != expectedValueType {
		return fmt.Errorf("get key from cache: incorrect value data type from cache: expected %d, actual %d", expectedValueType, actual)
	}
	return reader.readAny(value, actual)
}

func validateDataType(data interface{}) (err error) {
	switch data.(type) {
	case int8, uint8, *int8, *uint8:
	case int16, uint16, *int16, *uint16:
	case int32, uint32, *int32, *uint32:
	case int64, uint64, *int64, *uint64:
	case float32, float64, *float32, *float64:
	case bool, *bool:
	case string, *string:
		return
	default:
		err = fmt.Errorf("validateDataType: unsupported data type %T", data)
	}
	return
}

func getDataType(data interface{}) byte {
	switch data.(type) {
	case int8, uint8, *int8, *uint8:
		return typeByte
	case int16, uint16, *int16, *uint16:
		return typeShort
	case int32, uint32, *int32, *uint32:
		return typeInt
	case int64, uint64, *int64, *uint64:
		return typeLong
	case float32, *float32:
		return typeFloat
	case float64, *float64:
		return typeDouble
	case bool, *bool:
		return typeBool
	case string, *string:
		return typeString
	}
	return typeError
}
