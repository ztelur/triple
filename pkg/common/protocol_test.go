package common

import (
	"context"
	dubboCommon "github.com/apache/dubbo-go/common"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"gotest.tools/assert"
	"reflect"
	"testing"
)

type ImplProtocolHeader struct {
	Method   string
	StreamID uint32
}

func (t *ImplProtocolHeader) GetMethod() string {
	return t.Method
}
func (t *ImplProtocolHeader) GetStreamID() uint32 {
	return t.StreamID
}

// FieldToCtx parse triple Header that user defined, to ctx of server end
func (t *ImplProtocolHeader) FieldToCtx() context.Context {
	return context.Background()
}

type ImplProtocolHeaderHandler struct {
}

func (ihh *ImplProtocolHeaderHandler) ReadFromH2MetaHeader(frame *http2.MetaHeadersFrame) ProtocolHeader {
	return &ImplProtocolHeader{}
}

func (hh ImplProtocolHeaderHandler) WriteHeaderField(url *dubboCommon.URL, ctx context.Context, headerFields []hpack.HeaderField) []hpack.HeaderField {
	return nil
}

func NewTestHeaderHandler() ProtocolHeaderHandler {
	return &ImplProtocolHeaderHandler{}
}

func TestSetAndGetProtocolHeaderHandler(t *testing.T) {
	oriHandler := NewTestHeaderHandler()
	SetProtocolHeaderHandler("test-protocol", NewTestHeaderHandler)
	handler, err := GetProtocolHeaderHandler("test-protocol")
	assert.Equal(t, err, nil)
	assert.Equal(t, reflect.TypeOf(handler), reflect.TypeOf(oriHandler))
}

type TestTriplePackageHandler struct {
}

func (t *TestTriplePackageHandler) Frame2PkgData(frameData []byte) ([]byte, uint32) {
	return frameData, 0
}
func (t *TestTriplePackageHandler) Pkg2FrameData(pkgData []byte) []byte {
	return pkgData
}

func newTestTriplePackageHandler() PackageHandler {
	return &TestTriplePackageHandler{}
}

func TestSetAndGetGetPackagerHandler(t *testing.T) {
	oriHandler := newTestTriplePackageHandler()
	SetPackageHandler("test-protocol", newTestTriplePackageHandler)
	handler, err := GetPackagerHandler("test-protocol")
	assert.Equal(t, err, nil)
	assert.Equal(t, reflect.TypeOf(handler), reflect.TypeOf(oriHandler))
}

type TestDubbo3Serializer struct {
}

func (p *TestDubbo3Serializer) Marshal(v interface{}) ([]byte, error) {
	return []byte{}, nil
}
func (p *TestDubbo3Serializer) Unmarshal(data []byte, v interface{}) error {
	return nil
}

func newTestDubbo3Serializer() Dubbo3Serializer {
	return &TestDubbo3Serializer{}
}

func TestGetAndSetSerilizer(t *testing.T) {
	oriSerializer := newTestDubbo3Serializer()
	SetDubbo3Serializer("test-protocol", newTestDubbo3Serializer)
	ser, err := GetDubbo3Serializer("test-protocol")
	assert.Equal(t, err, nil)
	assert.Equal(t, reflect.TypeOf(ser), reflect.TypeOf(oriSerializer))
}
