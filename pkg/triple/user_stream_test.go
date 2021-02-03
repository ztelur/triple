package triple

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type TestRPCService struct {
}

func (t *TestRPCService) Reference() string {
	return ""
}

func TestBaseUserStream(t *testing.T) {
	service := &TestRPCService{}
	baseUserStream := newBaseStream(1, service)
	assert.NotNil(t, baseUserStream)
	assert.Equal(t, baseUserStream.ID, uint32(1))
	assert.Equal(t, baseUserStream.service, service)

	// get msg
	sendGetChan := baseUserStream.getSend()
	recvGetChan := baseUserStream.getRecv()
	closeChan := make(chan struct{})
	testData := []byte("test message of TestBaseUserStream")
	counter := 0
	go func() {
		for {
			select {
			case <-closeChan:
				baseUserStream.closeWithError(errors.New("close error"))
				return
			case sendGetBuf := <-sendGetChan:
				counter++
				assert.Equal(t, sendGetBuf.buffer.Bytes(), testData)
				assert.Equal(t, sendGetBuf.msgType, DataMsgType)
			case recvGetBuf := <-recvGetChan:
				counter++
				assert.Equal(t, recvGetBuf.buffer.Bytes(), testData)
				assert.Equal(t, recvGetBuf.msgType, ServerStreamCloseMsgType)
			}
		}

	}()

	// put msg
	for i := 0; i < 500; i++ {
		baseUserStream.putRecv(testData, ServerStreamCloseMsgType)
		baseUserStream.putSend(testData, DataMsgType)
	}
	time.Sleep(time.Second)
	assert.Equal(t, counter, 1000)
	closeChan <- struct{}{}
	closeMsg := <-recvGetChan
	assert.Equal(t, closeMsg.msgType, ServerStreamCloseMsgType)
	assert.Equal(t, closeMsg.err, errors.New("close error"))
}
