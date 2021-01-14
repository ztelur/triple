/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package triple

import (
	"bytes"
)
import (
	dubboCommon "github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	"google.golang.org/grpc"
)
import (
	"github.com/dubbogo/triple/internal/status"
	"github.com/dubbogo/triple/pkg/common"
)

////////////////////////////////Buffer and MsgType

// MsgType show the type of Message in buffer
type MsgType uint8

const (
	DataMsgType              = MsgType(1)
	ServerStreamCloseMsgType = MsgType(2)
)

// BufferMsg is the basic transfer unit in one stream
type BufferMsg struct {
	buffer  *bytes.Buffer
	msgType MsgType
	st      *status.Status
	err     error
}

// GetMsgType can get buffer's type
func (bm *BufferMsg) GetMsgType() MsgType {
	return bm.msgType
}

// MsgBuffer contain the chan of BufferMsg
type MsgBuffer struct {
	c   chan BufferMsg
	err error
}

func newRecvBuffer() *MsgBuffer {
	b := &MsgBuffer{
		c: make(chan BufferMsg, 1),
	}
	return b
}

func (b *MsgBuffer) put(r BufferMsg) {
	b.c <- r
}

func (b *MsgBuffer) get() <-chan BufferMsg {
	return b.c
}

/////////////////////////////////stream state

type streamState uint32

const (
	open      = streamState(0)
	halfClose = streamState(1)
	closed    = streamState(2)
)

/////////////////////////////////stream
// stream is not only a buffer stream
// but an abstruct stream in h2 defination
type stream interface {
	getStreamID() uint32

	// channel usage
	putRecv(data []byte, msgType MsgType)
	putSend(data []byte, msgType MsgType)
	putRecvErr(err error)
	getSend() <-chan BufferMsg
	getRecv() <-chan BufferMsg
	close()

	// state usage
	getState() streamState
	setState(ss streamState)
	onRecvEs() streamState
}

// baseStream is the basic  impl of stream interface, it impl for basic function of stream
type baseStream struct {
	ID      uint32
	recvBuf *MsgBuffer
	sendBuf *MsgBuffer
	url     *dubboCommon.URL
	header  common.ProtocolHeader
	service dubboCommon.RPCService

	// On client-side it is the status error received from the server.
	// On server-side it is unused.
	status *status.Status

	state streamState
}

func (s *baseStream) getStreamID() uint32 {
	return s.ID
}

func (s *baseStream) WriteStatus(st *status.Status) {
	s.sendBuf.put(BufferMsg{
		st:      st,
		msgType: ServerStreamCloseMsgType,
	})
}

func (s *baseStream) putRecv(data []byte, msgType MsgType) {
	s.recvBuf.put(BufferMsg{
		buffer:  bytes.NewBuffer(data),
		msgType: msgType,
	})
}

func (s *baseStream) putRecvErr(err error) {
	s.recvBuf.put(BufferMsg{
		err:     err,
		msgType: ServerStreamCloseMsgType,
	})
}

func (s *baseStream) putSend(data []byte, msgType MsgType) {
	s.sendBuf.put(BufferMsg{
		buffer:  bytes.NewBuffer(data),
		msgType: msgType,
	})
}

func (s *baseStream) getRecv() <-chan BufferMsg {
	return s.recvBuf.get()
}

func (s *baseStream) getSend() <-chan BufferMsg {
	return s.sendBuf.get()
}

func (s *baseStream) getState() streamState {
	return s.state
}

func (s *baseStream) setState(ss streamState) {
	logger.Debug("setState, set to close")
	s.state = ss
}

func (s *baseStream) onRecvEs() streamState {
	if s.state == open {
		logger.Debug("onRecvEs, change to half Close")
		s.state = halfClose
	} else if s.state == halfClose {
		logger.Debug("onRecvEs, change to closed")
		s.state = closed
	}
	return s.state
}

func newBaseStream(streamID uint32, service dubboCommon.RPCService) *baseStream {
	// stream and pkgHeader are the same level
	return &baseStream{
		ID:      streamID,
		recvBuf: newRecvBuffer(),
		sendBuf: newRecvBuffer(),
		service: service,
		state:   open,
	}
}

// serverStream is running in server end
type serverStream struct {
	baseStream
	processor processor
	header    common.ProtocolHeader
}

func (ss *serverStream) close() {
	ss.processor.close()
	close(ss.sendBuf.c)
	close(ss.recvBuf.c)
}

func newServerStream(header common.ProtocolHeader, desc interface{}, url *dubboCommon.URL, service dubboCommon.RPCService) (*serverStream, error) {
	baseStream := newBaseStream(header.GetStreamID(), service)

	serverStream := &serverStream{
		baseStream: *baseStream,
		header:     header,
	}
	pkgHandler, err := common.GetPackagerHandler(url.Protocol)
	if err != nil {
		logger.Error("GetPkgHandler error with err = ", err)
		return nil, err
	}
	if methodDesc, ok := desc.(grpc.MethodDesc); ok {
		// pkgHandler and processor are the same level
		serverStream.processor, err = newUnaryProcessor(serverStream, pkgHandler, methodDesc)
	} else if streamDesc, ok := desc.(grpc.StreamDesc); ok {
		serverStream.processor, err = newStreamingProcessor(serverStream, pkgHandler, streamDesc)
	} else {
		logger.Error("grpc desc invalid:", desc)
		return nil, nil
	}

	serverStream.processor.runRPC()

	return serverStream, nil
}

func (s *serverStream) getService() dubboCommon.RPCService {
	return s.service
}

func (s *serverStream) getHeader() common.ProtocolHeader {
	return s.header
}

// clientStream is running in client end
type clientStream struct {
	baseStream
	closeChan chan struct{}
}

func newClientStream(streamID uint32) *clientStream {
	baseStream := newBaseStream(streamID, nil)
	newclientStream := &clientStream{
		baseStream: *baseStream,
		closeChan:  make(chan struct{}, 1),
	}
	return newclientStream
}

func (cs *clientStream) close() {
	cs.closeChan <- struct{}{}
	close(cs.sendBuf.c)
	close(cs.recvBuf.c)
}

func (cs *clientStream) runSendDataToServerStream(sendChan chan interface{}) {
	send := cs.getSend()
	for {
		select {
		case <-cs.closeChan:
			return
		case sendMsg := <-send:
			sendData := sendMsg.buffer.Bytes()
			sendChan <- sendChanDataPkg{
				streamID:  cs.ID,
				endStream: false,
				data:      sendData,
			}
		}

	}
}
