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
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"go.uber.org/atomic"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

import (
	dubboCommon "github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/apache/dubbo-go/protocol"
	"github.com/golang/protobuf/proto"
	h2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
)

// load codec support impl, can be import every where
import (
	"github.com/dubbogo/triple/internal/codec"
	_ "github.com/dubbogo/triple/internal/codec"
	"github.com/dubbogo/triple/pkg/common"
)

// H2Controller is an important object of h2 protocol
// it can be used as serverEnd and clientEnd, identified by isServer field
// it can shake hand by client or server end, to start triple transfer
// higher layer can call H2Controller's StreamInvoke or UnaryInvoke method to deal with event
// it maintains streamMap, with can contain have many higher layer object: stream, used by event driven.
// it maintains the data stream from lower layer's net/h2frame to higher layer's stream/userStream
type H2Controller struct {
	// conn is used to init h2 framer
	conn net.Conn

	//
	address string

	// streamMap store all non-closed stream, key by streamID (uint32)
	streamMap sync.Map

	// mdMap strMap is used to store discover of user impl function
	mdMap  map[string]grpc.MethodDesc
	strMap map[string]grpc.StreamDesc

	// url is to get protocol, which is key of triple components, like codec header
	// url is also used to init triple header
	url *dubboCommon.URL

	handler    common.ProtocolHeaderHandler
	pkgHandler common.PackageHandler
	service    Dubbo3GrpcService

	// state shows conn state, after receiving go away frame, state will be set to draining
	// so as not to receive request from dubbo3 client, and client will new an other conn to replace
	state common.H2ControllerState
}

const defaultReadBuffer = 1000000

func skipHeader(frameData []byte) ([]byte, uint32) {
	if len(frameData) < 5 {
		return []byte{}, 0
	}
	lineHeader := frameData[:5]
	length := binary.BigEndian.Uint32(lineHeader[1:])
	return frameData[5:], length
}

func (hc *H2Controller) readSplitedDatas(rBody io.ReadCloser) chan BufferMsg {
	cbm := make(chan BufferMsg)
	go func() {
		buf := make([]byte, defaultReadBuffer)
		for {
			splitBuffer := BufferMsg{
				buffer: bytes.NewBuffer(make([]byte, 0)),
			}

			fromFrameHeaderDataSize := uint32(0)
			for {
				var n int
				var err error
				if splitBuffer.buffer.Len() < int(fromFrameHeaderDataSize) || splitBuffer.buffer.Len() == 0 {
					n, err = rBody.Read(buf)
				}

				if err != nil {
					cbm <- BufferMsg{
						msgType: ServerStreamCloseMsgType,
					}
					return
				}
				splitedData := buf[:n]
				splitBuffer.buffer.Write(splitedData)
				if fromFrameHeaderDataSize == 0 {
					// should parse data frame header first
					data := splitBuffer.buffer.Bytes()
					var totalSize uint32
					if data, totalSize = skipHeader(data); totalSize == 0 {
						break
					} else {
						fromFrameHeaderDataSize = totalSize
					}
					splitBuffer.buffer.Reset()
					splitBuffer.buffer.Write(data)
				}
				if splitBuffer.buffer.Len() >= int(fromFrameHeaderDataSize) {
					allDataBody := make([]byte, fromFrameHeaderDataSize)
					splitBuffer.buffer.Read(allDataBody)
					cbm <- BufferMsg{
						buffer:  bytes.NewBuffer(allDataBody),
						msgType: DataMsgType,
					}
					fromFrameHeaderDataSize = 0
				}
			}
		}
	}()
	return cbm
}

func (hc *H2Controller) GetHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		fm := h2.MetaHeadersFrame{
			HeadersFrame: &h2.HeadersFrame{
				FrameHeader: h2.FrameHeader{},
			},
		}
		for k, v := range r.Header {
			fm.Fields = append(fm.Fields, hpack.HeaderField{
				Name:  k,
				Value: v[0],
			})
		}
		fm.Fields = append(fm.Fields, hpack.HeaderField{
			Name:  ":path",
			Value: r.URL.Path,
		})

		header := hc.handler.ReadFromH2MetaHeader(&fm)
		hc.addServerStream(header)

		val, ok := hc.streamMap.Load(fm.StreamID)
		if !ok {
			logger.Error("with not exist stream id")
			return
		}
		stream := val.(stream)
		sendChan := stream.getSend()

		ch := hc.readSplitedDatas(r.Body)

		go func() {
			for {
				msgData := <-ch
				if msgData.msgType == ServerStreamCloseMsgType {
					fmt.Println("read from client stop!")
					return
				}
				data := hc.pkgHandler.Pkg2FrameData(msgData.buffer.Bytes())
				stream.putRecv(data, DataMsgType)
			}
		}()

		w.Header().Add("content-type", "application/grpc")
		w.Header().Add(h2.TrailerPrefix+"grpc-status", strconv.Itoa(int(0))) // sendMsg.st.Code()
		w.Header().Add(h2.TrailerPrefix+"grpc-message", encodeGrpcMessage(""))

		for {
			sendMsg := <-sendChan
			if sendMsg.buffer == nil || sendMsg.msgType != DataMsgType {
				return
			}
			sendData := sendMsg.buffer.Bytes()
			w.Write(sendData)
		}
	}
}

// Dubbo3GrpcService is gRPC service, used to check impl
type Dubbo3GrpcService interface {
	// SetProxyImpl sets proxy.
	SetProxyImpl(impl protocol.Invoker)
	// GetProxyImpl gets proxy.
	GetProxyImpl() protocol.Invoker
	// ServiceDesc gets an RPC service's specification.
	ServiceDesc() *grpc.ServiceDesc
}

func getMethodAndStreamDescMap(ds Dubbo3GrpcService) (map[string]grpc.MethodDesc, map[string]grpc.StreamDesc, error) {
	sdMap := make(map[string]grpc.MethodDesc, 8)
	strMap := make(map[string]grpc.StreamDesc, 8)
	for _, v := range ds.ServiceDesc().Methods {
		sdMap[v.MethodName] = v
	}
	for _, v := range ds.ServiceDesc().Streams {
		strMap[v.StreamName] = v
	}
	return sdMap, strMap, nil
}

// NewH2Controller can create H2Controller with conn
func NewH2Controller(conn net.Conn, isServer bool, service Dubbo3GrpcService, url *dubboCommon.URL) (*H2Controller, error) {
	var mdMap map[string]grpc.MethodDesc
	var strMap map[string]grpc.StreamDesc
	var err error
	if isServer {
		mdMap, strMap, err = getMethodAndStreamDescMap(service)
		if err != nil {
			logger.Error("new H2 controller error:", err)
			return nil, err
		}
	}

	fm := h2.NewFramer(conn, conn)
	// another change of fm readframe size is when receiving settig to update in handleSettingFrame()
	fm.SetMaxReadFrameSize(common.DefaultMaxFrameSize)
	fm.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	var headerHandler common.ProtocolHeaderHandler
	var pkgHandler common.PackageHandler

	if url != nil {
		headerHandler, _ = common.GetProtocolHeaderHandler(url.Protocol)
		pkgHandler, _ = common.GetPackagerHandler(url.Protocol)
	}
	defaultMaxCurrentStream := atomic.Uint32{}
	defaultMaxCurrentStream.Store(math.MaxUint32)

	h2c := &H2Controller{
		conn:       conn,
		url:        url,
		streamMap:  sync.Map{},
		mdMap:      mdMap,
		strMap:     strMap,
		service:    service,
		handler:    headerHandler,
		pkgHandler: pkgHandler,
		state:      common.Reachable,
	}
	return h2c, nil
}

// addServerStream can create a serverStream and add to h2Controller by @data read from frame,
// after receiving a request from client.
func (h *H2Controller) addServerStream(data common.ProtocolHeader) {

	methodName := strings.Split(data.GetMethod(), "/")[2]
	md, okm := h.mdMap[methodName]
	streamd, oks := h.strMap[methodName]
	if !okm && !oks {
		logger.Errorf("method name %s not found in desc", methodName)
		return
	}
	var newstm *serverStream
	var err error
	if okm {
		newstm, err = newServerStream(data, md, h.url, h.service)
		if err != nil {
			logger.Error("newServerStream error", err)
			return
		}
		// now, unary stream is only invoked by http2 api
		//go h.runSendUnaryRsp(newstm)
	} else {
		newstm, err = newServerStream(data, streamd, h.url, h.service)
		if err != nil {
			logger.Error("newServerStream error", err)
			return
		}
		// go h.runSendStreamRsp(newstm)
	}
	h.streamMap.Store(newstm.ID, newstm)
}

// StreamInvoke can start streaming invocation, called by triple client
func (h *H2Controller) StreamInvoke(ctx context.Context, method string) (grpc.ClientStream, error) {
	clientStream := newClientStream(0)
	serilizer, err := common.GetDubbo3Serializer(codec.DefaultDubbo3SerializerName)
	if err != nil {
		logger.Error("get serilizer error = ", err)
		return nil, err
	}

	client := http.Client{
		Transport: &h2.Transport{
			// Pretend we are dialing a TLS endpoint. (Note, we ignore the passed tls.Config)
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}

	tosend := clientStream.getSend()
	sendStreamChan := make(chan h2.BufferMsg)
	go func() {
		for {
			select {
			case sendMsg := <-tosend:
				sendStreamChan <- h2.BufferMsg{
					Buffer:  bytes.NewBuffer(sendMsg.buffer.Bytes()),
					MsgType: h2.MsgType(sendMsg.msgType),
				}
			default:

			}
		}
	}()
	stremaReq := h2.StreamingRequest{
		SendChan: sendStreamChan,
	}
	go func() {
		rsp, err := client.Post("https://"+h.address+method, "application/grpc", &stremaReq)
		if err != nil {
			panic(err)
		}
		ch := h.readSplitedDatas(rsp.Body)
		for {
			data := <-ch
			if data.buffer == nil {
				return
			}
			pkg := h.pkgHandler.Pkg2FrameData(data.buffer.Bytes())
			clientStream.putRecv(pkg, DataMsgType)
		}
	}()

	//buf.Write(h.pkgHandler.Pkg2FrameData(data))

	// 4. start receive data to send
	pkgHandler, err := common.GetPackagerHandler(h.url.Protocol)
	return newClientUserStream(clientStream, serilizer, pkgHandler), nil
}

// UnaryInvoke can start unary invocation, called by dubbo3 client
func (h *H2Controller) UnaryInvoke(ctx context.Context, method string, addr12 string, data []byte, reply interface{}, url *dubboCommon.URL) error {
	client := http.Client{
		Transport: &h2.Transport{
			// Pretend we are dialing a TLS endpoint. (Note, we ignore the passed tls.Config)
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}

	sendStreamChan := make(chan h2.BufferMsg, 1)
	sendStreamChan <- h2.BufferMsg{
		Buffer:  bytes.NewBuffer(h.pkgHandler.Pkg2FrameData(data)),
		MsgType: h2.MsgType(DataMsgType),
	}

	stremaReq := h2.StreamingRequest{
		SendChan: sendStreamChan,
	}

	rsp, err := client.Post("https://"+h.address+method, "application/grpc", &stremaReq)
	if err != nil {
		panic(err)
	}
	readBuf := make([]byte, 1000000)

	if err != nil {
		panic(err)
	}

	splitBuffer := BufferMsg{
		buffer: bytes.NewBuffer(make([]byte, 0)),
	}
	fromFrameHeaderDataSize := uint32(0)
	for {
		n, err := rsp.Body.Read(readBuf)
		if err != nil {
			panic(err)
		}
		splitedData := readBuf[:n]
		if fromFrameHeaderDataSize == 0 {
			// should parse data frame header first
			var totalSize uint32
			if splitedData, totalSize = h.pkgHandler.Frame2PkgData(splitedData); totalSize == 0 {
				return nil
			} else {
				fromFrameHeaderDataSize = totalSize
			}
			splitBuffer.buffer.Reset()
		}
		splitBuffer.buffer.Write(splitedData)
		if splitBuffer.buffer.Len() > int(fromFrameHeaderDataSize) {
			panic("Receive Splited Data is bigger than wanted!!!")
			return nil
		}

		if splitBuffer.buffer.Len() == int(fromFrameHeaderDataSize) {
			break
		}
	}

	if err := proto.Unmarshal(splitBuffer.buffer.Bytes(), reply.(proto.Message)); err != nil {
		logger.Error("client unmarshal rsp err:", err)
		return err
	}
	return nil
}

func (h2 *H2Controller) close() {
	h2.streamMap.Range(func(k, v interface{}) bool {
		v.(stream).close()
		return true
	})
	h2.state = common.Closing
}
