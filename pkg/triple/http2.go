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
	"fmt"
	"github.com/dubbogo/triple/internal/codes"
	"github.com/dubbogo/triple/internal/status"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

import (
	dubboCommon "github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/apache/dubbo-go/protocol"
	"github.com/golang/protobuf/proto"
	perrors "github.com/pkg/errors"
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

type h2Configure struct {
	MaxActiveStream uint32
}

var defaultH2Configure = h2Configure{
	MaxActiveStream: 100,
}

// H2Controller is an important object of h2 protocol
// it can be used as serverEnd and clientEnd, identified by isServer field
// it can shake hand by client or server end, to start triple transfer
// higher layer can call H2Controller's StreamInvoke or UnaryInvoke method to deal with event
// it maintains streamMap, with can contain have many higher layer object: stream, used by event driven.
// it maintains the data stream from lower layer's net/h2frame to higher layer's stream/userStream
type H2Controller struct {
	config    *h2Configure
	conn      net.Conn
	rawFramer *h2.Framer
	isServer  bool

	streamID int32
	//streamMap  map[uint32]stream
	streamMap  sync.Map
	mdMap      map[string]grpc.MethodDesc
	strMap     map[string]grpc.StreamDesc
	url        *dubboCommon.URL
	handler    common.ProtocolHeaderHandler
	pkgHandler common.PackageHandler
	service    dubboCommon.RPCService

	sendChan         chan interface{}
	tripleHeaderFlag bool
	// state shows conn state, after receiving go away frame, state will be set to draining
	// so as not to receive request from dubbo3 client, and client will new an other conn to replace
	state H2ControllerState
}

type sendChanDataPkg struct {
	data      []byte
	endStream bool
	streamID  uint32
}

type sendChanGoAwayPkg struct {
	maxSteamID uint32
	code       h2.ErrCode
	debugData  []byte
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

func getMethodAndStreamDescMap(service dubboCommon.RPCService) (map[string]grpc.MethodDesc, map[string]grpc.StreamDesc, error) {
	ds, ok := service.(Dubbo3GrpcService)
	if !ok {
		logger.Error("service is not Impl Dubbo3GrpcService")
		return nil, nil, perrors.New("service is not Impl Dubbo3GrpcService")
	}
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
func NewH2Controller(conn net.Conn, isServer bool, service dubboCommon.RPCService, url *dubboCommon.URL) (*H2Controller, error) {
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
	fm.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	var headerHandler common.ProtocolHeaderHandler
	var pkgHandler common.PackageHandler

	if url != nil {
		headerHandler, _ = common.GetProtocolHeaderHandler(url.Protocol)
		pkgHandler, _ = common.GetPackagerHandler(url.Protocol)
	}

	h2c := &H2Controller{
		config:           &defaultH2Configure,
		rawFramer:        fm,
		conn:             conn,
		url:              url,
		isServer:         isServer,
		streamMap:        sync.Map{},
		mdMap:            mdMap,
		strMap:           strMap,
		service:          service,
		handler:          headerHandler,
		pkgHandler:       pkgHandler,
		sendChan:         make(chan interface{}, 16),
		streamID:         int32(-1),
		tripleHeaderFlag: false,
		state:            reachable,
	}
	return h2c, nil
}

const (
	tripleFlagSettingID  = 8848
	tripleFlagSettingVal = 8848
)

func isTripleFlagSetting(setting h2.Setting) bool {
	return setting.ID == tripleFlagSettingID && setting.Val == tripleFlagSettingVal
}

// H2ShakeHand can send magic data and setting at the beginning of conn
// after check and send, it start listening from h2frame to streamMap
func (h *H2Controller) H2ShakeHand() error {
	// todo change to real setting
	// triple flag
	tripleFlagSetting := []h2.Setting{{
		ID:  tripleFlagSettingID,
		Val: tripleFlagSettingVal,
	}}
	// todo default setting
	settings := []h2.Setting{{
		ID:  0x5,
		Val: 16384,
	}}

	if h.isServer { // server
		// server end 1: write empty setting
		if err := h.rawFramer.WriteSettings(settings...); err != nil {
			logger.Error("server write setting frame error", err)
			return err
		}
		// server end 2：read magic
		// Check the validity of client preface.
		preface := make([]byte, len(h2.ClientPreface))
		if _, err := io.ReadFull(h.conn, preface); err != nil {
			if err == io.EOF {
				return err
			}
			logger.Error("server read preface err = ", err)
			return err
		}
		if !bytes.Equal(preface, []byte(h2.ClientPreface)) {
			logger.Error("server recv reface = ", string(preface), "not as expected")
			return perrors.Errorf("Preface Not Equal")
		}
		logger.Debug("server Preface check successful!")
		// server end 3：read empty setting
		frame, err := h.rawFramer.ReadFrame()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			logger.Error("server read firest setting err = ", err)
			return err
		}
		if err != nil {
			logger.Error("server read setting err = ", err)
			return err
		}
		setting, ok := frame.(*h2.SettingsFrame)
		if !ok {
			logger.Error("server read frame not setting frame type")
			return perrors.Errorf("server read frame not setting frame type")
		}
		if setting.NumSettings() != 0 {
			h.tripleHeaderFlag = isTripleFlagSetting(setting.Setting(0))
		}

		if err := h.rawFramer.WriteSettingsAck(); err != nil {
			logger.Error("server write setting Ack() err = ", err)
			return err
		}
	} else { // client
		// client end 1 write magic
		if _, err := h.conn.Write([]byte(h2.ClientPreface)); err != nil {
			logger.Errorf("client write preface err = ", err)
			return err
		}

		// server end 2：write first empty setting
		if err := h.rawFramer.WriteSettings(tripleFlagSetting...); err != nil {
			logger.Errorf("client write setting frame err = ", err)
			return err
		}

		// client end 3：read one setting
		frame, err := h.rawFramer.ReadFrame()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			logger.Error("client read setting err = ", err)
			return err
		}
		if err != nil {
			logger.Error("client read setting err = ", err)
			return err
		}
		_, ok := frame.(*h2.SettingsFrame)
		if !ok {
			logger.Error("client read frame not setting frame type")
			return perrors.Errorf("client read frame not setting frame type")
		}
	}
	// after shake hand, start send and receive listening
	go h.runSend()
	if h.isServer {
		go h.serverRunRecv()
	} else {
		go h.clientRunRecv()
	}
	return nil
}

func (h *H2Controller) getActiveMapLen() int {
	count := 0
	h.streamMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// runSendUnaryRsp start a rsp loop,  called when server response unary rpc
func (h *H2Controller) runSendUnaryRsp(stream *serverStream) {
	sendChan := stream.getSend()
	id := stream.getStreamID()
	handler, err := common.GetProtocolHeaderHandler(h.url.Protocol)
	if err != nil {
		logger.Errorf("Protocol Header Handler not registered: %s", h.url.Protocol)
		return
	}
	for {
		sendMsg := <-sendChan
		if sendMsg.GetMsgType() == ServerStreamCloseMsgType {
			var buf bytes.Buffer
			enc := hpack.NewEncoder(&buf)
			st := sendMsg.st
			headerFields := make([]hpack.HeaderField, 0) // at least :status, content-type will be there if none else.
			headerFields = append(headerFields, hpack.HeaderField{Name: ":status", Value: "200"})
			headerFields = append(headerFields, hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
			headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-status", Value: strconv.Itoa(int(st.Code()))})
			headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-message", Value: encodeGrpcMessage(st.Message())})
			for _, f := range headerFields {
				if err := enc.WriteField(f); err != nil {
					logger.Error("error: enc.WriteField err = ", err)
				}
			}
			hfData := buf.Next(buf.Len())
			h.sendChan <- h2.HeadersFrameParam{
				StreamID:      id,
				EndHeaders:    true,
				BlockFragment: hfData,
				EndStream:     true,
			}
			h.onEndStreamRecvOrSend(stream)
			return
		}

		sendData := sendMsg.buffer.Bytes()
		// header
		headerFields := make([]hpack.HeaderField, 0) // at least :status, content-type will be there if none else.
		headerFields = append(headerFields, hpack.HeaderField{Name: ":status", Value: "200"})
		headerFields = append(headerFields, hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
		if h.tripleHeaderFlag {
			headerFields = handler.WriteHeaderField(h.url, context.Background(), headerFields)
		}
		var buf bytes.Buffer
		enc := hpack.NewEncoder(&buf)
		for _, f := range headerFields {
			if err := enc.WriteField(f); err != nil {
				logger.Error("error: enc.WriteField err = ", err)
			}
		}
		hflen := buf.Len()
		hfData := buf.Next(hflen)

		logger.Debug("server send stream id = ", id)

		h.sendChan <- h2.HeadersFrameParam{
			StreamID:      id,
			EndHeaders:    true,
			BlockFragment: hfData,
			EndStream:     false,
		}
		h.sendChan <- sendChanDataPkg{
			streamID:  id,
			endStream: false,
			data:      sendData,
		}

		var buf2 bytes.Buffer
		// end unary rpc with status
		enc = hpack.NewEncoder(&buf2)
		headerFields = make([]hpack.HeaderField, 0, 2) // at least :status, content-type will be there if none else.
		headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-status", Value: "0"})
		headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-message", Value: ""})
		for _, f := range headerFields {
			if err := enc.WriteField(f); err != nil {
				logger.Error("error: enc.WriteField err = ", err)
			}
		}
		hfData2 := buf2.Next(buf2.Len())
		h.sendChan <- h2.HeadersFrameParam{
			StreamID:      id,
			EndHeaders:    true,
			BlockFragment: hfData2,
			EndStream:     true,
		}
		h.onEndStreamRecvOrSend(stream)
		break
	}
}

// runSendStreamRsp start a rsp loop,  called when server response stream rpc
func (h *H2Controller) runSendStreamRsp(stream *serverStream) {
	sendChan := stream.getSend()
	headerWrited := false
	id := stream.getStreamID()
	handler, err := common.GetProtocolHeaderHandler(h.url.Protocol)
	if err != nil {
		logger.Errorf("Protocol Header Handler not registered: %s", h.url.Protocol)
		return
	}
	for {
		sendMsg := <-sendChan
		if sendMsg.GetMsgType() == ServerStreamCloseMsgType { // end stream rpc with status
			var buf bytes.Buffer
			enc := hpack.NewEncoder(&buf)
			st := sendMsg.st
			headerFields := make([]hpack.HeaderField, 0, 2) // at least :status, content-type will be there if none else.
			if !headerWrited {
				headerFields = append(headerFields, hpack.HeaderField{Name: ":status", Value: "200"})
				headerFields = append(headerFields, hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
			}
			headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-status", Value: strconv.Itoa(int(st.Code()))})
			headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-message", Value: encodeGrpcMessage(st.Message())})
			for _, f := range headerFields {
				if err := enc.WriteField(f); err != nil {
					logger.Error("error: enc.WriteField err = ", err)
				}
			}
			hfData := buf.Next(buf.Len())
			h.sendChan <- h2.HeadersFrameParam{
				StreamID:      id,
				EndHeaders:    true,
				BlockFragment: hfData,
				EndStream:     true,
			}
			h.handleResetStream(id)
			h.sendChan <- h2.RSTStreamFrame{
				FrameHeader: h2.FrameHeader{
					StreamID: id,
				},
				ErrCode: h2.ErrCodeNo,
			}
			return
		}
		sendData := sendMsg.buffer.Bytes()
		if !headerWrited {
			// header
			headerFields := make([]hpack.HeaderField, 0, 2) // at least :status, content-type will be there if none else.
			headerFields = append(headerFields, hpack.HeaderField{Name: ":status", Value: "200"})
			headerFields = append(headerFields, hpack.HeaderField{Name: "content-type", Value: "application/grpc"})
			if h.tripleHeaderFlag {
				headerFields = handler.WriteHeaderField(&dubboCommon.URL{}, context.Background(), headerFields)
			}

			var buf bytes.Buffer
			enc := hpack.NewEncoder(&buf)
			for _, f := range headerFields {
				if err := enc.WriteField(f); err != nil {
					logger.Error("error: enc.WriteField err = ", err)
				}
			}
			hflen := buf.Len()
			hfData := buf.Next(hflen)

			logger.Debug("server send stream id = ", id)

			// todo need serilization
			h.sendChan <- h2.HeadersFrameParam{
				StreamID:      id,
				EndHeaders:    true,
				BlockFragment: hfData,
				EndStream:     false,
			}
			headerWrited = true
		}

		h.sendChan <- sendChanDataPkg{
			streamID:  id,
			endStream: false,
			data:      sendData,
		}
	}
}

func (h *H2Controller) handleDataFrame(fm *h2.DataFrame) {
	id := fm.StreamID
	logger.Debug("DataFrame = ", fm.String(), "id = ", id)
	data := make([]byte, len(fm.Data()))
	copy(data, fm.Data())
	val, ok := h.streamMap.Load(id)
	if !ok {
		return
	}
	val.(stream).putRecv(data, DataMsgType)
	if fm.Flags.Has(h2.FlagDataEndStream) {
		h.onEndStreamRecvOrSend(val.(stream))
	}
}

func (h *H2Controller) clientRunRecv() {
	for {
		fm, err := h.rawFramer.ReadFrame()
		if err != nil {
			if err.Error() != "EOF" {
				logger.Info("read frame error = ", err)
			}
			continue
		}
		switch fm := fm.(type) {
		case *h2.MetaHeadersFrame:
			h.ifStreamWillStillAndOperate(fm)
		case *h2.DataFrame:
			h.handleDataFrame(fm)
		case *h2.RSTStreamFrame:
			h.handleResetStream(fm.StreamID)
		case *h2.SettingsFrame:
		case *h2.PingFrame:
			h.handlePingFrame(fm)
		case *h2.WindowUpdateFrame:
		case *h2.GoAwayFrame:
			logger.Debug("GoAwayFrame recv!")
			h.handleGoAway(fm)
		default:
		}
	}
}

// handlePingFrame handle ping frame
func (h *H2Controller) handlePingFrame(fm *h2.PingFrame) {
	if fm.Flags.Has(h2.FlagPingAck) {
		logger.Debug("handle ping frame ACK")
		return
	}
	logger.Debug("handle ping frame")
	h.sendChan <- h2.PingFrame{
		FrameHeader: h2.FrameHeader{
			Flags: h2.FlagPingAck,
		},
	}
}

// handleResetStream close stream directly
func (h *H2Controller) handleResetStream(streamID uint32) {
	logger.Debug("handleResetStream with id = ", streamID)
	if s, ok := h.streamMap.Load(streamID); ok {
		s.(stream).close()
		s.(stream).setState(closed)
		h.streamMap.Delete(streamID)
	}
}

// handleGoAway receive goaway fm
// go away is a conn level frame, which would close all unhandle stream and set conn state draining
func (h *H2Controller) handleGoAway(fm *h2.GoAwayFrame) {
	id := fm.LastStreamID
	logger.Debug("goaway : lastStreamID = ", id)
	if id > 0 && id%2 != 1 {
		h.close()
		return
	}
	streamIDToClose := make([]uint32, 0)
	h.streamMap.Range(func(key, value interface{}) bool {
		if key.(uint32) > id {
			value.(stream).close()
			streamIDToClose = append(streamIDToClose, key.(uint32))
		}
		return true
	})
	for _, v := range streamIDToClose {
		h.streamMap.Delete(v)
	}
	h.state = draining
}

func (h *H2Controller) onEndStreamRecvOrSend(s stream) {
	if tempStreamState := s.onRecvEs(); tempStreamState == closed {
		// stream after receive ES flag, change to close means it is really close
		h.streamMap.Delete(s.getStreamID())
	}
}

// ifStreamWillStillAndOperate read status of grpc and close stream if need
// ifStreamWillStillAndOperate deal with end stream flag in H2 header
// if stream would close, return false
func (h *H2Controller) ifStreamWillStillAndOperate(fm *h2.MetaHeadersFrame) bool {
	id := fm.StreamID
	val, ok := h.streamMap.Load(id)
	if !ok {
		logger.Debug("operateHeaders not get stream in h2 controller")
		return true
	}

	s := val.(stream)
	state := &decodeState{}
	state.data.isGRPC = true

	for _, hf := range fm.Fields {
		state.processHeaderField(hf)
	}
	if state.data.rawStatusCode != nil && *state.data.rawStatusCode != 0 {
		s.putRecvErr(state.status().Err())
		return false
	}

	if fm.Flags.Has(h2.FlagDataEndStream) {
		h.onEndStreamRecvOrSend(s)
		return false
	}
	return true
}

//func (h*H2Controller) if

// serverRun start a loop, server start listening h2 metaheader
func (h *H2Controller) serverRunRecv() {
	for {
		fm, err := h.rawFramer.ReadFrame()
		if err != nil {
			if err != io.EOF {
				logger.Info("read frame error = ", err)
			}
			continue
		}
		switch fm := fm.(type) {
		case *h2.MetaHeadersFrame:
			if ok := h.ifStreamWillStillAndOperate(fm); !ok { // stream would be close
				continue
			}
			logger.Debug("MetaHeader frame = ", fm.String(), "id = ", fm.StreamID)
			header := h.handler.ReadFromH2MetaHeader(fm)
			if err := h.addServerStream(header); err != nil {
				logger.Errorf("add server stream err %+v", err)
			}
		case *h2.DataFrame:
			h.handleDataFrame(fm)
		case *h2.RSTStreamFrame:
			h.handleResetStream(fm.StreamID)
		case *h2.SettingsFrame:
		case *h2.PingFrame:
			h.handlePingFrame(fm)
		case *h2.WindowUpdateFrame:
		case *h2.GoAwayFrame:
			// TODO: Handle GoAway from the client appropriately.
			// now it's useless for client to send go away frame to server
		//case *h2.:
		default:
		}
	}
}

// addServerStream can create a serverStream and add to h2Controller by @data read from frame,
// after receiving a request from client.
func (h *H2Controller) addServerStream(data common.ProtocolHeader) error {
	methodName := strings.Split(data.GetMethod(), "/")[2]
	md, okm := h.mdMap[methodName]
	streamd, oks := h.strMap[methodName]
	if !okm && !oks {
		logger.Errorf("method name %s not found in desc", methodName)
		return perrors.New(fmt.Sprintf("method name %s not found in desc", methodName))
	}
	var newstm *serverStream
	var err error
	if okm {
		newstm, err = newServerStream(data, md, h.url, h.service)
		if err != nil {
			return err
		}
		go h.runSendUnaryRsp(newstm)
	} else {
		newstm, err = newServerStream(data, streamd, h.url, h.service)
		if err != nil {
			return err
		}
		go h.runSendStreamRsp(newstm)
	}
	h.streamMap.Store(newstm.ID, newstm)
	if h.getActiveMapLen() >= int(h.config.MaxActiveStream) {
		h.state = draining
		h.sendChan <- sendChanGoAwayPkg{
			maxSteamID: uint32(h.streamID),
			code:       h2.ErrCodeNo,
			debugData:  make([]byte, 0),
		}
		return nil
	}
	return nil
}

// runSend called after shakehand, start sending data from chan to h2frame
func (h *H2Controller) runSend() {
	for {
		toSend := <-h.sendChan
		switch toSend := toSend.(type) {
		case h2.HeadersFrameParam:
			if err := h.rawFramer.WriteHeaders(toSend); err != nil {
				logger.Errorf("rawFramer writeHeaders err = ", err)
			}
		case sendChanDataPkg:
			if err := h.rawFramer.WriteData(toSend.streamID, toSend.endStream, toSend.data); err != nil {
				logger.Errorf("rawFramer writeData err = ", err)
			}
		case sendChanGoAwayPkg:
			if err := h.rawFramer.WriteGoAway(toSend.maxSteamID, toSend.code, toSend.debugData); err != nil {
				logger.Errorf("rawFramer writeGoAway err = ", err)
			}
		case h2.RSTStreamFrame:
			if err := h.rawFramer.WriteRSTStream(toSend.StreamID, toSend.ErrCode); err != nil {
				logger.Errorf("rawFramer WriteRSTStream err = ", err)
			}
		case h2.PingFrame:
			if err := h.rawFramer.WritePing(toSend.IsAck(), toSend.Data); err != nil {
				logger.Errorf("rawFramer WriteRSTStream err = ", err)
			}

		default:
		}
	}
}

// StreamInvoke can start streaming invocation, called by triple client
func (h *H2Controller) StreamInvoke(ctx context.Context, method string) (grpc.ClientStream, error) {
	// metadata header
	handler, err := common.GetProtocolHeaderHandler(h.url.Protocol)
	if err != nil {
		return nil, err
	}
	h.url.SetParam(":path", method)
	headerFields := handler.WriteHeaderField(h.url, ctx, nil)
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	for _, f := range headerFields {
		if err := enc.WriteField(f); err != nil {
			logger.Error("error: enc.WriteField err = ", err)
		} else {
			//logger.Debug("encode field f = ", f.Name, " ", f.Value, " success")
		}
	}
	hflen := buf.Len()
	hfData := buf.Next(hflen)

	//todo rpcID change
	id := uint32(atomic.AddInt32(&h.streamID, 2))
	h.sendChan <- h2.HeadersFrameParam{
		StreamID:      id,
		EndHeaders:    true,
		BlockFragment: hfData,
		EndStream:     false,
	}
	clientStream := newClientStream(id)
	serilizer, err := common.GetDubbo3Serializer(codec.DefaultDubbo3SerializerName)
	if err != nil {
		logger.Error("get serilizer error = ", err)
		return nil, err
	}
	h.streamMap.Store(clientStream.ID, clientStream)
	go clientStream.runSendDataToServerStream(h.sendChan)
	pkgHandler, err := common.GetPackagerHandler(h.url.Protocol)
	return newClientUserStream(clientStream, serilizer, pkgHandler), nil
}

// UnaryInvoke can start unary invocation, called by dubbo3 client
func (h *H2Controller) UnaryInvoke(ctx context.Context, method string, addr string, data []byte, reply interface{}, url *dubboCommon.URL) error {
	// 1. set client stream
	id := uint32(atomic.AddInt32(&h.streamID, 2))
	clientStream := newClientStream(id)
	h.streamMap.Store(clientStream.ID, clientStream)

	// 2. send req messages

	// metadata header
	handler, err := common.GetProtocolHeaderHandler(url.Protocol)
	if err != nil {
		logger.Error("can't find handler with protocol:", url.Protocol)
		return err
	}
	// method name from grpc stub, set to :path field
	url.SetParam(":path", method)
	headerFields := handler.WriteHeaderField(url, ctx, nil)
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	for _, f := range headerFields {
		if err := enc.WriteField(f); err != nil {
			logger.Error("error: enc.WriteField err = ", err)
		} else {
			//logger.Debug("encode field f = ", f.Name, " ", f.Value, " success")
		}
	}
	hflen := buf.Len()
	hfData := buf.Next(hflen)

	// header send
	h.sendChan <- h2.HeadersFrameParam{
		StreamID:      id,
		EndHeaders:    true,
		BlockFragment: hfData,
		EndStream:     false,
	}

	// data send
	h.sendChan <- sendChanDataPkg{
		streamID:  id,
		endStream: true,
		data:      h.pkgHandler.Pkg2FrameData(data),
	}
	h.onEndStreamRecvOrSend(clientStream)

	// 3. recv rsp
	recvChan := clientStream.getRecv()
	recvData := <-recvChan
	// todo add timeout
	if recvData.GetMsgType() == ServerStreamCloseMsgType {
		return toRPCErr(recvData.err)
	}

	// recv chan was closed
	if recvData.buffer == nil {
		return status.Err(codes.Canceled, "conn reset by peers")
	}

	if err := proto.Unmarshal(h.pkgHandler.Frame2PkgData(recvData.buffer.Bytes()), reply.(proto.Message)); err != nil {
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
	h2.state = closing
}
