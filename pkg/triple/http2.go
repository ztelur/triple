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
	"github.com/dubbogo/triple/internal/codes"
	"github.com/dubbogo/triple/internal/status"
	"github.com/dubbogo/triple/pkg/triple/flowControl"
	"github.com/dubbogo/triple/pkg/triple/frameSize"
	"go.uber.org/atomic"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
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

// H2Controller is an important object of h2 protocol
// it can be used as serverEnd and clientEnd, identified by isServer field
// it can shake hand by client or server end, to start triple transfer
// higher layer can call H2Controller's StreamInvoke or UnaryInvoke method to deal with event
// it maintains streamMap, with can contain have many higher layer object: stream, used by event driven.
// it maintains the data stream from lower layer's net/h2frame to higher layer's stream/userStream
type H2Controller struct {
	// conn is used to init h2 framer
	conn net.Conn

	// rawFramer is h2 frame handler of golang standard repository
	rawFramer *h2.Framer

	// isServer is to judge whether this Controller is used by,
	// I just want to reuse more code, so I define only H2Controller struct, not the way as grpc does, that split c/s
	isServer bool

	// streamID store temp max stream id, which is init by MAX_UINT32 and increased by 2 each time
	streamID uint32
	// streamIDLock is used to lock increase of streamID and send Header, to ensure the stream id is increasing
	// it's HTTP2 standard
	streamIDLock sync.Mutex

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

	// sendChan is used to sendout any frame from this H2controller
	// the receiver of sendChan: func runSend() will never be blocked
	sendChan chan interface{}

	// tripleHeaderFlag is used to identify if client is triple client or grpc client
	// not it is useless, but may be useful in the futurego run
	tripleHeaderFlag bool

	// state shows conn state, after receiving go away frame, state will be set to draining
	// so as not to receive request from dubbo3 client, and client will new an other conn to replace
	state common.H2ControllerState

	// maxCurrentStream stores the max number of concurrent stream
	// if reach this number, h2 will let incoming stream block, until aother stream is closed
	maxCurrentStream atomic.Uint32

	// blockStreamChan store the ok chan for every blocked stream, when exceed maxCurrentStream
	blockStreamChan []chan struct{}

	blockChanListMutex sync.Mutex

	// flowController is h2 flow controller
	flowController *flowControl.H2FlowController

	// frameSizeController frameSizeController is max data size controller and spliter
	frameSizeController *frameSize.H2MaxFrameController
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

	sendChan := make(chan interface{}, 16)

	h2c := &H2Controller{
		rawFramer:           fm,
		conn:                conn,
		url:                 url,
		isServer:            isServer,
		streamMap:           sync.Map{},
		mdMap:               mdMap,
		strMap:              strMap,
		service:             service,
		handler:             headerHandler,
		pkgHandler:          pkgHandler,
		sendChan:            sendChan,
		streamID:            uint32(math.MaxUint32),
		tripleHeaderFlag:    false,
		state:               common.Reachable,
		maxCurrentStream:    defaultMaxCurrentStream,
		blockStreamChan:     make([]chan struct{}, 0),
		frameSizeController: frameSize.NewH2MaxFrameController(),
		streamIDLock:        sync.Mutex{},
		flowController:      flowControl.NewH2FlowController(sendChan),
	}
	return h2c, nil
}

// these 2 ID is used to identify triple client, it's meaningless of h2's setting
const (
	tripleFlagSettingID  = 8848
	tripleFlagSettingVal = 8848
)

// isTripleFlagSetting is used to identy if the first setting from client match triple ids above
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
	// default setting
	settings := []h2.Setting{{
		ID:  h2.SettingInitialWindowSize,
		Val: common.DefaultConnInitWindowSize,
	}}

	if h.isServer { // server
		// flowContorl
		settings = append(settings,
			[]h2.Setting{{
				ID:  h2.SettingMaxFrameSize,
				Val: common.DefaultMaxFrameSize,
			}, {
				ID:  h2.SettingMaxConcurrentStreams,
				Val: common.DefaultMaxConcurrentStreams,
			}}...)

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
			h.handleSettingFrame(setting)
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
		tripleFlagSetting = append(tripleFlagSetting, settings...)
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
		settingFrames, ok := frame.(*h2.SettingsFrame)
		if !ok {
			logger.Error("client read frame not setting frame type")
			return perrors.Errorf("client read frame not setting frame type")
		}
		if settingFrames.NumSettings() > 0 {
			h.handleSettingFrame(settingFrames)
		}
	}
	// after shake hand, start send and receive listening
	go h.runSend()

	// start flowControl sending to prepare to send data frame
	go h.flowController.RunFlowControlSending()

	// start receiving frame
	if h.isServer {
		go h.serverRunRecv()
	} else {
		go h.clientRunRecv()
	}
	return nil
}

// getActiveMapLen used in concurrentControl, it can get active streams number
func (h *H2Controller) getActiveMapLen() uint32 {
	count := uint32(0)
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
	// handler, err := common.GetProtocolHeaderHandler(h.url.Protocol)
	//if err != nil {
	//	logger.Errorf("Protocol Header Handler not registered: %s", h.url.Protocol)
	//	return
	//}
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
		// use data frame spliter and flow control to send DataFrame
		cb := h.frameSizeController.SendGeneralDataFrame(id, false, sendData, h.flowController.PushToFlowControlMap)
		// until the data is really sent
		<-cb

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
	//handler, err := common.GetProtocolHeaderHandler(h.url.Protocol)
	//if err != nil {
	//	logger.Errorf("Protocol Header Handler not registered: %s", h.url.Protocol)
	//	return
	//}
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
			h.resetStream(id, h2.ErrCodeNo)
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
		// stream rpc is useless to use call back to ensure data is send?
		ch := h.frameSizeController.SendGeneralDataFrame(id, false, sendData, h.flowController.PushToFlowControlMap)
		// until the data is really sent
		<-ch
	}
}

// handleWindowUpdateHandler handle when get window update frame
func (h *H2Controller) handleWindowUpdateHandler(fm *h2.WindowUpdateFrame) {
	// now triple only support conn level flow control
	if fm.StreamID == 0 {
		h.flowController.AddSendQuota(fm.Increment)
		return
	}
}

// handleDataFrame handle when get data frame
func (h *H2Controller) handleDataFrame(fm *h2.DataFrame) {
	id := fm.StreamID
	logger.Debug("DataFrame = ", fm.String(), "id = ", id)
	if id == 0 {
		logger.Error("ID == 0, send goaway with errorcode = Protocol")
		h.sendGoAwayWithErrorCode(h2.ErrCodeProtocol)
		return
	}
	if w := h.flowController.OnData(fm.Header().Length); w > 0 {
		h.sendChan <- common.SendWindowsUpdataPkg{
			StreamID:  0,
			Increment: w,
		}
		h.sendChan <- common.SendWindowsUpdataPkg{
			StreamID:  id,
			Increment: w,
		}
	}
	data := make([]byte, len(fm.Data()))
	copy(data, fm.Data())
	val, ok := h.streamMap.Load(id)
	if !ok {
		logger.Error("streamID = ", id, " not exist, send goaway with errorcode = StreamClosed")
		h.sendGoAwayWithErrorCode(h2.ErrCodeStreamClosed)
		return
	}
	val.(stream).putSplitedDataRecv(data, DataMsgType, h.pkgHandler)
	if fm.Flags.Has(h2.FlagDataEndStream) {
		h.onEndStreamRecvOrSend(val.(stream))
	}
}

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

func (h *H2Controller) sendGoAwayWithErrorCode(errCode h2.ErrCode) {
	h.sendChan <- common.SendChanGoAwayPkg{
		MaxSteamID: h.maxCurrentStream.Load(),
		Code:       errCode,
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
			h.handleResetStreamFrame(fm)
		case *h2.SettingsFrame:
			h.handleSettingFrame(fm)
		case *h2.PingFrame:
			h.handlePingFrame(fm)
		case *h2.WindowUpdateFrame:
			h.handleWindowUpdateHandler(fm)
		case *h2.GoAwayFrame:
			h.handleGoAway(fm)
		default:
		}
	}
}

// handlePingFrame handle ping frame

// handleResetStream close stream directly
func (h *H2Controller) handleResetStreamFrame(fm *h2.RSTStreamFrame) {
	h.resetStream(fm.StreamID, fm.ErrCode)
}

func (h *H2Controller) refreshBlockingStream() {
	h.blockChanListMutex.Lock()
	defer h.blockChanListMutex.Unlock()
	blockChanLen := len(h.blockStreamChan)
	if blockChanLen > 0 { // need refresh
		canBeReleaseNum := h.maxCurrentStream.Load() - h.getActiveMapLen()
		if canBeReleaseNum > 0 { // can release some chan
			refreshNum := 0
			if blockChanLen <= int(canBeReleaseNum) {
				// release all
				for _, v := range h.blockStreamChan {
					v <- struct{}{}
					refreshNum++
				}
			} else {
				//blockChanLen > can release num
				for k, v := range h.blockStreamChan {
					if k < int(canBeReleaseNum) {
						v <- struct{}{}
						refreshNum++
					} else {
						break
					}
				}
			}
			h.blockStreamChan = h.blockStreamChan[refreshNum:]
		}
	}
}

func (h *H2Controller) resetStream(id uint32, err h2.ErrCode) {
	logger.Debug("handleResetStream with id = ", id)
	// todo reset not too fast
	go func() {
		// todo: reset with using buffer, which will cause panic
		// we should reset it after all data sended
		time.Sleep(time.Second)
		if s, ok := h.streamMap.Load(id); ok {
			s.(stream).closeWithError(status.Err(codes.Internal, fmt.Sprintf("stream terminated by RST_STREAM with error code: %v", err)))
			s.(stream).setState(closed)
			h.streamMap.Delete(id)
			h.refreshBlockingStream()
		}
	}()

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
	h.refreshBlockingStream()
	h.state = common.Draining
}

// is called when send or recv EndStream flag, whether header frame or data frame
// this function can change status of stream, and automatic delete\close\refreshif necessary
func (h *H2Controller) onEndStreamRecvOrSend(s stream) {
	if tempStreamState := s.onRecvEs(); tempStreamState == closed {
		// stream after receive ES flag, change to close means it is really close
		h.streamMap.Delete(s.getStreamID())
		s.close()
		h.refreshBlockingStream()
	}
}

// ifStreamWillStillAndOperate read status of grpc and close stream if need
// ifStreamWillStillAndOperate deal with end stream flag in H2 header
// if stream would close, return false
func (h *H2Controller) ifStreamWillStillAndOperate(fm *h2.MetaHeadersFrame) bool {
	id := fm.StreamID
	val, ok := h.streamMap.Load(id)
	if !ok {
		logger.Debug("operateHeaders not get stream in h2 controller with id = ", id)
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
			h.addServerStream(header)
		case *h2.DataFrame:
			h.handleDataFrame(fm)
		case *h2.RSTStreamFrame:
			h.handleResetStreamFrame(fm)
		case *h2.SettingsFrame:
			h.handleSettingFrame(fm)
		case *h2.PingFrame:
			h.handlePingFrame(fm)
		case *h2.WindowUpdateFrame:
			h.handleWindowUpdateHandler(fm)
		case *h2.GoAwayFrame:
			// TODO: Handle GoAway from the client appropriately.
			// now it's useless for client to send go away frame to server
		//case *h2.:
		default:
		}
	}
}

func (h *H2Controller) handleSettingFrame(fm *h2.SettingsFrame) {
	maxStreams := new(uint32)
	if err := fm.ForeachSetting(func(s h2.Setting) error {
		switch s.ID {
		case h2.SettingMaxConcurrentStreams:
			*maxStreams = s.Val
			logger.Debug("set max concurrent stream = ", *maxStreams)
			if *maxStreams > h.maxCurrentStream.Load() {
				// can release blocking stream
				h.refreshBlockingStream()
			}
			h.maxCurrentStream.Store(*maxStreams)
			h.sendChan <- common.SendSettigACK{}
		case h2.SettingMaxHeaderListSize:
			h.sendChan <- common.SendSettigACK{}
		case h2.SettingMaxFrameSize:
			h.sendChan <- common.SendSettigACK{}
			h.frameSizeController.SetMaxFrameSize(s.Val)
			h.rawFramer.SetMaxReadFrameSize(s.Val)
		default:
			//ss = append(ss, s)
		}
		return nil
	}); err != nil {
		logger.Error("handle setting frame error!", err)
		return
	}

}

// addServerStream can create a serverStream and add to h2Controller by @data read from frame,
// after receiving a request from client.
func (h *H2Controller) addServerStream(data common.ProtocolHeader) {
	// block if necessary
	h.blockIfNecessory()

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
		go h.runSendUnaryRsp(newstm)
	} else {
		newstm, err = newServerStream(data, streamd, h.url, h.service)
		if err != nil {
			logger.Error("newServerStream error", err)
			return
		}
		go h.runSendStreamRsp(newstm)
	}
	h.streamMap.Store(newstm.ID, newstm)
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
		case common.SendChanDataPkg:
			if err := h.rawFramer.WriteData(toSend.StreamID, toSend.EndStream, toSend.Data); err != nil {
				logger.Errorf("rawFramer writeData err = ", err)
			}
			// call back signal means all data have sent
			if toSend.Cb != nil {
				go func() {
					toSend.Cb <- struct{}{}
				}()
			}

		case *common.SendChanGoAwayPkg:
			if err := h.rawFramer.WriteGoAway(toSend.MaxSteamID, toSend.Code, toSend.DebugData); err != nil {
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
		case common.SendWindowsUpdataPkg:
			if err := h.rawFramer.WriteWindowUpdate(toSend.StreamID, toSend.Increment); err != nil {
				logger.Errorf("rawFramer WriteWindowUpdate err = ", err)
			}
		case common.SendSettigACK:
			if err := h.rawFramer.WriteSettingsAck(); err != nil {
				logger.Errorf("rawFramer WriteSettingsAck err = ", err)
			}
		default:
		}
	}
}

func (h *H2Controller) blockIfNecessory() {
	if h.getActiveMapLen() >= h.maxCurrentStream.Load() {
		// meet max current Stream number, block!
		logger.Debug("MAX!!!BLOCK")
		blockChan := make(chan struct{})
		h.blockChanListMutex.Lock()
		h.blockStreamChan = append(h.blockStreamChan, blockChan)
		h.blockChanListMutex.Unlock()
		logger.Debug("BLOCKING!!!!!!!!")
		<-blockChan
		logger.Debug("RELEASE!!!!!!!")
	}
}

// StreamInvoke can start streaming invocation, called by triple client
func (h *H2Controller) StreamInvoke(ctx context.Context, method string) (grpc.ClientStream, error) {
	// check if block
	h.blockIfNecessory()
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
	// 1. get stream ID
	h.streamIDLock.Lock()
	h.streamID += 2
	id := h.streamID
	// 2. create stream
	clientStream := newClientStream(id)
	serilizer, err := common.GetDubbo3Serializer(codec.DefaultDubbo3SerializerName)
	if err != nil {
		logger.Error("get serilizer error = ", err)
		h.streamIDLock.Unlock()
		return nil, err
	}
	h.streamMap.Store(clientStream.ID, clientStream)
	// 3. start send data
	go clientStream.runSendDataToServerStream(h.flowController.PushToFlowControlMap, h.frameSizeController.SendGeneralDataFrame)
	h.sendChan <- h2.HeadersFrameParam{
		StreamID:      id,
		EndHeaders:    true,
		BlockFragment: hfData,
		EndStream:     false,
	}
	h.streamIDLock.Unlock()
	// 4. start receive data to send
	pkgHandler, err := common.GetPackagerHandler(h.url.Protocol)
	return newClientUserStream(clientStream, serilizer, pkgHandler), nil
}

// UnaryInvoke can start unary invocation, called by dubbo3 client
func (h *H2Controller) UnaryInvoke(ctx context.Context, method string, addr string, data []byte, reply interface{}, url *dubboCommon.URL) error {
	// check if block
	h.blockIfNecessory()

	// 1. set client stream

	h.streamIDLock.Lock()
	h.streamID += 2
	id := h.streamID

	clientStream := newClientStream(id)
	h.streamMap.Store(clientStream.ID, clientStream)

	// 2. send req messages
	// metadata header
	handler, err := common.GetProtocolHeaderHandler(url.Protocol)
	if err != nil {
		logger.Error("can't find handler with protocol:", url.Protocol)
		h.streamIDLock.Unlock()
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
	h.streamIDLock.Unlock()

	// data send
	cb := h.frameSizeController.SendGeneralDataFrame(id, true, h.pkgHandler.Pkg2FrameData(data), h.flowController.PushToFlowControlMap)
	<-cb
	// half close stream
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

	rawData, _ := h.pkgHandler.Frame2PkgData(recvData.buffer.Bytes())
	if err := proto.Unmarshal(rawData, reply.(proto.Message)); err != nil {
		logger.Error("client unmarshal rsp err:", err)
		return err
	}

	// 4. recv endstream header
	// if close successfully, it would read from closed chan, and got noError
	// if endStream header is with error, like RST frame, this would send Error to invoker
	recvData = <-recvChan
	if recvData.GetMsgType() == ServerStreamCloseMsgType {
		return toRPCErr(recvData.err)
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
