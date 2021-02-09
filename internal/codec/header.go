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

package codec

import (
	"context"
	h2 "golang.org/x/net/http2"
	"net/http"
	"net/textproto"
	"strconv"
)

import (
	dubboCommon "github.com/apache/dubbo-go/common"
)

import (
	"github.com/dubbogo/triple/pkg/common"
)

func init() {
	// if user choose dubbo3 as url.Protocol, triple Handler will use it to handle header
	common.SetProtocolHeaderHandler(DUBBO3, NewTripleHeaderHandler)
}

// TripleHeader define the h2 header of triple impl
type TripleHeader struct {
	Path           string
	StreamID       uint32
	ContentType    string
	ServiceVersion string
	ServiceGroup   string
	RPCID          string
	TracingID      string
	TracingRPCID   string
	TracingContext string
	ClusterInfo    string
	GrpcStatus     string
	GrpcMessage    string
	Authorization  []string
}

func (t *TripleHeader) GetPath() string {
	return t.Path
}
func (t *TripleHeader) GetStreamID() uint32 {
	return t.StreamID
}

// FieldToCtx parse triple Header that user defined, to ctx of server end
func (t *TripleHeader) FieldToCtx() context.Context {
	ctx := context.WithValue(context.Background(), "tri-service-version", t.ServiceVersion)
	ctx = context.WithValue(ctx, "tri-service-group", t.ServiceGroup)
	ctx = context.WithValue(ctx, "tri-req-id", t.RPCID)
	ctx = context.WithValue(ctx, "tri-trace-traceid", t.TracingID)
	ctx = context.WithValue(ctx, "tri-trace-rpcid", t.TracingRPCID)
	ctx = context.WithValue(ctx, "tri-trace-proto-bin", t.TracingContext)
	ctx = context.WithValue(ctx, "tri-unit-info", t.ClusterInfo)
	ctx = context.WithValue(ctx, "grpc-status", t.GrpcStatus)
	ctx = context.WithValue(ctx, "grpc-message", t.GrpcMessage)
	ctx = context.WithValue(ctx, "authorization", t.Authorization)
	return ctx
}

func NewTripleHeaderHandler(url *dubboCommon.URL, ctx context.Context) common.ProtocolHeaderHandler {
	return &TripleHeaderHandler{
		Url: url,
		Ctx: ctx,
	}
}

// TripleHeaderHandler Handler the change of triple header field and h2 field
type TripleHeaderHandler struct {
	Url *dubboCommon.URL
	Ctx context.Context
}

// WriteHeaderField called before comsumer call remote serve,
// it parse field of url and ctx to HTTP2 Header field, developer must assure "tri-" prefix field be string
// if not, it will cause panic!
func (t *TripleHeaderHandler) WriteTripleReqHeaderField(header http.Header) http.Header {
	//header[":method"] = []string{"POST"}
	//header[":scheme"] = []string{"https"}
	//header[":path"] = []string{t.Url.GetParam(":path", "")} //
	//header[":authority"] = []string{t.Url.Location}
	//header["content-type"] = []string{t.Url.GetParam("content-type", "application/grpc")}
	header["user-agent"] = []string{"grpc-go/1.35.0-dev"}
	header["tri-service-version"] = []string{getCtxVaSave(t.Ctx, "tri-service-version")}
	header["tri-service-group"] = []string{getCtxVaSave(t.Ctx, "tri-service-group")}
	header["tri-req-id"] = []string{getCtxVaSave(t.Ctx, "tri-req-id")}
	header["tri-trace-traceid"] = []string{getCtxVaSave(t.Ctx, "tri-trace-traceid")}
	header["tri-trace-rpcid"] = []string{getCtxVaSave(t.Ctx, "tri-trace-rpcid")}
	header["tri-trace-proto-bin"] = []string{getCtxVaSave(t.Ctx, "tri-trace-proto-bin")}
	header["tri-unit-info"] = []string{getCtxVaSave(t.Ctx, "tri-unit-info")}
	if v, ok := t.Ctx.Value("authorization").([]string); !ok || len(v) != 2 {
		return header
	} else {
		header["authorization"] = v
	}

	return header
}

func (t *TripleHeaderHandler) WriteTripleFinalRspHeaderField(w http.ResponseWriter) {
	w.Header().Add("content-type", "application/grpc+proto")
	w.Header().Add(h2.TrailerPrefix+"grpc-status", strconv.Itoa(int(0)))     // sendMsg.st.Code()
	w.Header().Add(h2.TrailerPrefix+"grpc-message", "")                      //encodeGrpcMessage(""))
	w.Header().Add(h2.TrailerPrefix+"trace-proto-bin", strconv.Itoa(int(0))) // sendMsg.st.Code()
}

func getCtxVaSave(ctx context.Context, field string) string {
	val, ok := ctx.Value(field).(string)
	if ok {
		return val
	}
	return ""
}

// ReadFromH2MetaHeader read meta header field from h2 header, and parse it to ProtocolHeader as user defined
func (t *TripleHeaderHandler) ReadFromTripleReqHeader(r *http.Request) common.ProtocolHeader {
	tripleHeader := &TripleHeader{}
	header := r.Header
	tripleHeader.Path = r.URL.Path
	for k, v := range header {
		switch k {
		case textproto.CanonicalMIMEHeaderKey("tri-service-version"):
			tripleHeader.ServiceVersion = v[0]
		case textproto.CanonicalMIMEHeaderKey("tri-service-group"):
			tripleHeader.ServiceGroup = v[0]
		case textproto.CanonicalMIMEHeaderKey("tri-req-id"):
			tripleHeader.RPCID = v[0]
		case textproto.CanonicalMIMEHeaderKey("tri-trace-traceid"):
			tripleHeader.TracingID = v[0]
		case textproto.CanonicalMIMEHeaderKey("tri-trace-rpcid"):
			tripleHeader.TracingRPCID = v[0]
		case textproto.CanonicalMIMEHeaderKey("tri-trace-proto-bin"):
			tripleHeader.TracingContext = v[0]
		case textproto.CanonicalMIMEHeaderKey("tri-unit-info"):
			tripleHeader.ClusterInfo = v[0]
		case textproto.CanonicalMIMEHeaderKey("content-type"):
			tripleHeader.ContentType = v[0]
		case textproto.CanonicalMIMEHeaderKey("authorization"):
			tripleHeader.ContentType = v[0]
		// todo: usage of these part of fields needs to be discussed later
		//case "grpc-encoding":
		//case "grpc-status":
		//	tripleHeader.GrpcStatus = v[0]
		//case "grpc-message":
		//	tripleHeader.GrpcMessage = v[0]
		default:
		}
	}
	return tripleHeader
}
