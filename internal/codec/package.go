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
	"encoding/binary"
)

import (
	"github.com/dubbogo/triple/pkg/common"
)

const (
	// DUBBO3 is dubbo3 protocol name
	DUBBO3 = "dubbo3"
)

func init() {
	common.SetPackageHandler(DUBBO3, NewTriplePkgHandler)
}

// TriplePackageHandler handles package of triple
// e.g. now it impl as deal with pkg data as: [:5]is length and [5:lenght] is body
type TriplePackageHandler struct {
	header *TripleHeader
	//codec  *CodeC
}

// Frame2PkgData/Pkg2FrameData is not as useless as you think!
// it add the len of frameData to the front of Triple/grpc pkg
// when receiving streaming rpc package, as there is no EndStream flag between each stream pkg
// the len of frameData in the front of pkg is the only way to split each pkg in transporting
func (t *TriplePackageHandler) Frame2PkgData(frameData []byte) ([]byte, uint32) {
	if len(frameData) < 5 {
		return []byte{}, 0
	}
	lineHeader := frameData[:5]
	length := binary.BigEndian.Uint32(lineHeader[1:])
	if len(frameData) < 5+int(length) {
		// used in streaming rpc splited header
		// we only need length of all data
		return frameData[5:], length
	}
	return frameData[5 : 5+length], length
}
func (t *TriplePackageHandler) Pkg2FrameData(pkgData []byte) []byte {
	rsp := make([]byte, 5+len(pkgData))
	rsp[0] = byte(0)
	binary.BigEndian.PutUint32(rsp[1:], uint32(len(pkgData)))
	copy(rsp[5:], pkgData[:])
	return rsp
}

func NewTriplePkgHandler() common.PackageHandler {
	return &TriplePackageHandler{}
}
