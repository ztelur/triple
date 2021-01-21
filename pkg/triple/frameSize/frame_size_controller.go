package frameSize

import (
	"github.com/dubbogo/triple/pkg/common"
	"go.uber.org/atomic"
	"sync"
)

// H2MaxFrameController can split big data to small fm piece, and call flow control to send each piece
type H2MaxFrameController struct {
	lock         sync.Mutex
	maxFrameSize atomic.Uint32
}

func NewH2MaxFrameController() *H2MaxFrameController {
	defaultMaxFrameSize := atomic.Uint32{}
	defaultMaxFrameSize.Store(common.DefaultMaxFrameSize)
	return &H2MaxFrameController{
		lock:         sync.Mutex{},
		maxFrameSize: defaultMaxFrameSize,
	}
}

// SendGeneralDataFrame is used to split big package, send splited data with @f, and get callback chan
func (mfc *H2MaxFrameController) SendGeneralDataFrame(streamID uint32, endStream bool, data []byte, f func(uint322 uint32, pkg common.SendChanDataPkg)) chan struct{} {
	i := 0
	maxFramesize := int(mfc.maxFrameSize.Load())
	lenData := len(data)
	// split data logic
	cbChan := make(chan struct{}, 1)
	for i < lenData {
		if i+maxFramesize >= lenData { // final frame
			f(streamID, common.SendChanDataPkg{
				EndStream: endStream,
				Data:      data[i:lenData],
				StreamID:  streamID,
				Cb:        cbChan, // only final frame needs call back chan
			})
		} else { // not final frame
			f(streamID, common.SendChanDataPkg{
				EndStream: false,
				Data:      data[i : i+maxFramesize],
				StreamID:  streamID,
			})
		}
		i += maxFramesize
	}
	return cbChan
}

// SetMaxFrameSize update max frame size from setting of h2 frame
func (mfc *H2MaxFrameController) SetMaxFrameSize(newMaxFrameSize uint32) {
	mfc.maxFrameSize.Store(newMaxFrameSize)
}
