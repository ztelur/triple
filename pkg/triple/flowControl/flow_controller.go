package flowControl

import (
	"github.com/dubbogo/triple/pkg/common"
	"sync"
)

// H2FlowController cotrol
type H2FlowController struct {
	// TrInFlow is used to control windows update frame
	TrInFlow

	// FlowControlLock is locked when anyone touch sendQuota and flowControlMap
	FlowControlLock sync.Mutex

	flowControlMap  map[uint32][]common.SendChanDataPkg
	sendQuota       uint32
	sendChan        chan interface{}
}

// NewH2FlowController create H2 flow controller with @sendChan to send out common.SendChanDataPkg
func NewH2FlowController(sendChan chan interface{}) *H2FlowController {
	return &H2FlowController{
		FlowControlLock: sync.Mutex{},
		flowControlMap:  make(map[uint32][]common.SendChanDataPkg),
		sendQuota:       common.DefaultConnInitWindowSize,
		sendChan:        sendChan,
		TrInFlow: TrInFlow{
			limit: common.DefaultConnInitWindowSize,
		},
	}
}

// RunFlowControlSending starts loop to send data to sendChan
// call callback function of SendChanDataPkg if needed
func (fc *H2FlowController) RunFlowControlSending() {
	for {
		fc.FlowControlLock.Lock()
		keyList := make([]uint32, 0)
		for k, _ := range fc.flowControlMap {
			keyList = append(keyList, k)
		}
		for _, k := range keyList {
			dataList, _ := fc.flowControlMap[k]
			endIndex := 0
			for i, v := range dataList {
				if int(fc.sendQuota) > len(v.Data) {
					fc.sendChan <- v
					endIndex = i + 1
					if v.Cb != nil {
						v.Cb <- struct{}{}
					}
					fc.sendQuota -= uint32(len(v.Data))
				} else {
					break
				}
			}
			dataList = dataList[endIndex:]
			if len(dataList) == 0 {
				delete(fc.flowControlMap, k)
			} else {
				fc.flowControlMap[k] = dataList
			}
		}
		fc.FlowControlLock.Unlock()
	}
}

// pushToFlowControlMap push @pkg to fc's map
func (fc *H2FlowController) PushToFlowControlMap(id uint32, pkg common.SendChanDataPkg) {
	fc.FlowControlLock.Lock()
	defer fc.FlowControlLock.Unlock()
	v, ok := fc.flowControlMap[id]
	if ok {
		v = append(v, pkg)
		fc.flowControlMap[id] = v
	} else {
		fc.flowControlMap[id] = []common.SendChanDataPkg{pkg}
	}
}

// AddSendQuota add quota when receive WINDOW_UPDATE frame 当收到 WINDOW_UPDATE 后增加配额
func (fc *H2FlowController) AddSendQuota(addition uint32) {
	fc.FlowControlLock.Lock()
	fc.sendQuota += addition
	fc.FlowControlLock.Unlock()
}
