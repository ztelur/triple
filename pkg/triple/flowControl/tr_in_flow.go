package flowControl

import "sync/atomic"

// TrInFlow is used to control the send of window update frame
type TrInFlow struct {
	limit               uint32
	unacked             uint32
	effectiveWindowSize uint32
}

func (f *TrInFlow) newLimit(n uint32) uint32 {
	d := n - f.limit
	f.limit = n
	f.updateEffectiveWindowSize()
	return d
}

func (f *TrInFlow) OnData(n uint32) uint32 {
	f.unacked += n
	if f.unacked >= f.limit/4 {
		w := f.unacked
		f.unacked = 0
		f.updateEffectiveWindowSize()
		return w
	}
	f.updateEffectiveWindowSize()
	return 0
}

func (f *TrInFlow) reset() uint32 {
	w := f.unacked
	f.unacked = 0
	f.updateEffectiveWindowSize()
	return w
}

func (f *TrInFlow) updateEffectiveWindowSize() {
	atomic.StoreUint32(&f.effectiveWindowSize, f.limit-f.unacked)
}

func (f *TrInFlow) getSize() uint32 {
	return atomic.LoadUint32(&f.effectiveWindowSize)
}
