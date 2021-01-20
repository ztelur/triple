package triple

import "sync/atomic"

type trInFlow struct {
	limit               uint32
	unacked             uint32
	effectiveWindowSize uint32
}

func (f *trInFlow) newLimit(n uint32) uint32 {
	d := n - f.limit
	f.limit = n
	f.updateEffectiveWindowSize()
	return d
}

func (f *trInFlow) onData(n uint32) uint32 {
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

func (f *trInFlow) reset() uint32 {
	w := f.unacked
	f.unacked = 0
	f.updateEffectiveWindowSize()
	return w
}

func (f *trInFlow) updateEffectiveWindowSize() {
	atomic.StoreUint32(&f.effectiveWindowSize, f.limit-f.unacked)
}

func (f *trInFlow) getSize() uint32 {
	return atomic.LoadUint32(&f.effectiveWindowSize)
}
