package triple

type H2ControllerState uint32

const (
	reachable = H2ControllerState(0)
	closing   = H2ControllerState(1)
	draining  = H2ControllerState(2)
)

const defaultMaxFrameSize = 16384
