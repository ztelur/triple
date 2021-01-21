package common

type H2ControllerState uint32

const (
	Reachable = H2ControllerState(0)
	Closing   = H2ControllerState(1)
	Draining  = H2ControllerState(2)
)

const DefaultMaxFrameSize = 16384
const DefaultMaxConcurrentStreams = 100
const DefaultStreamInitWindowSize = 65535
const DefaultConnInitWindowSize = 65535
