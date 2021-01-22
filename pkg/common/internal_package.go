package common

import h2 "golang.org/x/net/http2"

type SendChanDataPkg struct {
	Data      []byte
	EndStream bool
	StreamID  uint32
	Cb        chan struct{}
}

type SendChanGoAwayPkg struct {
	MaxSteamID uint32
	Code       h2.ErrCode
	DebugData  []byte
}

type SendWindowsUpdataPkg struct {
	StreamID  uint32
	Increment uint32
}

type SendSettigACK struct {
}
