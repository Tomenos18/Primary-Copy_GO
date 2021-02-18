package cltalm

import (
	"Primary-Copy_GO/internal/comun"
	"Primary-Copy_GO/internal/msgsys"
	"Primary-Copy_GO/internal/srvviews"
	"Primary-Copy_GO/pkg/cltviews"
	"time"
)

type ClienteAlm struct {
	numSec int
	me     comun.HostPort
	gv     comun.HostPort
	ms     msgsys.MessageSystem
}

func InitCltAlm(gv, me comun.HostPort,
	ms msgsys.MessageSystem) (ret ClienteAlm) {
	ret.numSec = 0
	ret.me = me
	ret.gv = gv
	ret.ms = ms
	return ret
}

func (ca *ClienteAlm) GetPrimario() comun.HostPort {
	cltviews.RequestPrimary(ca.gv, ca.me, ca.ms)
	return cltviews.ReceivePrimary(ca.ms)
}

func (ca *ClienteAlm) Write(key, value string) (res string, ok bool) {
	aux, ok := ca.ms.SendReceive(ca.GetPrimario(),
		msgsys.MsgWrite{Sender: ca.me, SecNum: ca.numSec,
			Value: value, Key: key},
		srvviews.Answerwaittime*time.Millisecond)
	if ok {
		p, ok := aux.(msgsys.MsgWriteAck)
		if ok {
			ca.numSec++
			res = p.Value
		}
	}
	return res, ok
}

func (ca *ClienteAlm) Read(key string) (res string, ok bool) {
	aux, ok := ca.ms.SendReceive(ca.GetPrimario(),
		msgsys.MsgRead{Sender: ca.me, SecNum: ca.numSec,
			Key: key},
		srvviews.Answerwaittime*time.Millisecond)
	if ok {
		p, ok := aux.(msgsys.MsgReadAck)
		if ok {
			ca.numSec++
			res = p.Value
		}
	}
	return res, ok
}
