package cltviews

import (
	"Primary-Copy_GO/internal/comun"
	"Primary-Copy_GO/internal/msgsys"
)

func RequestPrimary(viewsServer comun.HostPort, adrSender comun.HostPort,
	ms msgsys.MessageSystem) {
	msg := new(msgsys.MsgRequestPrimary)
	msg.Sender = adrSender
	ms.Send(viewsServer, msg)
}

func ReceivePrimary(ms msgsys.MessageSystem) (primary comun.HostPort) {
	msg := ms.Receive()
	aux, ok := msg.(msgsys.MsgAnswerPrimary)
	for !ok {
		msg = ms.Receive()
		aux, ok = msg.(msgsys.MsgAnswerPrimary)
	}
	primary = aux.Server
	return primary
}

func SendHeartbeat(viewsServer comun.HostPort, actualView int,
	adrSender comun.HostPort, ms msgsys.MessageSystem) {
	msg := new(msgsys.MsgHeartbeat)
	msg.NumView = actualView
	msg.Sender = adrSender
	ms.Send(viewsServer, msg)
}

func ReceiveView(ms msgsys.MessageSystem) (view comun.View) {
	msg := ms.Receive()
	aux, ok := msg.(msgsys.MsgView)
	for !ok {
		msg = ms.Receive()
		aux, ok = msg.(msgsys.MsgView)
	}
	view = aux.View
	return view
}
