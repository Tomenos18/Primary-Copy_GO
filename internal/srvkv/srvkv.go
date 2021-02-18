package srvkv

import (
	"Primary-Copy_GO/internal/comun"
	"Primary-Copy_GO/internal/msgsys"
	"Primary-Copy_GO/pkg/cltviews"
	"fmt"
	"os"
	"time"
)

const (
	// ALL its times are in ms
	timeHeartbeat  = 50  // time between two heartbeat
	Answerwaittime = 35  // timeout for answer
	timeoutCopy    = 200 // timeout for answer of a copy server
)

// Declaration of all states
const (
	s_init = iota
	s_copyOperative
	s_copyDown
	s_primaryOperative
	s_primaryDown
	s_waitList
	s_fatal
)

type state int

type typeSkv struct {
	tick            <-chan time.Time
	requests        chan msgsys.Message
	actualView      comun.View
	adrServerViews  comun.HostPort
	me              comun.HostPort
	ms              msgsys.MessageSystem
	state           state
	readAcks        chan msgsys.MsgReadAck
	writeAcks       chan msgsys.MsgWriteAck
	transferingAcks chan msgsys.MsgTransferDatabaseAck
	transfer        chan msgsys.MsgTransferingDatabase
	databasekv      map[string]string
	clientsMemory   map[comun.HostPort]int
	lastValue       map[comun.HostPort]string
}

func InitSrvKV(srvvtsDir comun.HostPort, me comun.HostPort) {
	e := new(typeSkv)
	e.tick = time.Tick(timeHeartbeat * time.Millisecond)
	e.requests = make(chan msgsys.Message, 1000)
	e.actualView.NumView = -1
	e.adrServerViews = srvvtsDir
	e.me = me
	e.ms = msgsys.Make(me)
	e.state = s_init
	e.readAcks = make(chan msgsys.MsgReadAck, 100)
	e.writeAcks = make(chan msgsys.MsgWriteAck, 100)
	e.transferingAcks = make(chan msgsys.MsgTransferDatabaseAck, 100)
	e.transfer = make(chan msgsys.MsgTransferingDatabase, 100)
	e.databasekv = make(map[string]string)
	e.clientsMemory = make(map[comun.HostPort]int)
	e.lastValue = make(map[comun.HostPort]string)

	go e.filter()
	fmt.Println("Skv operative -> ", e.me)
	cltviews.SendHeartbeat(e.adrServerViews, 0, e.me, e.ms)

	go e.sendHeartbeat()

	for e.state != s_fatal {
		msg := <-e.requests
		e.request(msg)
	}
}

func (e *typeSkv) filter() {
	for {
		m := e.ms.Receive()
		switch msg := m.(type) {
		case msgsys.MsgReadAck:
			e.readAcks <- msg
		case msgsys.MsgWriteAck:
			e.writeAcks <- msg
		case msgsys.MsgTransferDatabaseAck:
			e.transferingAcks <- msg
		case msgsys.MsgTransferingDatabase:
			e.transfer <- msg
		default:
			e.requests <- msg
		}
	}
}

func (e *typeSkv) sendHeartbeat() {
	for {
		<-e.tick
		cltviews.SendHeartbeat(e.adrServerViews, e.actualView.NumView,
			e.me, e.ms)
	}
}

func (e *typeSkv) request(msg msgsys.Message) {
	switch m := msg.(type) {
	case msgsys.MsgRead:
		e.read(m)
	case msgsys.MsgSpreadRead:
		e.spreadRead(m)
	case msgsys.MsgWrite:
		e.write(m)
	case msgsys.MsgSpreadWrite:
		e.spreadWrite(m)
	case msgsys.MsgView:
		e.view(m)
	case msgsys.MsgEnd:
		fmt.Println("Skv down -> ", e.me)
		os.Exit(1)
	default:
		fmt.Println("The message type is unknown", m)
	}
}

func (e *typeSkv) read(msg msgsys.MsgRead) {
	if e.state == s_primaryOperative {
		_, ok := e.clientsMemory[msg.Sender]
		if !ok {
			e.clientsMemory[msg.Sender] = msg.SecNum - 1
		}
		Value, ok := e.databasekv[msg.Key]
		if !ok {
			Value = ""
		}
		if e.clientsMemory[msg.Sender] < msg.SecNum {
			msgRet := new(msgsys.MsgSpreadRead)
			msgRet.Key = msg.Key
			msgRet.SecNum = msg.SecNum
			msgRet.Sender = msg.Sender
			e.ms.Send(e.actualView.Copy, msgRet)
			timeout := time.Tick(timeoutCopy * time.Millisecond)
			sig := true
			for sig {
				select {
				case <-timeout:
					sig = false
				case msgAck := <-e.readAcks:
					if msgAck.Sender == msg.Sender &&
						msgAck.SecNum == msg.SecNum &&
						msgAck.Value == Value {
						e.clientsMemory[msg.Sender] = msg.SecNum
						e.lastValue[msg.Sender] = Value
						msgVal := new(msgsys.MsgReadAck)
						msgVal.SecNum = msg.SecNum
						msgVal.Value = Value
						e.ms.Send(msg.Sender, msgVal)
						sig = false
					}
				}
			}
		} else {
			msgVal := new(msgsys.MsgReadAck)
			msgVal.SecNum = msg.SecNum
			msgVal.Value = e.lastValue[msg.Sender]
			e.ms.Send(msg.Sender, msgVal)
		}
	}
}

func (e *typeSkv) spreadRead(msg msgsys.MsgSpreadRead) {
	if e.state == s_copyOperative {
		e.clientsMemory[msg.Sender] = msg.SecNum
		Value, ok := e.databasekv[msg.Key]
		e.lastValue[msg.Sender] = Value
		msgVal := new(msgsys.MsgReadAck)
		if ok {
			msgVal.Value = Value
		} else {
			msgVal.Value = ""
		}
		msgVal.SecNum = msg.SecNum
		msgVal.Sender = msg.Sender
		e.ms.Send(e.actualView.Primary, msgVal)
	}
}

func (e *typeSkv) write(msg msgsys.MsgWrite) {
	if e.state == s_primaryOperative {
		_, ok := e.clientsMemory[msg.Sender]
		if !ok {
			e.clientsMemory[msg.Sender] = msg.SecNum - 1
		}
		if e.clientsMemory[msg.Sender] < msg.SecNum {
			msgRet := new(msgsys.MsgSpreadWrite)
			msgRet.Key = msg.Key
			msgRet.Value = msg.Value
			msgRet.SecNum = msg.SecNum
			msgRet.Sender = msg.Sender
			e.ms.Send(e.actualView.Copy, msgRet)
			timeout := time.Tick(timeoutCopy * time.Millisecond)
			sig := true
			for sig {
				select {
				case <-timeout:
					sig = false
				case msgAck := <-e.writeAcks:
					if msgAck.Sender == msg.Sender &&
						msgAck.SecNum == msg.SecNum &&
						msgAck.Value == msgRet.Value {
						e.clientsMemory[msg.Sender] = msg.SecNum
						e.databasekv[msg.Key] = msg.Value
						e.lastValue[msg.Sender] = msg.Value
						msgVal := new(msgsys.MsgWriteAck)
						msgVal.SecNum = msg.SecNum
						msgVal.Value = msg.Value
						e.ms.Send(msg.Sender, msgVal)
						sig = false
					}
				}
			}
		} else {
			msgVal := new(msgsys.MsgReadAck)
			msgVal.SecNum = msg.SecNum
			msgVal.Value = e.lastValue[msg.Sender]
			e.ms.Send(msg.Sender, msgVal)
		}
	}
}

func (e *typeSkv) spreadWrite(msg msgsys.MsgSpreadWrite) {
	if e.state == s_copyOperative {
		e.clientsMemory[msg.Sender] = msg.SecNum
		e.databasekv[msg.Key] = msg.Value
		e.lastValue[msg.Sender] = msg.Value
		msgVal := new(msgsys.MsgWriteAck)
		msgVal.SecNum = msg.SecNum
		msgVal.Value = msg.Value
		msgVal.Sender = msg.Sender
		e.ms.Send(e.actualView.Primary, msgVal)
	}
}

func (e *typeSkv) view(msg msgsys.MsgView) {
	if e.actualView.NumView < msg.View.NumView {
		if msg.View.Primary == e.me {
			if msg.View.Copy == "" {
				e.state = s_primaryDown
				e.actualizarVista(msg.View)
			} else {
				msgVal := new(msgsys.MsgTransferingDatabase)
				msgVal.Clients = e.clientsMemory
				msgVal.Database = e.databasekv
				msgVal.LastValue = e.lastValue
				msgVal.NumView = msg.View.NumView
				e.ms.Send(msg.View.Copy, msgVal)
				timeout := time.Tick(timeoutCopy * time.Millisecond)
				sig := true
				for sig {
					select {
					case <-timeout:
						sig = false
						e.state = s_primaryDown
					case msgAck := <-e.transferingAcks:
						if msgAck.CopyIp == msg.View.Copy &&
							msgAck.NumView == msg.View.NumView {
							sig = false
							e.actualizarVista(msg.View)
							e.state = s_primaryOperative
						}
					}
				}
			}
		} else if msg.View.Copy == e.me {
			timeout := time.Tick(timeoutCopy * time.Millisecond)
			sig := true
			for sig {
				select {
				case <-timeout:
					sig = false
					e.state = s_copyDown
				case msgCp := <-e.transfer:
					if msgCp.NumView == msg.View.NumView {
						msgVal := new(msgsys.MsgTransferDatabaseAck)
						msgVal.CopyIp = e.me
						msgVal.NumView = msg.View.NumView
						e.ms.Send(msg.View.Primary, msgVal)
						sig = false
						e.clientsMemory = msgCp.Clients
						e.databasekv = msgCp.Database
						e.lastValue = msgCp.LastValue
						e.actualizarVista(msg.View)
						e.state = s_copyOperative
					}
				}
			}
		} else {
			e.actualizarVista(msg.View)
			e.state = s_waitList
		}
	}
}

func (e *typeSkv) actualizarVista(vts comun.View) {
	e.actualView.NumView = vts.NumView
	e.actualView.Primary = vts.Primary
	e.actualView.Copy = vts.Copy
}
