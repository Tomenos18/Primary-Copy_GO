package srvviews

import (
	"Primary-Copy_GO/internal/comun"
	"Primary-Copy_GO/internal/msgsys"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	// ALL its times are in ms
	timeHeartbeat  = 50 // time between two heartbeat
	Answerwaittime = 35 // timeout for answer
	failHeartbeat  = 4  // nº látidos fallidos para decidir fallo definitivo
)

// Declaracion de los estados por los que puede pasar el servidor de vistas
const (
	s_init = iota
	s_noCopy
	s_copyingFiles
	s_operative
	s_fatal
)

type state int

type typeSV struct {
	me          comun.HostPort          // me ip:port
	tick        <-chan time.Time        // Timer
	requests    chan msgsys.Message     // Requests
	nodeList    map[comun.HostPort]*int // List that have all OPERATIVE nodes
	actualView  comun.View              // Actual view
	attemptView comun.View              // Attempt view
	ms          msgsys.MessageSystem    // Message System
	state       state                   // State
}

// Init and start all things
func InitServidorVistas(me comun.HostPort) {
	// Init
	e := new(typeSV)
	e.tick = time.Tick(timeHeartbeat * time.Millisecond)
	e.requests = make(chan msgsys.Message)
	e.nodeList = make(map[comun.HostPort]*int)
	e.actualView.NumView = 0
	e.attemptView.NumView = 0
	e.ms = msgsys.Make(me)
	e.state = s_init
	// Start the mail box for request
	go e.mailBox()
	fmt.Println("GV operative -> ", e.me)
	// Loop while all ok
	for e.state != s_fatal {
		e.tratarEventos()
	}
	log.Fatal("ERROR: The content has been losed")
}

// Deal with events, event are ticks and request
func (e *typeSV) tratarEventos() {
	select {
	// Timer goes ring ring
	case <-e.tick:
		e.tickRing()
	// Some request has been received
	case msg := <-e.requests:
		e.request(msg)
	}
}

// Receives all messages and put in the request channel
func (e *typeSV) mailBox() {
	for {
		msg := e.ms.Receive()
		e.requests <- msg
	}
}

// Timer event (50 ms pass)
func (e *typeSV) tickRing() {
	// For all nodes
	for adrNode, cont := range e.nodeList {
		// Decrements the counter
		*cont--
		if *cont == 0 {
			// Delete node from the node list
			delete(e.nodeList, adrNode)
			// Thing that at this time, attemptView and actualView should be equal
			if e.attemptView.Primary == adrNode {
				e.attemptView.NumView++
				e.newPrimary()
			} else if e.attemptView.Copy == adrNode {
				e.attemptView.NumView++
				e.newCopy()
			}
		}
	}
}

func (e *typeSV) request(msg msgsys.Message) {
	switch m := msg.(type) {
	case msgsys.MsgRequestPrimary:
		e.clientPetition(m)
	case msgsys.MsgHeartbeat:
		e.heartbeat(m)
	case msgsys.MsgRequestView:
		e.requestView(m.Sender)
	case msgsys.MsgEnd:
		fmt.Println("GV down -> ", e.me)
		os.Exit(1)
	default:
		fmt.Println("The message type is unknown", m)
	}
}

func (e *typeSV) clientPetition(msg msgsys.MsgRequestPrimary) {
	msgRet := new(msgsys.MsgAnswerPrimary)
	msgRet.Server = e.actualView.Primary
	e.ms.Send(msg.Sender, msgRet)
}

func (e *typeSV) heartbeat(msg msgsys.MsgHeartbeat) {
	switch msg.NumView {
	case 0:
		e.heartbeatZero(msg.Sender)
	default:
		e.heartbeatNormal(msg)
	}
}

func (e *typeSV) newPrimary() {
	if e.attemptView.Copy != msgsys.UNDEFINEDHOST {
		if e.state != s_copyingFiles {
			e.attemptView.Primary = e.attemptView.Copy
			e.newCopy()
		} else {
			e.state = s_fatal
		}
	} else {
		e.attemptView.Primary = msgsys.UNDEFINEDHOST
		e.attemptView.Primary = e.chooseNewNodeWaitList()
		if e.attemptView.Primary != msgsys.UNDEFINEDHOST {
			e.newCopy()
		} else {
			e.state = s_fatal
		}
	}
}

func (e *typeSV) newCopy() {
	e.attemptView.Copy = msgsys.UNDEFINEDHOST
	e.attemptView.Copy = e.chooseNewNodeWaitList()
	if e.attemptView.Copy != "" {
		e.state = s_copyingFiles
	} else {
		e.state = s_noCopy
	}
}

func (e *typeSV) chooseNewNodeWaitList() (ret comun.HostPort) {
	if len(e.nodeList) > 0 {
		for adrNode := range e.nodeList {
			if adrNode != e.attemptView.Primary &&
				adrNode != e.attemptView.Copy {
				ret = adrNode
				break
			}
		}
	} else {
		ret = ""
	}
	return ret
}

func (e *typeSV) heartbeatNormal(msg msgsys.MsgHeartbeat) {
	if msg.Sender == e.attemptView.Primary &&
		msg.NumView == e.attemptView.NumView &&
		e.state == s_copyingFiles {
		e.state = s_operative
		e.actualView = e.attemptView
	}
	if e.nodeList[msg.Sender] == nil {
		aux := new(int)
		e.nodeList[msg.Sender] = aux
	}
	*e.nodeList[msg.Sender] = failHeartbeat
	msgRet := new(msgsys.MsgView)
	msgRet.View = e.attemptView
	e.ms.Send(msg.Sender, msgRet)
}

func (e *typeSV) heartbeatZero(adrNode comun.HostPort) {
	if e.nodeList[adrNode] == nil {
		aux := new(int)
		e.nodeList[adrNode] = aux
	}
	*e.nodeList[adrNode] = failHeartbeat
	switch e.state {
	case s_init:
		e.attemptView.NumView++
		e.newPrimary()
	case s_noCopy:
		if adrNode == e.attemptView.Primary {
			e.state = s_fatal
		}
		e.attemptView.NumView++
		e.newCopy()
	case s_copyingFiles:
		if adrNode == e.actualView.Primary {
			e.state = s_fatal
		} else if adrNode == e.actualView.Copy {
			e.actualView.NumView++
			e.newCopy()
		}
	default:
		if adrNode == e.actualView.Primary {
			e.actualView.NumView++
			e.newPrimary()
		} else if adrNode == e.actualView.Copy {
			e.actualView.NumView++
			e.newCopy()
		}
	}
	msgRet := new(msgsys.MsgView)
	msgRet.View = e.actualView
	e.ms.Send(adrNode, msgRet)
}

func (e *typeSV) requestView(adrNode comun.HostPort) {
	msgRet := new(msgsys.MsgAnswerView)
	msgRet.View = e.actualView
	e.ms.Send(adrNode, msgRet)
}
