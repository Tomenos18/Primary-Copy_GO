// Most of this code is from Unai, a teacher of Zaragoza University
// but I modified the code so it's not exactly like Unai's code
package msgsys

import (
	"Primary-Copy_GO/internal/comun"
	"encoding/gob"
	"log"
	"net"
	"time"
)

const (
	UNDEFINEDHOST = ""
	MAXMESSAGES   = 10000
)

type Message interface{} // Generic type

// Data types about primary-copy replication
type MsgHeartbeat struct {
	NumView int
	Sender  comun.HostPort
}
type MsgView struct {
	View comun.View
}

type MsgRequestView struct {
	Sender comun.HostPort
}

type MsgAnswerView struct {
	View comun.View
}

type MsgRequestPrimary struct {
	Sender comun.HostPort
}

type MsgAnswerPrimary struct {
	Server comun.HostPort
}

type MsgEnd struct{}

// Data types about kv service
// Read messages
type MsgRead struct {
	Key    string
	SecNum int
	Sender comun.HostPort
}

type MsgReadAck struct {
	Value  string
	SecNum int
	Sender comun.HostPort
}

type MsgSpreadRead struct {
	Key    string
	SecNum int
	Sender comun.HostPort
}

// Write messages
type MsgWrite struct {
	Key    string
	Value  string
	SecNum int
	Sender comun.HostPort
}

type MsgWriteAck struct {
	Value  string
	SecNum int
	Sender comun.HostPort
}

type MsgSpreadWrite struct {
	Key    string
	Value  string
	SecNum int
	Sender comun.HostPort
}

// Messages for transfering the database
type MsgTransferingDatabase struct {
	Clients   map[comun.HostPort]int
	Database  map[string]string
	LastValue map[comun.HostPort]string
	NumView   int
}

type MsgTransferDatabaseAck struct {
	CopyIp  comun.HostPort
	NumView int
}

// Type that implements asynchronous messages
type MessageSystem struct {
	me       comun.HostPort
	listener net.Listener
	mailBox  chan Message
	done     chan struct{}
	tmr      *time.Timer
}

// Constructor
func Make(me comun.HostPort) (ms MessageSystem) {
	ms = MessageSystem{me: me,
		mailBox: make(chan Message, MAXMESSAGES),
		done:    make(chan struct{}),
		tmr:     time.NewTimer(0)}

	if !ms.tmr.Stop() {
		<-ms.tmr.C
	}

	var err error
	ms.listener, err = net.Listen("tcp", string(ms.me))
	comun.CheckError(err, "Problem with acceptance of networkReceiver (func Make)")

	go ms.networkReceiver()
	ms.RegisterAllMessages()

	return ms
}

func (ms *MessageSystem) RegisterAllMessages() {
	tiposMensaje := []Message{MsgHeartbeat{}, MsgView{}, MsgRequestView{},
		MsgAnswerView{}, MsgRequestPrimary{}, MsgAnswerPrimary{}, MsgEnd{},
		MsgRead{}, MsgReadAck{}, MsgSpreadRead{}, MsgWrite{},
		MsgWriteAck{}, MsgSpreadWrite{}, MsgTransferingDatabase{},
		MsgTransferDatabaseAck{}}
	Register(tiposMensaje)
}

func Register(tiposMensaje []Message) {
	for _, t := range tiposMensaje {
		gob.Register(t)
	}
}

func (ms MessageSystem) Me() comun.HostPort {
	return ms.me
}

// Close message system and all its goroutines
func (ms *MessageSystem) CloseMessageSystem() {
	close(ms.done)
	err := ms.listener.Close()
	comun.CheckError(err, "Problem about close Listener (func CloseMessageSystem)")
	close(ms.mailBox)
	// Wait a milisecond for goroutine to die
	time.Sleep(time.Millisecond)
}

// recieve the messages and put in the mailBox
func (ms *MessageSystem) networkReceiver() {
	for {
		conn, err := ms.listener.Accept()
		if err != nil {
			select {
			case <-ms.done:
				log.Println("STOPPED listening for messages at", string(ms.me))
				return
			default:
				comun.CheckError(err, "Problem with acceptance of listener (func networkReceiver)")
			}
		}

		decoder := gob.NewDecoder(conn)
		var msg Message
		err = decoder.Decode(&msg)
		comun.CheckError(err, "Problem with decode (func networkReceiver)")

		conn.Close()

		ms.mailBox <- msg
	}
}

// Send some message
func (ms MessageSystem) Send(receiver comun.HostPort, msg Message) {

	conn, err := net.Dial("tcp", string(receiver))
	comun.CheckError(err, "Problem with DialTCP (func Send)")

	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(&msg)
	comun.CheckError(err, "Problem with encode (func Send)")

	conn.Close()
}

// Send some message and receive the reply
func (ms *MessageSystem) SendReceive(receiver comun.HostPort,
	msg Message, timeout time.Duration) (res Message, ok bool) {

	log.SetFlags(log.Lmicroseconds)

	ms.Send(receiver, msg)

	ms.tmr.Reset(timeout)

	select {
	case res = <-ms.mailBox:
		if !ms.tmr.Stop() {
			<-ms.tmr.C
		}
		return res, true

	case <-ms.tmr.C:
		return struct{}{}, false
	}
}

// Read recieved messages, if there is not it get stoped
func (ms *MessageSystem) Receive() Message {
	return <-ms.mailBox
}
