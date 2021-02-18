package main

import (
	"Primary-Copy_GO/internal/comun"
	"Primary-Copy_GO/internal/srvkv"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Sintax error\nTry:\n./srvkv <ip:port of server-key-value> <ip:port of server-views>\n")
		os.Exit(1)
	}
	me := os.Args[1]
	srvviews := os.Args[2]
	// Start the server
	srvkv.InitSrvKV(comun.HostPort(srvviews), comun.HostPort(me))
}
