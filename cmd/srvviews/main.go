package main

import (
	"Primary-Copy_GO/internal/comun"
	"Primary-Copy_GO/internal/srvviews"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Sintax error\nTry:\n./srvviews <ip:port server-views>\n")
		os.Exit(1)
	}
	me := os.Args[1]
	srvviews.InitServidorVistas(comun.HostPort(me))
}
