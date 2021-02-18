package comun

import (
	"fmt"
	"os"
)

type HostPort string

type View struct {
	NumView int
	Primary HostPort
	Copy    HostPort
}

func CheckError(err error, comment string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s -- %s", err.Error(), comment)
	}
}
