package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		format = time.Now().Format("15:04:05.99999999") + " " + format
		log.Printf(format, a...)
	}
	return
}
