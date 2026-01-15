package raft

import (
	"fmt"
	"log"
	"os"
)

const (
	LogLevelTrace = iota
	LogLevelDebug
	LogLevelInfo
	LogLevelWarning
	LogLevelError
	LogLevelNone
)

// Debugging
const (
	Debug    = false
	LogLevel = LogLevelNone
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	//logFile, err := os.OpenFile("raft.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//if err != nil {
	//	log.Println("OpenFile error:", err.Error())
	//} else {
	//	log.SetOutput(logFile)
	//}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DWriteTraceLog(v ...interface{}) {
	DWriteLog(LogLevelTrace, "[My6.824] TRACE - ", v...)
}

func DWriteDebugLog(v ...interface{}) {
	DWriteLog(LogLevelDebug, "[My6.824] DEBUG - ", v...)
}

func DWriteInfoLog(v ...interface{}) {
	DWriteLog(LogLevelInfo, "[My6.824] INFO - ", v...)
}

func DWriteWarningLog(v ...interface{}) {
	DWriteLog(LogLevelWarning, "[My6.824] WARNING - ", v...)
}

func DWriteErrorLog(v ...interface{}) {
	DWriteLog(LogLevelError, "[My6.824] ERROR - ", v...)
}

func DWriteLog(level int, prefix string, v ...interface{}) {
	if level < LogLevel {
		return
	}
	log.Print(prefix + fmt.Sprintln(v...)) // write the log message
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
