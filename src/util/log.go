package util

import (
	"log"
	"os"
)

const (
	LogFormatSendAppendEntriesReq         = "[AppendEntries] [%v] SendAppendEntriesReq [T%v] [S%v]->[S%v]"
	LogFormatReceiveAppendEntriesReq      = "[AppendEntries] [%v] RecvAppendEntriesReq [T%v] [S%v]->[S%v]"
	LogFormatSendAppendEntriesResponse    = "[AppendEntries] [%v] SendAppendEntriesResp [T%v] [S%v]->[S%v]"
	LogFormatReceiveAppendEntriesResponse = "[AppendEntries] [%v] RecvAppendEntriesResp [T%v] [S%v]->[S%v]"
)

var Logger *log.Logger

func init() {
	// file, err := os.Create(fmt.Sprintf("log-%v.txt", time.Now().Unix()))
	// if err != nil {
	// 	panic(err)
	// }
	Logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
	// Logger.SetOutput(io.Discard)
}
