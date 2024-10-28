package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tidwall/redcon"

	"KeyValor"
	"KeyValor/cmd/key-val-redis/commands"
	"KeyValor/log"
)

var addr = ":6379"

func main() {
	var mu sync.RWMutex

	homeDir, _ := os.UserHomeDir()
	keyValurStoreDir := filepath.Join(homeDir, "keyvalor")
	logDir := filepath.Join(homeDir, "keyvalorlogs")
	log.InitLogger(logDir)

	os.MkdirAll(keyValurStoreDir, fs.ModePerm)
	db, err := KeyValor.NewKeyValorDB(KeyValor.WithDirectory(keyValurStoreDir))
	if err != nil {
		panic(fmt.Sprintf("cannot initialize KeyValor store, err: [%+v]", err))
	}

	err = redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {

			if len(cmd.Args) == 0 {
				conn.WriteError(fmt.Sprintf("ERR no arguments for command: [%s]", string(cmd.Raw)))
				return
			}

			commandName := strings.ToLower(string(cmd.Args[0]))

			commandFunc, supported := commands.CommandMap[commandName]
			if !supported {
				conn.WriteError("ERR unknown command '" + commandName + "'")
			} else {
				commandFunc(conn, cmd.Args, &mu, db)
			}
		},
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			log.Infof("closed: %s, err: %v", conn.RemoteAddr(), err)
			log.Infof("closing key-value store")
			db.Shutdown()
		},
	)
	if err != nil {
		log.Fatalf("%v", err)
	}
}
