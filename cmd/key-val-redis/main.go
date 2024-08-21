package main

import (
	"KeyValor"
	"KeyValor/cmd/key-val-redis/commands"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

var addr = ":6379"

func main() {
	var mu sync.RWMutex
	// var items = make(map[string][]byte)
	// var ps redcon.PubSub
	// go log.Printf("started server at %s", addr)

	homeDir, _ := os.UserHomeDir()
	keyValurStoreDir := filepath.Join(homeDir, "keyvalor")
	os.MkdirAll(keyValurStoreDir, fs.ModePerm)
	store, err := KeyValor.NewKeyValorDB(KeyValor.WithDirectory(keyValurStoreDir))
	if err != nil {
		panic(fmt.Sprintf("cannot initialize KeyValor store, err: [%+v]", err))
	}

	err = redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			commandName := strings.ToLower(string(cmd.Args[0]))
			switch commandName {
			default:
				conn.WriteError("ERR unknown command '" + commandName + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set", "get", "del", "keys", "exists", "expire", "ttl":
				commands.CommandMap[commandName](conn, cmd.Args, &mu, store)
			}
		},
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
			log.Println("closing key-value store")
			store.Shutdown()
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
