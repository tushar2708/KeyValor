package commands

import (
	"KeyValor"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/redcon"
)

const InfalidArgumentsErrorMsg = "ERR wrong number of arguments for '%s' command"

type CommandFunc func(
	conn redcon.Conn,
	args [][]byte,
	mu *sync.RWMutex,
	store *KeyValor.KeyValorDatabase,
)

var CommandMap map[string]CommandFunc = map[string]CommandFunc{
	"set":    Set,
	"get":    Get,
	"del":    Delete,
	"keys":   Keys,
	"exists": Exists,
	"expire": Expire,
	"ttl":    Ttl,
}

var Set CommandFunc = func(
	conn redcon.Conn,
	args [][]byte,
	mu *sync.RWMutex,
	store *KeyValor.KeyValorDatabase,
) {
	if len(args) != 3 {
		conn.WriteError(fmt.Sprintf(InfalidArgumentsErrorMsg, string(args[0])))
		return
	}
	mu.Lock()
	err := store.Set(string(args[1]), args[2])
	mu.Unlock()

	if err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteString("OK")
	}
}

var Get CommandFunc = func(
	conn redcon.Conn,
	args [][]byte,
	mu *sync.RWMutex,
	store *KeyValor.KeyValorDatabase,
) {
	if len(args) != 2 {
		conn.WriteError(fmt.Sprintf(InfalidArgumentsErrorMsg, string(args[0])))
		return
	}
	mu.RLock()
	val, err := store.Get(string(args[1]))
	mu.RUnlock()

	if err != nil {
		// conn.WriteError(err.Error())
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

var Delete CommandFunc = func(
	conn redcon.Conn,
	args [][]byte,
	mu *sync.RWMutex,
	store *KeyValor.KeyValorDatabase,
) {
	if len(args) != 2 {
		conn.WriteError(fmt.Sprintf(InfalidArgumentsErrorMsg, string(args[0])))
		return
	}
	mu.Lock()
	err := store.Delete(string(args[1]))
	mu.Unlock()
	if err != nil {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

var Exists CommandFunc = func(
	conn redcon.Conn,
	args [][]byte,
	mu *sync.RWMutex,
	store *KeyValor.KeyValorDatabase,
) {
	if len(args) != 2 {
		conn.WriteError(fmt.Sprintf(InfalidArgumentsErrorMsg, string(args[0])))
		return
	}
	mu.RLock()
	exists := store.Exists(string(args[1]))
	mu.RUnlock()
	if !exists {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

var Keys CommandFunc = func(
	conn redcon.Conn,
	args [][]byte,
	mu *sync.RWMutex,
	store *KeyValor.KeyValorDatabase,
) {
	if len(args) != 2 {
		conn.WriteError(fmt.Sprintf(InfalidArgumentsErrorMsg, string(args[0])))
		return
	}
	mu.RLock()
	keys, err := store.Keys(string(args[1]))
	mu.RUnlock()
	if err != nil {
		conn.WriteError(err.Error())
	} else {
		WriteRedisArray(conn, keys)
	}
}

var Ttl CommandFunc = func(
	conn redcon.Conn,
	args [][]byte,
	mu *sync.RWMutex,
	store *KeyValor.KeyValorDatabase,
) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return
	}

	mu.RLock()
	ttl, err := store.TTL(string(args[1]))
	mu.RUnlock()
	if err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteInt(int(ttl))
	}
}

var Expire CommandFunc = func(
	conn redcon.Conn,
	args [][]byte,
	mu *sync.RWMutex,
	store *KeyValor.KeyValorDatabase,
) {
	if len(args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(args[0]) + "' command")
		return
	}
	ttl, err := strconv.Atoi(string(args[2]))
	if err != nil {
		conn.WriteInt(-2)
		return
	}
	expiryNanos := time.Now().UnixNano() + int64(ttl*int(time.Second))
	expiryTime := time.Unix(0, expiryNanos)

	mu.Lock()
	err = store.Expire(string(args[1]), &expiryTime)
	mu.Unlock()
	if err != nil {
		conn.WriteInt(-1)
	} else {
		conn.WriteInt(1)
	}
}

func WriteRedisArray(conn redcon.Conn, strArr []string) {
	conn.WriteArray(len(strArr))
	for _, st := range strArr {
		conn.WriteBulkString(st)
	}
}
