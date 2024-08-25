package strictchecks

import "fmt"

func MustBeTrueOrPanic(condition bool, message string, args ...interface{}) {
	if !condition {
		panic(fmt.Sprintf(message, args...))
	}
}
