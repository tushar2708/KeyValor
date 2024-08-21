package timeutils

import "time"

func CurrentTimeNanos() int64 {
	return time.Now().UnixNano()
}
