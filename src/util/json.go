package util

import "encoding/json"

func JSONMarshal(i interface{}) string {
	bytes, _ := json.Marshal(i)
	return string(bytes)
}
