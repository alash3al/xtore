package main

import (
	"encoding/json"
)

func jsonEncode(d interface{}) []byte {
	b, _ := json.Marshal(d)
	return b
}

func jsonDecode(d []byte, i interface{}) {
	json.Unmarshal(d, &i)
}
