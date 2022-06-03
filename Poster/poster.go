package main

import (
	str "Poster/Model"
	"encoding/json"
	"github.com/nats-io/stan.go"
	"math/rand"
	"os"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func main() {
	data, err := os.ReadFile("model.json")
	if err != nil {
		panic(err)
	}
	var S str.Orders
	if err2 := json.Unmarshal(data, &S); err2 != nil {
		panic(err2)
	}
	S.OrderUid = RandStringBytesRmndr(12)
	S.CustomerId = RandStringBytesRmndr(12)
	datai, err := json.MarshalIndent(S, "", "    ")
	if err != nil {
		panic(err)
	}
	sc, _ := stan.Connect("prod", "simple-pub")
	defer sc.Close()
	sc.Publish("foo", datai)
}
