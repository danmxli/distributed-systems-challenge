package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	store = make(map[int]int)
	mu    sync.RWMutex
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body TxnMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for _, txn_op := range body.Txn {
			op_type := txn_op[0].(string)
			op_key := int(txn_op[1].(float64))

			switch op_type {
			case "r":
				mu.RLock()
				if val, ok := store[op_key]; ok {
					txn_op[2] = val
				}
				mu.RUnlock()
			case "w":
				mu.Lock()
				store[op_key] = int(txn_op[2].(float64))
				mu.Unlock()
			}
		}

		body.Type = "txn_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type TxnMessageBody struct {
	maelstrom.MessageBody
	Txn [][]any `json:"txn"`
}
