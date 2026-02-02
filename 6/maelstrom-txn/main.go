package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	store = make(map[int]int)
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
				if val, ok := store[op_key]; ok {
					txn_op[2] = val
				}
			case "w":
				store[op_key] = int(txn_op[2].(float64))
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
