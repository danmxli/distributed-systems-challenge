package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func nodeKey(nodeID string) string {
	return "gcounter:" + nodeID
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body struct {
			Type  string `json:"type"`
			Delta int    `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		key := nodeKey(n.ID())
		for {
			curr, err := kv.ReadInt(ctx, key)
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					// initialize key value to zero if current does not exist
					if werr := kv.Write(ctx, key, 0); werr == nil {
						continue
					}
				}
				return err
			}

			next := curr + body.Delta
			err = kv.CompareAndSwap(ctx, key, curr, next, false)
			if err == nil {
				break
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		sum := 0
		for _, nodeID := range n.NodeIDs() {
			v, err := kv.ReadInt(ctx, nodeKey(nodeID))
			if err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					continue
				}
				return err
			}
			sum += v
		}
		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": sum,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
