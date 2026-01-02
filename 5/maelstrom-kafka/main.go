package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	// in-memory message store
	msg_store = make(map[string][][]int)
	// in-memory highest offset per key
	offsets = make(map[string]int)
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Type string `json:"type"`
			Key  string `json:"key"`
			Msg  int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offset := len(msg_store[body.Key])
		msg_store[body.Key] = append(msg_store[body.Key], []int{offset, body.Msg})
		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body struct {
			Type    string         `json:"type"`
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		response := make(map[string][][]int)
		for key, offset := range body.Offsets {
			if messages, exists := msg_store[key]; exists {
				if offset < len(messages) {
					response[key] = messages[offset:]
				} else {
					response[key] = [][]int{}
				}
			} else {
				response[key] = [][]int{}
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": response,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Type    string         `json:"type"`
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for key, offset := range body.Offsets {
			if currentOffset, exists := offsets[key]; !exists || offset > currentOffset {
				offsets[key] = offset
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Type string   `json:"type"`
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := make(map[string]int)
		for _, key := range body.Keys {
			if offset, exists := offsets[key]; exists {
				response[key] = offset
			}
		}

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": response,
		})
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
