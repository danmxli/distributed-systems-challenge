package main

import (
	"context"
	"encoding/json"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// var (
// 	// in-memory message store
// 	msg_store = make(map[string][][]int)
// 	// in-memory highest offset per key
// 	offsets = make(map[string]int)
// )

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Type string `json:"type"`
			Key  string `json:"key"`
			Msg  int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// offset := len(msg_store[body.Key])
		// msg_store[body.Key] = append(msg_store[body.Key], []int{offset, body.Msg})
		ctx := context.Background()
		key := fmt.Sprintf("log:%s", body.Key)

		// CAS loop for concurrent writes
		for {
			var messages [][]int
			createFromNotExist := false
			if err := kv.ReadInto(ctx, key, &messages); err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					// key does not exist, initialize empty messages
					messages = make([][]int, 0)
					createFromNotExist = true
				} else {
					return err
				}
			}

			// attempt CAS write
			offset := len(messages)
			
			newMessages := make([][]int, len(messages)+1)
			copy(newMessages, messages)
			newMessages[len(messages)] = []int{offset, body.Msg}

			var err error
			if createFromNotExist {
				err = kv.CompareAndSwap(ctx, key, messages, newMessages, true)
			} else {
				err = kv.CompareAndSwap(ctx, key, messages, newMessages, false)
			}
			if err == nil {
				// success
				return n.Reply(msg, map[string]any{
					"type":   "send_ok",
					"offset": offset,
				})
			}
			// retry on CAS failure
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				continue
			}
			return err
		}
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body struct {
			Type    string         `json:"type"`
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		context := context.Background()
		response := make(map[string][][]int)

		for key, offset := range body.Offsets {
			var messages [][]int
			if err := kv.ReadInto(context, fmt.Sprintf("log:%s", key), &messages); err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					// key does not exist, return empty list
					response[key] = [][]int{}
					continue
				} else {
					return err
				}
			}

			if offset < len(messages) {
				response[key] = messages[offset:]
			} else {
				response[key] = [][]int{}
			}
			// if messages, exists := msg_store[key]; exists {
			// 	if offset < len(messages) {
			// 		response[key] = messages[offset:]
			// 	} else {
			// 		response[key] = [][]int{}
			// 	}
			// } else {
			// 	response[key] = [][]int{}
			// }
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

		ctx := context.Background()
		for key, offset := range body.Offsets {
			// if currentOffset, exists := offsets[key]; !exists || offset > currentOffset {
			// 	offsets[key] = offset
			// }

			offsetKey := fmt.Sprintf("offset:%s", key)
			// CAS loop for concurrent writes
			for {
				var currentOffset int
				createFromNotExist := false
				if err := kv.ReadInto(ctx, offsetKey, &currentOffset); err != nil {
					if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
						// key does not exist, initialize to zero
						currentOffset = -1
						createFromNotExist = true
					} else {
						return err
					}
				}

				if offset <= currentOffset {
					break
				}
				var err error
				if createFromNotExist {
					err = kv.CompareAndSwap(ctx, offsetKey, currentOffset, offset, true)
				} else {
					err = kv.CompareAndSwap(ctx, offsetKey, currentOffset, offset, false)
				}
				if err == nil {
					// success
					break
				}
				// retry on CAS failure
				if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
					continue
				}
				return err
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

		ctx := context.Background()
		response := make(map[string]int)
		for _, key := range body.Keys {
			// if offset, exists := offsets[key]; exists {
			// 	response[key] = offset
			// }
			var offset int
			if err := kv.ReadInto(ctx, fmt.Sprintf("offset:%s", key), &offset); err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					continue
				} else {
					return err
				}
			}
			response[key] = offset
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
