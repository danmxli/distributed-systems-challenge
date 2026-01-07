package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// local caches to reduce KV reads
var (
	localLogs   = make(map[string]map[int]int) // key -> offset -> msg
	localLogsMu sync.RWMutex

	localOffsets   = make(map[string]int) // key -> highest known offset
	localOffsetsMu sync.RWMutex
)

// returns leader node ID responsible for a given key; leader = hash(key) % num_nodes
func getLeaderForKey(key string, nodeIDs []string) string {
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := int(h.Sum32()) % len(nodeIDs)
	return nodeIDs[idx]
}

// writes to the log for the given key, leader only
func writeMessage(ctx context.Context, kv *maelstrom.KV, logKey string, message int) (int, error) {
	counterKey := fmt.Sprintf("counter:%s", logKey)

	// CAS loop
	// counter represents next offset to write, start from 0
	for {
		var nextOffset int
		createNew := false
		if err := kv.ReadInto(ctx, counterKey, &nextOffset); err != nil {
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				nextOffset = 0
				createNew = true
			} else {
				return 0, err
			}
		}

		// try to claim this offset by incrementing counter
		var err error
		if createNew {
			err = kv.CompareAndSwap(ctx, counterKey, nil, nextOffset+1, true)
		} else {
			err = kv.CompareAndSwap(ctx, counterKey, nextOffset, nextOffset+1, false)
		}

		if err == nil {
			// counter incremented, write the message
			msgKey := fmt.Sprintf("log:%s:%d", logKey, nextOffset)
			if err := kv.Write(ctx, msgKey, message); err != nil {
				return 0, err
			}

			// update local cache
			localLogsMu.Lock()
			if localLogs[logKey] == nil {
				localLogs[logKey] = make(map[int]int)
			}
			localLogs[logKey][nextOffset] = message
			localLogsMu.Unlock()

			localOffsetsMu.Lock()
			if nextOffset >= localOffsets[logKey] {
				localOffsets[logKey] = nextOffset + 1
			}
			localOffsetsMu.Unlock()

			return nextOffset, nil
		}

		if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
			continue // retry
		}
		return 0, err
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("internal_send", func(msg maelstrom.Message) error {
		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()
		offset, err := writeMessage(ctx, kv, body.Key, body.Msg)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":   "internal_send_ok",
			"offset": offset,
		})
	})

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Type string `json:"type"`
			Key  string `json:"key"`
			Msg  int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx := context.Background()
		leader := getLeaderForKey(body.Key, n.NodeIDs())

		if leader == n.ID() {
			offset, err := writeMessage(ctx, kv, body.Key, body.Msg)
			if err != nil {
				return err
			}
			return n.Reply(msg, map[string]any{
				"type":   "send_ok",
				"offset": offset,
			})
		}

		// forward to leader
		resp, err := n.SyncRPC(ctx, leader, map[string]any{
			"type": "internal_send",
			"key":  body.Key,
			"msg":  body.Msg,
		})
		if err != nil {
			return err
		}

		var respBody struct {
			Offset int `json:"offset"`
		}
		if err := json.Unmarshal(resp.Body, &respBody); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": respBody.Offset,
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
		ctx := context.Background()
		response := make(map[string][][]int)

		for key, startOffset := range body.Offsets {
			// get the current max offset from counter
			counterKey := fmt.Sprintf("counter:%s", key)
			var currOffset int
			if err := kv.ReadInto(ctx, counterKey, &currOffset); err != nil {
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					response[key] = [][]int{}
					continue
				} else {
					return err
				}
			}

			// read messages from startOffset to currOffset-1
			var messages [][]int
			for off := startOffset; off < currOffset; off++ {
				// check cache
				localLogsMu.RLock()
				if logMap, exists := localLogs[key]; exists {
					if msgVal, ok := logMap[off]; ok {
						messages = append(messages, []int{off, msgVal})
						localLogsMu.RUnlock()
						continue
					}
				}
				localLogsMu.RUnlock()

				// read from KV
				msgKey := fmt.Sprintf("log:%s:%d", key, off)
				var msgVal int
				if err := kv.ReadInto(ctx, msgKey, &msgVal); err != nil {
					if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
						// message not yet visible
						break
					}
					return err
				}
				messages = append(messages, []int{off, msgVal})

				// update cache
				localLogsMu.Lock()
				if localLogs[key] == nil {
					localLogs[key] = make(map[int]int)
				}
				localLogs[key][off] = msgVal
				localLogsMu.Unlock()
			}
			response[key] = messages
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
