package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	message_set = make(map[int]struct{})
	neighbors   = make([]string, 0)
	mu          sync.RWMutex
)

func snapshotMessages() []int {
	// helper to convert hashset to slice with read lock
	mu.RLock()
	defer mu.RUnlock()

	msgs := make([]int, 0, len(message_set))
	for m := range message_set {
		msgs = append(msgs, m)
	}
	return msgs
}

func main() {
	n := maelstrom.NewNode()

	// non-blocking periodic gossiping
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond) // 200–500ms common
		defer ticker.Stop()

		for range ticker.C {
			mu.RLock()
			nbs := append([]string(nil), neighbors...)
			mu.RUnlock()

			if len(nbs) == 0 {
				continue
			}

			msgs := snapshotMessages()
			if len(msgs) == 0 {
				continue
			}

			for _, nb := range nbs {
				for _, msg := range msgs {
					go n.Send(nb, map[string]any{
						"type":    "gossip",
						"message": msg,
					})
				}
			}
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := int(body["message"].(float64))

		mu.Lock()
		message_set[message] = struct{}{}
		mu.Unlock()

		// first reply to sender
		if err := n.Reply(msg, map[string]any{"type": "broadcast_ok"}); err != nil {
			return err
		}

		// gossip to neighbors
		mu.RLock()
		nbs := append([]string(nil), neighbors...)
		mu.RUnlock()
		for _, neighbor := range nbs {
			if neighbor == msg.Src {
				continue
			}
			go n.Send(neighbor, map[string]any{
				"type":    "gossip",
				"message": message,
			})
		}

		return nil
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		// no reply needed for gossip
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// write lock to write to hashset
		mu.Lock()
		message := int(body["message"].(float64))
		if _, exists := message_set[message]; exists {
			// already seen
			mu.Unlock()
			return nil
		}

		message_set[message] = struct{}{}
		mu.Unlock()

		// gossip to neighbors
		mu.RLock()
		nbs := append([]string(nil), neighbors...)
		mu.RUnlock()
		for _, neighbor := range nbs {
			if neighbor == msg.Src {
				continue
			}
			go n.Send(neighbor, map[string]any{
				"type":    "gossip",
				"message": message,
			})
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// read lock to read from hashset
		mu.RLock()
		messages := make([]int, 0, len(message_set))
		for message := range message_set {
			messages = append(messages, message)
		}
		mu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// extract neighboring nodes from topology
		topology := body["topology"].(map[string]any)
		node_id := n.ID()

		// write lock to update neighbors
		mu.Lock()
		// clear previous neighbors
		neighbors = neighbors[:0]

		// store neighboring nodes
		for _, neighbor := range topology[node_id].([]any) {
			neighbors = append(neighbors, neighbor.(string))
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
