package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	msg_lookup = make(map[int]struct{}) // hashset to track seen messages
	msg_data   = make([]int, 0)
	neighbors  = make([]string, 0)
	mu         sync.RWMutex
)

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

			mu.RLock()
			msgs := append([]int(nil), msg_data...)
			mu.RUnlock()

			if len(msgs) == 0 {
				continue
			}

			for _, nb := range nbs {
				go n.Send(nb, map[string]any{
					"type":     "gossip",
					"messages": msgs,
				})
			}
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// write lock
		mu.Lock()
		_, exists := msg_lookup[body.Message]
		if !exists {
			msg_data = append(msg_data, body.Message)
			msg_lookup[body.Message] = struct{}{}
		}
		mu.Unlock()

		// gossip to neighbors
		if !exists {
			mu.RLock()
			nbs := append([]string(nil), neighbors...)
			mu.RUnlock()
			for _, nb := range nbs {
				go n.Send(nb, map[string]any{
					"type":     "gossip",
					"messages": []int{body.Message},
				})
			}
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body GossipMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// write lock
		mu.Lock()
		newMsgs := make([]int, 0)
		for _, message := range body.Messages {
			if _, exists := msg_lookup[message]; !exists {
				msg_lookup[message] = struct{}{}
				msg_data = append(msg_data, message)
				newMsgs = append(newMsgs, message)
			}
		}
		mu.Unlock()

		// gossip to neighbors
		if len(newMsgs) > 0 {
			mu.RLock()
			nbs := append([]string(nil), neighbors...)
			mu.RUnlock()
			for _, nb := range nbs {
				if nb == msg.Src {
					continue
				}
				go n.Send(nb, map[string]any{
					"type":     "gossip",
					"messages": newMsgs,
				})
			}
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body ReadMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// read lock
		mu.RLock()
		messages := append([]int(nil), msg_data...)
		mu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyMessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// use a star topology with n0 as the hub
		mu.Lock()
		allNodes := n.NodeIDs()
		hub := allNodes[0]
		if n.ID() == hub {
			for _, node := range allNodes {
				if node != hub {
					neighbors = append(neighbors, node)
				}
			}
		} else {
			neighbors = []string{hub}
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

type TopologyMessageBody struct {
	maelstrom.MessageBody
	Topology map[string][]string `json:"topology"`
}

type BroadcastMessageBody struct {
	maelstrom.MessageBody
	Message int `json:"message"`
}

type GossipMessageBody struct {
	maelstrom.MessageBody
	Messages []int `json:"messages"`
}

type ReadMessageBody struct {
	maelstrom.MessageBody
	Messages []int `json:"messages"`
}
