package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Generate a new UUID.
		newUUID := uuid.New().String()

		// Create the response body with the generated UUID.
		response := map[string]any{
			"type": "generate_ok",
			"id":   newUUID,
		}

		// Reply with the response containing the UUID.
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
