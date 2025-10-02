package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddPayload struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type BroadcastCounterPayload struct {
	Type    string `json:"type"`
	Counter int    `json:"counter"`
}

func main() {
	node := maelstrom.NewNode()
	var mutex sync.Mutex
	kvStore := maelstrom.NewSeqKV(node)

	node.Handle("init", func(msg maelstrom.Message) error {
		kvStore.Write(context.Background(), "gcounter", 0)
		kvStore.Write(context.Background(), node.ID(), 0)
		return node.Reply(msg, map[string]any{"type": "init_ok"})
	})

	node.Handle("add", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload AddPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		for {
			mutex.Lock()
			currentCounter, err := kvStore.ReadInt(context.Background(), "counter")
			if err != nil {
				currentCounter = 0
			}

			newCounter := currentCounter + payload.Delta

			storeErr := kvStore.CompareAndSwap(context.Background(), "counter", currentCounter, newCounter, true)
			mutex.Unlock()

			if storeErr != nil {
				return storeErr
			} else {
				break
			}
		}

		body["type"] = "add_ok"
		return node.Reply(msg, body)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mutex.Lock()
		counter, err := kvStore.ReadInt(context.Background(), "counter")
		if err != nil {
			counter = 0
		}
		mutex.Unlock()

		body["value"] = counter
		body["type"] = "read_ok"

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
