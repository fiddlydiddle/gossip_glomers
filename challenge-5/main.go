package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendPaylod struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type BroadcastCounterPayload struct {
	Type    string `json:"type"`
	Counter int    `json:"counter"`
}

type Message struct {
	Counter int
	Msg     int
}

func main() {
	node := maelstrom.NewNode()
	var mutex sync.Mutex
	kvStore := maelstrom.NewSeqKV(node)

	node.Handle("send", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload SendPaylod
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		for {
			mutex.Lock()
			currentCounter, err := kvStore.ReadInt(context.Background(), "counter")
			if err != nil {
				currentCounter = 0
			}

			newCounter := currentCounter + 1
			newMessage := Message{Counter: newCounter, Msg: payload.Msg}
			storeMsgErr := kvStore.Write(context.Background(), "messages", newMessage)
			if storeMsgErr != nil {
				return storeMsgErr
			}
			storeCounterErr := kvStore.CompareAndSwap(context.Background(), "counter", currentCounter, newCounter, true)
			mutex.Unlock()

			if storeCounterErr != nil {
				return storeCounterErr
			} else {
				go broadcastCounter(node, newCounter)
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

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload BroadcastCounterPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		for {
			mutex.Lock()
			currentCounter, err := kvStore.ReadInt(context.Background(), "counter")
			if err != nil {
				currentCounter = 0
			}

			if payload.Counter <= currentCounter {
				mutex.Unlock()
				break
			}

			storeErr := kvStore.CompareAndSwap(context.Background(), "counter", currentCounter, payload.Counter, true)
			mutex.Unlock()

			if storeErr != nil {
				return storeErr
			} else {
				break
			}
		}

		body["type"] = "broadcast_ok"
		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func broadcastCounter(node *maelstrom.Node, counter int) {
	nodeIds := node.NodeIDs()
	for _, nodeId := range nodeIds {
		if nodeId == node.ID() {
			continue
		}
		node.RPC(nodeId, BroadcastCounterPayload{
			Type:    "broadcast",
			Counter: counter,
		}, func(msg maelstrom.Message) error {
			return nil
		})
	}
}
