package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastPayload struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type TopologyPayload struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type ReplicatePayload struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func main() {
	node := maelstrom.NewNode()
	topology := make(map[string][]string)
	seenMessages := make(map[int]bool)
	var mutex sync.Mutex

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload BroadcastPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		// Check if we've seen this broadcast before
		mutex.Lock()
		if seenMessages[payload.Message] {
			mutex.Unlock()
			return nil
		}

		// Store new values
		seenMessages[payload.Message] = true
		mutex.Unlock()

		// Echo message to neighbors
		thisNode := node.ID()
		neighbors := topology[thisNode]
		for _, neighbor := range neighbors {
			if neighbor == msg.Src {
				continue
			}
			node.Send(neighbor, msg.Body)
		}

		// Respond
		body["type"] = "broadcast_ok"
		return node.Reply(msg, body)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Read the seenMessages
		mutex.Lock()
		values := make([]int, len(seenMessages))
		for key := range seenMessages {
			values = append(values, key)
		}
		mutex.Unlock()

		body["messages"] = values
		body["type"] = "read_ok"

		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload TopologyPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		topology = payload.Topology

		go startReplication(node, topology, seenMessages, &mutex)

		body["type"] = "topology_ok"

		return node.Reply(msg, body)
	})

	node.Handle("replicate", func(msg maelstrom.Message) error {
		var payload ReplicatePayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		mutex.Lock()
		defer mutex.Unlock()
		for _, message := range payload.Messages {
			seenMessages[message] = true
		}

		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

}

// Every 200 milliseconds, have a node replicate all of its messages to a random neighbor
func startReplication(node *maelstrom.Node, topology map[string][]string, seenMessages map[int]bool, mutex *sync.Mutex) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		neighbors := topology[node.ID()]
		if len(neighbors) == 0 {
			continue
		}

		// Select random neighbor to which to replicate messages
		neighbor := neighbors[rand.Intn(len(neighbors))]

		mutex.Lock()
		values := make([]int, len(seenMessages))
		for key := range seenMessages {
			values = append(values, key)
		}
		mutex.Unlock()

		replicateMessageBody := map[string]any{
			"type":     "replicate",
			"messages": values,
		}

		node.Send(neighbor, replicateMessageBody)
	}
}
