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

	// Receive a broadcast from client or another node
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

	// Respond to read requests with all messages the node has received
	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Convert seenMessages map to slice
		mutex.Lock()
		values := make([]int, len(seenMessages))
		for key := range seenMessages {
			values = append(values, key)
		}
		mutex.Unlock()

		// Build and send message
		body["messages"] = values
		body["type"] = "read_ok"
		return node.Reply(msg, body)
	})

	// Receive the topology of the network and store it to node state
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

	// Handler for receiving the entire dataset from another node
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

// Every 200 milliseconds, have nodes "gossip" with a random neighbor
// The nodes will share their whole data set with the neighbor
// This allows nodes to keep each other up-to-date in between broadcasts (solves temporary partition issues)
func startReplication(node *maelstrom.Node, topology map[string][]string, seenMessages map[int]bool, mutex *sync.Mutex) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		// Read the neighbors from topology
		neighbors := topology[node.ID()]
		if len(neighbors) == 0 {
			continue
		}

		// Select random neighbor to which to replicate messages
		neighbor := neighbors[rand.Intn(len(neighbors))]

		// Convert seenMessages map to slice
		mutex.Lock()
		values := make([]int, len(seenMessages))
		for key := range seenMessages {
			values = append(values, key)
		}
		mutex.Unlock()

		// Build and send message
		replicateMessageBody := map[string]any{
			"type":     "replicate",
			"messages": values,
		}
		node.Send(neighbor, replicateMessageBody)
	}
}
