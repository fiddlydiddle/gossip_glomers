package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendPaylod struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type PollPaylod struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitPayload struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type ListCommitsPayload struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type logMessage struct {
	Offset int
	Msg    int
}

func main() {
	node := maelstrom.NewNode()
	offsetKvStore := maelstrom.NewLinKV(node)
	messageKvStore := maelstrom.NewLinKV(node)
	commitedOffsetKvStore := maelstrom.NewLinKV(node)

	node.Handle("send", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload SendPaylod
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		var finalOffset int
		var finalErr error
		if node.ID() != node.NodeIDs()[0] {
			// this node is not the leader. Forward the request to the leader
			resp, forwardErr := node.SyncRPC(context.Background(), node.NodeIDs()[0], msg.Body)
			if forwardErr != nil {
				return forwardErr
			}
			// test

			return node.Reply(msg, resp.Body)
		} else {
			// This node is the leader. Save the message to the kvStore
			for {
				// Fetch current offset from distributed store and increment
				currentOffset, err := offsetKvStore.ReadInt(context.Background(), fmt.Sprintf("%s_offset", payload.Key))
				if err != nil {
					currentOffset = 0
				}
				newOffset := currentOffset + 1

				// Write new message for the key-offset pair
				messageStoreErr := messageKvStore.Write(
					context.Background(),
					fmt.Sprintf("%s_%s", payload.Key, strconv.Itoa(newOffset)),
					payload.Msg,
				)
				if messageStoreErr != nil {
					return messageStoreErr
				}

				// Now store the new offset
				offsetStoreErr := offsetKvStore.CompareAndSwap(
					context.Background(),
					"offset",
					currentOffset,
					newOffset,
					true,
				)
				if offsetStoreErr != nil {
					if rpcErr, ok := offsetStoreErr.(*maelstrom.RPCError); ok {
						if rpcErr.Error() == "precondition_failed" {
							// CAS error, retry
							continue
						}
					}

					finalErr = offsetStoreErr
					break
				}

				finalOffset = newOffset
				break
			}
		}

		if finalErr != nil {
			return finalErr
		}

		body["type"] = "send_ok"
		body["offset"] = finalOffset
		return node.Reply(msg, body)
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload PollPaylod
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		messages := make(map[string][][2]int)
		for key, requestedOffset := range payload.Offsets {
			// Get latest offset for key
			offsetKey := fmt.Sprintf("%s_offset", key)
			latestOffset, err := offsetKvStore.ReadInt(context.Background(), offsetKey)
			if err != nil {
				// New key
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					latestOffset = 0
				} else {
					return err
				}
			}

			for offset := requestedOffset; offset <= latestOffset; offset++ {
				messageKey := fmt.Sprintf("%s:%d", key, offset)

				messageValue, readErr := messageKvStore.ReadInt(context.Background(), messageKey)

				if readErr != nil {
					if maelstrom.ErrorCode(readErr) == maelstrom.KeyDoesNotExist {
						break
					}
					return readErr
				}

				messages[key] = append(messages[key], [2]int{offset, messageValue})
			}
		}

		body["msgs"] = messages
		body["type"] = "poll_ok"

		return node.Reply(msg, body)
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload CommitPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		// Write each key/offset pair to the store
		for key, offset := range payload.Offsets {
			currentCommitted, err := commitedOffsetKvStore.ReadInt(context.Background(), key)
			if err != nil {
				currentCommitted = 0
			}
			_ = commitedOffsetKvStore.CompareAndSwap(context.Background(), key, currentCommitted, offset, true)
		}

		body["type"] = "commit_offsets_ok"
		return node.Reply(msg, body)
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload ListCommitsPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		// Read committed offset for each requested key
		result := make(map[string]int)
		for _, key := range payload.Keys {
			currentCommitted, err := commitedOffsetKvStore.ReadInt(context.Background(), key)
			if err != nil {
				currentCommitted = 0
			}
			result[key] = currentCommitted
		}

		body["type"] = "list_committed_offsets_ok"
		body["offsets"] = result
		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
