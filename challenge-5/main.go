package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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
	kvStore := maelstrom.NewLinKV(node)

	node.Handle("send", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload SendPaylod
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		var finalOffset int
		var finalErr error
		for {
			// Fetch current offset from distributed store and increment
			offsetKey := payload.Key + "_offset"
			currentOffset, err := kvStore.ReadInt(context.Background(), offsetKey)
			if err != nil {
				currentOffset = 0
			}
			newOffset := currentOffset + 1

			// Fetch current messages from distributed store for key
			var currentKeyMessages []logMessage
			existingRawValue, messageReadErr := kvStore.Read(context.Background(), payload.Key)
			if messageReadErr != nil {
				if maelstrom.ErrorCode(messageReadErr) == maelstrom.KeyDoesNotExist {
					// Key doesn't exist yet
					existingRawValue = nil
				} else {
					return messageReadErr
				}
			}

			// Serialize the kv-store data to logMessage type
			if existingRawValue != nil {
				rawJSONBytes, marshalErr := json.Marshal(existingRawValue)
				if marshalErr != nil {
					return fmt.Errorf("failed to marshal existing log for unmarshal: %w", marshalErr)
				}

				if unmarshalErr := json.Unmarshal(rawJSONBytes, &currentKeyMessages); unmarshalErr != nil {
					return fmt.Errorf("failed to unmarshal log: %w. Raw value type: %T", unmarshalErr, existingRawValue)
				}
			}

			// Add new message and save to distributed store
			newMessage := logMessage{Offset: newOffset, Msg: payload.Msg}
			newKeyMessages := append(currentKeyMessages, newMessage)
			newRawValue := newKeyMessages
			messageStoreErr := kvStore.CompareAndSwap(context.Background(),
				payload.Key, existingRawValue, newRawValue, true)

			if messageStoreErr != nil {
				if rpcErr, ok := messageStoreErr.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
					// Log CAS failed, retry
					continue
				}
				return messageStoreErr
			}

			// store the new offset
			offsetStoreErr := kvStore.CompareAndSwap(context.Background(), offsetKey, currentOffset, newOffset, true)
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
			// Fetch current messages from distributed store for key
			var currentKeyMessages []logMessage
			messageReadErr := kvStore.ReadInto(context.Background(), key, &currentKeyMessages)
			if messageReadErr != nil {
				if maelstrom.ErrorCode(messageReadErr) != maelstrom.KeyDoesNotExist {
					return messageReadErr
				}
			}

			var startIdx int = -1
			for i, logMessage := range currentKeyMessages {
				if logMessage.Offset >= requestedOffset {
					startIdx = i
					break
				}
			}

			if startIdx != -1 {
				for _, logMessage := range currentKeyMessages[startIdx:] {
					messages[key] = append(messages[key], [2]int{logMessage.Offset, logMessage.Msg})
				}
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

		for key, offset := range payload.Offsets {
			currentCommitted, err := kvStore.ReadInt(context.Background(), key)
			if err != nil {
				currentCommitted = 0
			}
			_ = kvStore.CompareAndSwap(context.Background(), key, currentCommitted, offset, true)
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

		result := make(map[string]int)
		for _, key := range payload.Keys {
			currentCommitted, err := kvStore.ReadInt(context.Background(), key)
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
