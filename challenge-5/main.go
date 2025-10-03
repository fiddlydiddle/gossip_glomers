package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

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

// //////////////////////////////////////////////
// LogMessage struct and toString() method
// //////////////////////////////////////////////
type LogMessage struct {
	Offset int
	Msg    int
}

func (logMessage LogMessage) toString() string {
	return fmt.Sprintf("%d:%d", logMessage.Offset, logMessage.Msg)
}

////////////////////////////////////////////////
// End LogMessage struct and toString() method
////////////////////////////////////////////////

// /////////////////////////////////////////////////
// LogMessages slice wrapper stuct and methods
// ////////////////////////////////////////////////
type LogMessages struct {
	messages []LogMessage
}

func (logMessages LogMessages) toString() string {
	result := make([]string, len(logMessages.messages))
	for i := 0; i < len(logMessages.messages); i++ {
		result[i] = logMessages.messages[i].toString()
	}
	return strings.Join(result, ",")
}

func (logMessages *LogMessages) append(logMessage LogMessage) {
	logMessages.messages = append(logMessages.messages, logMessage)
}

///////////////////////////////////////////////////
// End LogMessages slice wrapper stuct and methods
//////////////////////////////////////////////////

// ///////////////////////////////////////////////////
// server struct and helper methods
// ///////////////////////////////////////////////////
type Server struct {
	node       *maelstrom.Node
	kvStore    *maelstrom.KV
	topicLocks map[string]*sync.Mutex
	lockMapMu  sync.Mutex
}

func (server *Server) getTopicLock(key string) *sync.Mutex {
	server.lockMapMu.Lock()
	defer server.lockMapMu.Unlock()

	mu, ok := server.topicLocks[key]
	if !ok {
		mu = &sync.Mutex{}
		server.topicLocks[key] = mu
	}
	return mu
}

/////////////////////////////////////////////////////
// End server struct and helper methods
/////////////////////////////////////////////////////

func main() {
	node := maelstrom.NewNode()
	kvStore := maelstrom.NewLinKV(node)
	server := &Server{
		node:       node,
		kvStore:    kvStore,
		topicLocks: make(map[string]*sync.Mutex),
	}

	node.Handle("send", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		var payload SendPaylod
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		var finalOffset int
		var finalErr error

		topicMutex := server.getTopicLock(payload.Key)
		topicMutex.Lock()
		defer topicMutex.Unlock()
		for {
			// Fetch current offset from distributed store and increment
			offsetKey := payload.Key + "_offset"
			currentOffset, err := kvStore.ReadInt(context.Background(), offsetKey)
			if err != nil {
				currentOffset = 0
			}
			newOffset := currentOffset + 1

			// Fetch current messages from distributed store for key
			var logMessages LogMessages
			existingRawValue, messageReadErr := kvStore.Read(context.Background(), payload.Key)
			if messageReadErr != nil {
				if maelstrom.ErrorCode(messageReadErr) == maelstrom.KeyDoesNotExist {
					// Key doesn't exist yet
					existingRawValue = nil
				} else {
					return messageReadErr
				}
			}

			if existingRawValue != nil {
				logMessages, err = serializeToLogMessages(existingRawValue.(string))
				if err != nil {
					return err
				}
			}

			// Add new message and save to distributed store
			logMessages.append(LogMessage{
				Offset: newOffset,
				Msg:    payload.Msg,
			})
			newRawValue := logMessages.toString()
			messageStoreErr := kvStore.Write(
				context.Background(),
				payload.Key,
				newRawValue,
			)

			if messageStoreErr != nil {
				if rpcErr, ok := messageStoreErr.(*maelstrom.RPCError); ok && rpcErr.Code == maelstrom.PreconditionFailed {
					// Log CAS failed, retry
					continue
				}
				break
			}

			// store the new offset
			offsetStoreErr := kvStore.Write(
				context.Background(),
				offsetKey,
				newOffset,
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
			var logMessages LogMessages
			existingRawValue, messageReadErr := kvStore.Read(context.Background(), key)
			if messageReadErr != nil {
				if maelstrom.ErrorCode(messageReadErr) == maelstrom.KeyDoesNotExist {
					// Key doesn't exist yet
					existingRawValue = nil
				} else {
					return messageReadErr
				}
			}

			if existingRawValue != nil {
				logMessages, messageReadErr = serializeToLogMessages(existingRawValue.(string))
				if messageReadErr != nil {
					return messageReadErr
				}
			}

			var startIdx int = findOffset(logMessages, requestedOffset)

			if startIdx != -1 {
				for _, logMessage := range logMessages.messages[startIdx:] {
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

func findOffset(logMessages LogMessages, startingOffset int) int {
	left := 0
	right := len(logMessages.messages) - 1

	if len(logMessages.messages) == 0 || startingOffset <= logMessages.messages[0].Offset {
		return 0
	}

	// Optimization: If the requested offset is past the last log entry, return the length
	if startingOffset > logMessages.messages[right].Offset {
		return len(logMessages.messages)
	}

	// Binary search logic
	for left <= right {
		mid := left + (right-left)/2
		midOffset := logMessages.messages[mid].Offset

		if midOffset == startingOffset {
			return mid
		}

		if midOffset < startingOffset {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return left
}

// serializeToLogMessages(s string) ([]logMessage, error)
// Parses the stored comma-separated string back into a slice of logMessage structs.
func serializeToLogMessages(str string) (LogMessages, error) {
	if str == "" {
		return LogMessages{messages: nil}, nil
	}

	split := strings.Split(str, ",")
	messages := make([]LogMessage, 0, len(split))
	for _, val := range split {
		idx := strings.Index(val, ":")
		if idx == -1 {
			continue // Skip malformed entry
		}

		offset, err := strconv.Atoi(val[:idx])
		if err != nil {
			return LogMessages{messages: nil}, fmt.Errorf("failed to parse offset: %w", err)
		}
		message, err := strconv.Atoi(val[idx+1:])
		if err != nil {
			return LogMessages{messages: nil}, fmt.Errorf("failed to parse message: %w", err)
		}
		messages = append(messages, LogMessage{
			Offset: offset,
			Msg:    message,
		})
	}
	return LogMessages{messages: messages}, nil
}
