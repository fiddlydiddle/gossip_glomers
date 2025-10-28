package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Action struct {
	Type  string
	Key   int
	Value *int
}

type TransactionPayload struct {
	Type       string          `json:"type"`
	MessageId  int64           `json:"msg_id"`
	RawActions [][]interface{} `json:"txn"`
	Actions    []Action
}

func main() {
	node := maelstrom.NewNode()
	kvStore := maelstrom.NewLinKV(node)

	node.Handle("txn", func(msg maelstrom.Message) error {
		reply := make(map[string]any)
		var payload TransactionPayload
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			return err
		}

		// Read all old values from store for requested keys
		existingValues := make(map[string]interface{})
		var actions []Action
		for _, rawAction := range payload.RawActions {
			action := parseAction(rawAction)
			key := strconv.Itoa(action.Key)

			// Read existing value (needed for both read and writes)
			existingValue, readErr := kvStore.Read(context.Background(), strconv.Itoa(action.Key))
			existingValues[key] = existingValue

			if action.Type == "r" {
				if readErr != nil {
					// No such key yet, return nil value
					existingValue = nil
				} else {
					action.Value = existingValue.(*int)
				}
			}
			actions = append(actions, action)
		}

		// For write actions, write new values
		for _, action := range payload.Actions {
			if action.Type == "w" {
				key := strconv.Itoa(action.Key)

				existingValue := existingValues[key]

				// The 'to' value is the new value from the request.
				storeErr := kvStore.CompareAndSwap(
					context.Background(),
					key,
					existingValue,
					action.Value,
					true,
				)

				if storeErr != nil {
					// A write failure during any action should abort the entire transaction
					return maelstrom.NewRPCError(maelstrom.TxnConflict, "conflict during commit")
				}
			}
		}

		// Format txn back to string
		var responseTxn [][]interface{}
		for _, action := range actions {
			var val interface{}
			if action.Value != nil {
				val = *action.Value
			} else {
				val = nil
			}
			responseTxn = append(responseTxn, []interface{}{
				action.Type,
				action.Key,
				val,
			})
		}

		reply["type"] = "txn_ok"
		reply["in_reply_to"] = payload.MessageId
		reply["txn"] = responseTxn
		return node.Reply(msg, reply)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func parseAction(rawAction []interface{}) Action {
	actionType, _ := rawAction[0].(string)
	keyFloat, _ := rawAction[1].(float64)

	// Parse nullable third value
	var valPtr *int = nil
	if rawAction[2] != nil {
		valFloat, _ := rawAction[2].(float64)
		val := int(valFloat)
		valPtr = &val
	}

	return Action{
		Type:  actionType,
		Key:   int(keyFloat),
		Value: valPtr,
	}
}
