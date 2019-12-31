package main

import (
	"C"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/streadway/amqp"
)

var (
	connection          *amqp.Connection
	channel             *amqp.Channel
	topicName           string
	routingKey          string
	routingKeyDelimiter string
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	// Gets called only once when the plugin.so is loaded
	return output.FLBPluginRegister(def, "rabbitmq", "Stdout GO!")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	// Gets called only once for each instance you have configured.
	var err error
	host := output.FLBPluginConfigKey(plugin, "RabbitHost")
	port := output.FLBPluginConfigKey(plugin, "RabbitPort")
	user := output.FLBPluginConfigKey(plugin, "RabbitUser")
	password := output.FLBPluginConfigKey(plugin, "RabbitPassword")
	topicName = output.FLBPluginConfigKey(plugin, "TopicName")
	topicType := output.FLBPluginConfigKey(plugin, "TopicType")
	routingKey = output.FLBPluginConfigKey(plugin, "RoutingKey")
	routingKeyDelimiter = output.FLBPluginConfigKey(plugin, "RoutingKeyDelimiter")

	if len(routingKeyDelimiter) < 1 {
		routingKeyDelimiter = "."
		logInfo("The routing-key-delimiter is set to the default value '" + routingKeyDelimiter + "' ")
	}

	err = routingKeyIsValid(routingKey, routingKeyDelimiter)
	if err != nil {
		logError("The Parsing of the Routing-Key failed: ", err)
		return output.FLB_ERROR
	}

	connection, err = amqp.Dial("amqp://" + user + ":" + password + "@" + host + ":" + port + "/")
	if err != nil {
		logError("Failed to establish a connection to RabbitMQ: ", err)
		return output.FLB_ERROR
	}

	channel, err = connection.Channel()
	if err != nil {
		logError("Failed to open a channel: ", err)
		connection.Close()
		return output.FLB_ERROR
	}

	logInfo("Established successfully a connection to the RabbitMQ-Server")

	err = channel.ExchangeDeclare(
		topicName, // name
		topicType, // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		logError("Failed to declare an exchange: ", err)
		connection.Close()
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Gets called with a batch of records to be written to an instance.
	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	// Iterate Records
	for {
		// Extract Record
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		timestamp := ts.(output.FLBTime)

		parsedRecord := parseRecord(record)

		parsedRecord["@timestamp"] = timestamp.String()
		parsedRecord["@tag"] = C.GoString(tag)

		jsonString, err := json.Marshal(parsedRecord)
		if err != nil {
			logError("Couldn't parse record: ", err)
			continue
		}

		rk, err := createRoutingKey(routingKey, parsedRecord, routingKeyDelimiter)
		if err != nil {
			logError("Couldn't create the Routing-Key", err)
			continue
		}

		err = channel.Publish(
			topicName, // exchange
			rk,        // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonString,
			})
		if err != nil {
			logError("Couldn't publish record: ", err)
		}
	}
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func parseRecord(mapInterface map[interface{}]interface{}) map[string]interface{} {
	parsedMap := make(map[string]interface{})
	for k, v := range mapInterface {
		switch t := v.(type) {
		case []byte:
			// prevent encoding to base64
			parsedMap[k.(string)] = string(t)
		case map[interface{}]interface{}:
			parsedMap[k.(string)] = parseRecord(t)
		case []interface{}:
			parsedMap[k.(string)] = parseSubRecordArray(t)
		default:
			parsedMap[k.(string)] = v
		}
	}
	return parsedMap
}

func parseSubRecordArray(arr []interface{}) []interface{} {
	for idx, i := range arr {
		switch t := i.(type) {
		case []byte:
			arr[idx] = string(t)
		case map[interface{}]interface{}:
			arr[idx] = parseRecord(t)
		case []interface{}:
			arr[idx] = parseSubRecordArray(t)
		default:
			arr[idx] = t
		}
	}
	return arr
}

func routingKeyIsValid(rk string, rkDelimiter string) error {
	r, err := regexp.Compile(`^\$((\[\"([^\s\"]+)\"\])|(\[\'([^\s\']+)\'\])|(\[[1-9][0-9]*\])|(\[[0]\]))+$`)
	if err != nil {
		return err
	}

	if len(rk) <= 0 {
		return errors.New("Routing-Key shouldn't be empty")
	}

	if strings.Contains(rk, rkDelimiter) {
		splittedRk := strings.Split(rk, rkDelimiter)
		if arrayContainsString(splittedRk, "") {
			return errors.New("The given routing-key contains an empty value")
		}
		for _, subRk := range splittedRk {
			if strings.HasPrefix(subRk, "$") {
				if !r.MatchString(subRk) {
					return fmt.Errorf("The record_accessor '%s' is invalid", rk)
				}
			}
		}
	} else {
		if strings.HasPrefix(rk, "$") {
			if !r.MatchString(rk) {
				return fmt.Errorf("The record_accessor '%s' is invalid", rk)
			}
		}
	}
	return nil
}

func createRoutingKey(rk string, record map[string]interface{}, rkDelimiter string) (string, error) {
	recordAccessorRegex, err := regexp.Compile(`(\'[^\s\']+\')|(\"[^\s\"]+\")|(\[0\])|(\[[1-9][0-9]*\])`)
	if err != nil {
		return "", err
	}

	var builder strings.Builder
	recordAccessors := strings.Split(rk, rkDelimiter)

	for idx, recordAccessor := range recordAccessors {
		if strings.HasPrefix(recordAccessor, "$") {
			subRk, err := extractValueFromRecord(record, recordAccessorRegex.FindAllString(recordAccessor, -1))
			if err != nil {
				return "", err
			}
			builder.WriteString(subRk)
		} else {
			builder.WriteString(recordAccessor)
		}

		if idx != (len(recordAccessors) - 1) {
			builder.WriteString(rkDelimiter)
		}
	}

	return builder.String(), nil
}

func extractValueFromRecord(record map[string]interface{}, keys []string) (string, error) {

	if len(keys) > 0 {
		arrKey := []rune(keys[0])
		currentKey := string(arrKey[1:(len(arrKey) - 1)])

		if strings.HasPrefix(keys[0], "[") {
			return "", fmt.Errorf("Couldn't access the Record with the array-accessor '%s', record-accessor is required", currentKey)
		}

		val, recordContainsKey := record[currentKey]
		if len(keys) == 1 {
			if recordContainsKey {
				return fmt.Sprintf("%v", val), nil
			}
			return "", fmt.Errorf("Can't access the record with the given record-accessor '%s'", currentKey)
		}

		subRecord, recordContainsSubRecord := val.(map[string]interface{})
		if recordContainsSubRecord {
			return extractValueFromRecord(subRecord, keys[1:])
		}

		recordArray, recordContainsArray := val.([]interface{})
		if recordContainsArray {
			return extractValueFromArray(recordArray, keys[1:])
		}

		return "", fmt.Errorf("Couldn't access the Record with the record-accessor '%s'", currentKey)
	}

	return "", fmt.Errorf("The given routing-key doesn't contain any values")
}

func extractValueFromArray(arr []interface{}, keys []string) (string, error) {

	if len(keys) > 0 {
		arrKey := []rune(keys[0])
		currentKey := string(arrKey[1:(len(arrKey) - 1)])
		idx, err := strconv.Atoi(currentKey)

		if err != nil {
			return "", fmt.Errorf("Couldn't parse the array-accessor '%s' to int", currentKey)
		}

		if strings.HasPrefix(keys[0], "\"") || strings.HasPrefix(keys[0], "'") {
			return "", fmt.Errorf("Couldn't access the array with the record-accessor '%s', array-accessor is required", currentKey)
		}

		if len(arr) <= idx {
			return "", fmt.Errorf("The given index '%s' exceededs the array-size", currentKey)
		}

		val := arr[idx]

		if len(keys) == 1 {
			return fmt.Sprintf("%v", val), nil
		}

		subRecord, recordContainsSubRecord := val.(map[string]interface{})
		if recordContainsSubRecord {
			return extractValueFromRecord(subRecord, keys[1:])
		}

		recordArray, recordContainsArray := val.([]interface{})
		if recordContainsArray {
			return extractValueFromArray(recordArray, keys[1:])
		}

		return "", fmt.Errorf("Couldn't access the Record with the record-accessor '%s'", currentKey)
	}

	return "", fmt.Errorf("The given routing-key doesn't contain any values")
}

func logInfo(msg string) {
	log.Printf("%s", msg)
}

func logError(msg string, err error) {
	log.Printf("%s: %s", msg, err)
}

func arrayContainsString(arr []string, str string) bool {
	for _, item := range arr {
		if item == str {
			return true
		}
	}
	return false
}

func main() {
}
