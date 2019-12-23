package main

import (
	"C"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/streadway/amqp"
)
import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	connection *amqp.Connection
	channel    *amqp.Channel
	topicName  string
	routingKey string
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

	err = routingKeyIsValid(routingKey)
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
		println(timestamp.String())

		parsedRecord := parseRecord(record)

		parsedRecord["@timestamp"] = timestamp.String()
		parsedRecord["@tag"] = C.GoString(tag)

		jsonString, err := json.Marshal(parsedRecord)
		if err != nil {
			logError("Couldn't parse record: ", err)
			continue
		}

		err = channel.Publish(
			topicName,  // exchange
			routingKey, // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonString,
			})
		if err != nil {
			logError("Couldn't publish record: ", err)
		}
		println(string(jsonString))
	}
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
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
		default:
			parsedMap[k.(string)] = v
		}
	}
	return parsedMap
}

func routingKeyIsValid(rk string) error {
	r, err := regexp.Compile(`^\$((\[\"([^\s\"]+)\"\])|(\[\'([^\s\']+)\'\])|(\[[1-9][0-9]*\])|(\[[0]\]))+$`)
	if err != nil {
		return err
	}

	if len(rk) <= 0 {
		return errors.New("Routing-Key shouldn't be empty")
	}

	if strings.Contains(rk, ".") {
		splittedRk := strings.Split(rk, ".")
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

func createRoutingKey(record map[string]interface{}) {

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
