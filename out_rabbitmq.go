package main

import (
	"C"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/streadway/amqp"
)
import "encoding/json"

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
	host := output.FLBPluginConfigKey(plugin, "RabbitHost")
	port := output.FLBPluginConfigKey(plugin, "RabbitPort")
	user := output.FLBPluginConfigKey(plugin, "RabbitUser")
	password := output.FLBPluginConfigKey(plugin, "RabbitPassword")
	tn := output.FLBPluginConfigKey(plugin, "TopicName")
	topicType := output.FLBPluginConfigKey(plugin, "TopicType")
	rk := output.FLBPluginConfigKey(plugin, "RoutingKey")

	conn, err := amqp.Dial("amqp://" + user + ":" + password + "@" + host + ":" + port + "/")
	if err != nil {
		logError("Failed to establish a connection to RabbitMQ: ", err)
		return output.FLB_ERROR
	}

	ch, err := conn.Channel()
	if err != nil {
		logError("Failed to open a channel: ", err)
		conn.Close()
		return output.FLB_ERROR
	}

	logInfo("Established successfully a connection to the RabbitMQ-Server")

	err = ch.ExchangeDeclare(
		tn,        // name
		topicType, // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		logError("Failed to declare an exchange: ", err)
		conn.Close()
		return output.FLB_ERROR
	}

	connection = conn
	channel = ch
	topicName = tn
	routingKey = rk
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

		parsedRecord := parseMap(record)

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

func parseMap(mapInterface map[interface{}]interface{}) map[string]interface{} {
	parsedMap := make(map[string]interface{})
	for k, v := range mapInterface {
		switch t := v.(type) {
		case []byte:
			// prevent encoding to base64
			parsedMap[k.(string)] = string(t)
		case map[interface{}]interface{}:
			parsedMap[k.(string)] = parseMap(t)
		default:
			parsedMap[k.(string)] = v
		}
	}
	return parsedMap
}

func logInfo(msg string) {
	log.Printf("%s", msg)
}

func logError(msg string, err error) {
	log.Printf("%s: %s", msg, err)
}
