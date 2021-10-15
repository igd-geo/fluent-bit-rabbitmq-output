package main

import (
	"C"
	"encoding/json"
	"log"
	"strconv"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/streadway/amqp"
)

var (
	connection               *amqp.Connection
	channel                  *amqp.Channel
	exchangeName             string
	routingKey               string
	routingKeyDelimiter      string
	removeRkValuesFromRecord bool
	addTagToRecord           bool
	addTimestampToRecord     bool
	amqps					 bool
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
	exchangeName = output.FLBPluginConfigKey(plugin, "ExchangeName")
	exchangeType := output.FLBPluginConfigKey(plugin, "ExchangeType")
	routingKey = output.FLBPluginConfigKey(plugin, "RoutingKey")
	routingKeyDelimiter = output.FLBPluginConfigKey(plugin, "RoutingKeyDelimiter")
	removeRkValuesFromRecordStr := output.FLBPluginConfigKey(plugin, "RemoveRkValuesFromRecord")
	addTagToRecordStr := output.FLBPluginConfigKey(plugin, "AddTagToRecord")
	addTimestampToRecordStr := output.FLBPluginConfigKey(plugin, "AddTimestampToRecord")
	amqpsStr := output.FLBPluginConfigKey(plugin, "AMQPS")

	var urlPrefix = "amqp"

	if len(routingKeyDelimiter) < 1 {
		routingKeyDelimiter = "."
		logInfo("The routing-key-delimiter is set to the default value '" + routingKeyDelimiter + "' ")
	}

	removeRkValuesFromRecord, err = strconv.ParseBool(removeRkValuesFromRecordStr)
	if err != nil {
		logError("Couldn't parse RemoveRkValuesFromRecord to boolean: ", err)
		return output.FLB_ERROR
	}

	addTagToRecord, err = strconv.ParseBool(addTagToRecordStr)
	if err != nil {
		logError("Couldn't parse AddTagToRecord to boolean: ", err)
		return output.FLB_ERROR
	}

	addTimestampToRecord, err = strconv.ParseBool(addTimestampToRecordStr)
	if err != nil {
		logError("Couldn't parse AddTimestampToRecord to boolean: ", err)
		return output.FLB_ERROR
	}

	err = RoutingKeyIsValid(routingKey, routingKeyDelimiter)
	if err != nil {
		logError("The Parsing of the Routing-Key failed: ", err)
		return output.FLB_ERROR
	}

	amqps, err = strconv.ParseBool(amqpsStr)
	if len(amqpsStr) == 0 {
		logInfo("The AMQPS value was not, using default value of 'false', amqp protocol")
	}
	if err != nil {
		logInfo("Couldn't parse amqps to boolean, using amqp")
	}
	if err == nil && amqps {
		urlPrefix = "amqps"
	}

	connection, err = amqp.Dial(urlPrefix + "://" + user + ":" + password + "@" + host + ":" + port + "/")
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
		exchangeName, // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
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

		parsedRecord := ParseRecord(record)

		if addTagToRecord {
			parsedRecord["@tag"] = C.GoString(tag)
		}
		if addTimestampToRecord {
			parsedRecord["@timestamp"] = timestamp.String()
		}

		rk, err := CreateRoutingKey(routingKey, &parsedRecord, routingKeyDelimiter)
		if err != nil {
			logError("Couldn't create the Routing-Key", err)
			continue
		}

		jsonString, err := json.Marshal(parsedRecord)

		if err != nil {
			logError("Couldn't parse record: ", err)
			continue
		}

		err = channel.Publish(
			exchangeName, // exchange
			rk,           // routing key
			false,        // mandatory
			false,        // immediate
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
