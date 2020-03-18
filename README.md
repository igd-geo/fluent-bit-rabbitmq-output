# fluent-bit-rabbitmq-output

With this fluent-bit-plugin it is possible to send logs and metrics, which have been collected by fluent-bit, to a rabbitmq-exchange. Furthermore it is possible with this plugin to to store data fields from each collected log or metric into the routing-key .


## Build

Run the following command to build the plugin:
``` bash
build
```

## Configuration

### Configuration-Parameter

| Parameter                    | Description                                                                                     | Default-Value |
|------------------------------|-------------------------------------------------------------------------------------------------|---------------|
| RabbitHost                   | The hostname of the Rabbit-MQ server                                                            | ""            |
| RabbitPort                   | The port under which the Rabbit-MQ is reachable                                                 | ""            |
| RabbitUser                   | The user of the Rabbit-MQ host                                                                  | ""            |
| RabbitPassword               | The username of the user which connects to the Rabbit-MQ server                                 | ""            |
| ExchangeName                 | The exchange to which fluent-bit send its logs                                                  | ""            |
| ExchangeType                 | The exchange-type                                                                               | ""            |
| RoutingKey                   | The routing-key pattern                                                                         | ""            |
| RoutingKeyDelimiter          | The Delemiter which seperates the routing-key parts                                             |  "."          |
| RemoveRkValuesFromRecord     | If enabled fluentd deletes the values of the record,  which have been stored in the routing-key | ""            |

### Routing-Key pattern

You can access values from each record an store them into the routing-key, by specifying a record-accessor.

#### Example Record-Accessor
``` conf
$['key1'][0]["key2"]
```

The routing-key parts are delimited by the `RoutingKeyDelimiter`. If a string in one of the record-accesors containes the delimiter the plugin will not work as expected.


#### Example Routing-key

``` conf
$["loglevel_3"].$["loglevel_1"][2]["sublevel"][0].$["loglevel_2"]["info_loglevel"]
```

This functionality has been implemented with the [Record Accessor - Plugin Helper](https://docs.fluentd.org/plugin-helper-overview/api-plugin-helper-record_accessor)



## Run
If you want to run the plugin with the example config in `/conf`, you need to run the following command:
```bash
fluent-bit -e ./out_rabbitmq.so -c conf/fluent-bit-docker.conf
```