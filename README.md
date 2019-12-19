# fluent-bit-rabbitmq-output

## Build

Run the following command to build the plugin:
``` bash
build
```

## Run
If you want to run the plugin with the example config in `/conf`, you need to run the following command:
```bash
fluent-bit -e ./out_rabbitmq.so -c conf/fluent-bit-docker.conf
```