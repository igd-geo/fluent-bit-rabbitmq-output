
all:
	go build -buildmode=c-shared -o out_rabbitmq.so out_rabbitmq.go routing_key_validator.go routing_key_creator.go record_parser.go

clean:
	rm -rf *.so *.h *~