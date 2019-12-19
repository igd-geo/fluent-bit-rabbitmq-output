
all:
	go build -buildmode=c-shared -o out_rabbitmq.so out_rabbitmq.go

clean:
	rm -rf *.so *.h *~