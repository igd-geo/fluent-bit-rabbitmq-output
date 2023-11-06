FROM golang:1.20-bullseye as building-stage

RUN go install github.com/fluent/fluent-bit-go/output@latest; exit 0 && \ 
    go install github.com/rabbitmq/amqp091-go@latest; exit 0
    

COPY ./*.go /go/src/
COPY ./go.mod /go/src/
COPY ./go.sum /go/src/
COPY ./Makefile /go/src

WORKDIR /go/src

RUN make

FROM fluent/fluent-bit:2.1

LABEL maintainer="Björn Franke"

COPY --from=building-stage /go/src/out_rabbitmq.so  /fluent-bit/bin/
COPY ./conf/fluent-bit-docker.conf /fluent-bit/etc

EXPOSE 2020

CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit-docker.conf","-e","/fluent-bit/bin/out_rabbitmq.so"]
