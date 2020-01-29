FROM golang:1.13  as building-stage

RUN go get github.com/fluent/fluent-bit-go/output && \ 
    go get github.com/streadway/amqp

COPY ./*.go /go/src/

COPY ./Makefile /go/src

WORKDIR /go/src

RUN make

FROM fluent/fluent-bit:1.3

LABEL maintainer="Björn Franke"

COPY --from=building-stage /go/src/out_rabbitmq.so  /fluent-bit/bin/
COPY ./conf/fluent-bit-docker.conf /fluent-bit/etc

EXPOSE 2020

CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit-docker.conf","-e","/fluent-bit/bin/out_rabbitmq.so"]
