FROM golang AS builder

COPY . .

RUN go get -d -v

ENV bin /go/bin/syslog2mqtt

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -ldflags="-w -s" -o  $bin

FROM scratch

COPY --from=builder $bin $bin

USER 1000:1000

EXPOSE 1514/udp
EXPOSE 8080/tcp

ENTRYPOINT ["/go/bin/syslog2mqtt", "--syslog_listen", ":1514", \
            "--logtostderr"]
