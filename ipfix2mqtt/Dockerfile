FROM golang AS builder

COPY . .

RUN go get -d -v

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -ldflags="-w -s" -o  /go/bin/ipfix2mqtt

FROM scratch

COPY --from=builder /go/bin/ipfix2mqtt /go/bin/
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

USER 1000:1000

EXPOSE 8080/tcp

ENTRYPOINT ["/go/bin/ipfix2mqtt", "--logtostderr"]