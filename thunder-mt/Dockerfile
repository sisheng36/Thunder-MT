FROM golang:1.23-alpine AS builder

WORKDIR /build
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o thunder-mt .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /build/thunder-mt /usr/local/bin/thunder-mt
EXPOSE 8010
ENTRYPOINT ["thunder-mt"]
CMD ["--listen=:8010", "--piece=1M", "--buffer=50M", "--workers=10"]
