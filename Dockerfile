FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=${VERSION}" -o /thunder-mt .

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /thunder-mt /thunder-mt
EXPOSE 8010
CMD ["/thunder-mt"]
