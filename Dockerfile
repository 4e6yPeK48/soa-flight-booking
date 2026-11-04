FROM golang:1.26-alpine3.23 AS dev
RUN apk add --no-cache ca-certificates curl unzip gcc musl-dev docker-cli make
ARG PROTOC_VERSION=36.2
RUN case "$(uname -m)" in x86_64) arch=x86_64 ;; aarch64) arch=aarch_64 ;; *) exit 1 ;; esac \
    && curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${arch}.zip" -o /tmp/protoc.zip \
    && unzip -q /tmp/protoc.zip -d /usr/local \
    && rm /tmp/protoc.zip
WORKDIR /app
ENV GOBIN=/go/bin
COPY go.mod go.sum ./
RUN go mod download
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2 \
    && go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.0
COPY . .
RUN make generate

FROM dev AS build
RUN CGO_ENABLED=0 go build -trimpath -o /out/flight-service ./cmd/flight-service \
    && CGO_ENABLED=0 go build -trimpath -o /out/booking-service ./cmd/booking-service

FROM alpine:3.23 AS runtime
RUN apk add --no-cache ca-certificates && addgroup -S app && adduser -S -G app app
COPY --from=build /out/ /usr/local/bin/
USER app
CMD ["booking-service"]

FROM --platform=linux/amd64 redgate/flyway:11-alpine AS flight-migrate
COPY migrations/flight /flyway/sql

FROM --platform=linux/amd64 redgate/flyway:11-alpine AS booking-migrate
COPY migrations/booking /flyway/sql
