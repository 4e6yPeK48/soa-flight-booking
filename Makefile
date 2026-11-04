GOBIN ?= $(CURDIR)/bin
export GOBIN
export PATH := $(GOBIN):$(PATH)

.PHONY: tools generate check up down logs integration e2e e2e-report e2e-open

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
	go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.0

generate: tools
	@test "$$(protoc --version)" = 'libprotoc 36.2' || (echo 'Install protoc 36.2 before generating'; exit 1)
	protoc -I proto --go_out=. --go_opt=module=github.com/mchupryna/soa-flight-booking --go-grpc_out=. --go-grpc_opt=module=github.com/mchupryna/soa-flight-booking proto/flight/v1/flight.proto
	go generate ./internal/api

check: generate
	git diff --exit-code -- gen internal/api/models.gen.go
	go test -race ./...
	go vet ./...

up:
	docker compose up -d --build --wait --wait-timeout 180

down:
	docker compose down --remove-orphans

logs:
	docker compose logs --follow

integration:
	docker compose run --build --rm integration

e2e:
	sh scripts/run-e2e.sh

e2e-report:
	npm run e2e:report

e2e-open:
	npm run e2e:open
