package api

import _ "embed"

//go:embed openapi.json
var OpenAPI []byte

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.0 -generate types -package api -o models.gen.go openapi.json
