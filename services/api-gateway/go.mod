module github.com/amirzre/payment-system/services/api-gateway

go 1.26.5

require (
	github.com/amirzre/payment-system/gen/go v0.0.0-00010101000000-000000000000
	github.com/amirzre/payment-system/shared v0.0.0-00010101000000-000000000000
	github.com/joho/godotenv v1.5.1
	google.golang.org/grpc v1.82.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/redis/go-redis/v9 v9.21.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace (
	github.com/amirzre/payment-system/gen/go => ../../gen/go
	github.com/amirzre/payment-system/shared => ../../shared
)
