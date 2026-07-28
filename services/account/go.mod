module github.com/amirzre/payment-system/services/account

go 1.26.5

require (
	github.com/amirzre/payment-system/gen/go v0.0.0-00010101000000-000000000000
	github.com/amirzre/payment-system/shared v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.10.0
	github.com/joho/godotenv v1.5.1
	github.com/redis/go-redis v6.15.9+incompatible
	github.com/redis/go-redis/v9 v9.21.0
	google.golang.org/grpc v1.82.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-redis/redis v6.15.9+incompatible // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.42.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace (
	github.com/amirzre/payment-system/gen/go => ../../gen/go
	github.com/amirzre/payment-system/shared => ../../shared
)
