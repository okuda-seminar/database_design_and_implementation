test:
	go test -cover ./... -gcflags="all=-N -l" -v -coverprofile=cover.out
	go tool cover -html=cover.out