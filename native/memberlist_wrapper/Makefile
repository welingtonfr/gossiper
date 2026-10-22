BLOBS_LINUX = ../../blobs/linux

.PHONY: linux tidy

tidy:
	go mod tidy

linux: tidy
	mkdir -p $(BLOBS_LINUX)
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
	  go build -buildmode=c-shared -o $(BLOBS_LINUX)/libgossiper.so ./src
	@echo "Done: $(BLOBS_LINUX)/libgossiper.so"
