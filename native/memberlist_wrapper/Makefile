BLOBS_LINUX   = ../../blobs/linux
BLOBS_MACOS   = ../../blobs/macos

.PHONY: linux macos tidy

tidy:
	go mod tidy

linux: tidy
	mkdir -p $(BLOBS_LINUX)
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
	  go build -buildmode=c-shared -o $(BLOBS_LINUX)/libgossiper.so .
	@echo "Done: $(BLOBS_LINUX)/libgossiper.so"

macos: tidy
	mkdir -p $(BLOBS_MACOS)
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 \
	  go build -buildmode=c-shared -o $(BLOBS_MACOS)/libgossiper.dylib .
	@echo "Done: $(BLOBS_MACOS)/libgossiper.dylib"
