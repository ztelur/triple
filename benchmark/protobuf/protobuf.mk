.PHONY: compile
PROTOC_GEN_GO := $(GOPATH)/bin/protoc-gen-go
PROTOC := $(shell which protoc)
ifeq ($(PROTOC),)
	PROTOC = must-rebuild
endif

UNAME := $(shell uname)

$(PROTOC):
ifeq ($(UNAME), Darwin)
	brew install protobuf
endif
ifeq ($(UNAME), Linux)
	sudo apt-get install protobuf-compiler
endif

$(PROTOC_GEN_GO):
	go get -u github.com/dubbo-go/protoc-gen-dubbo3
service.pb.go: service.proto | $(PROTOC_GEN_GO) $(PROTOC)
	protoc -I . service.proto --dubbo3_out=plugins=grpc+dubbo:.

.PHONY: compile
compile: service.pb.go

