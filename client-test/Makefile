
NS ?= scalarlabs
IMAGE_NAME ?= kelpie-test-client
VERSION ?= latest

.PHONY: build push

build:
	gradle shadowJar
	docker build -t $(NS)/$(IMAGE_NAME):$(VERSION) .

push:
	docker push $(NS)/$(IMAGE_NAME):$(VERSION)