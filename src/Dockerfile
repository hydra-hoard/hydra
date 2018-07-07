FROM karanchahal/go-protoc:1.0.0

WORKDIR /go/src/
COPY . .

RUN go get -d -v ./...
RUN go get -u github.com/golang/protobuf/protoc-gen-go
RUN sh build.sh

CMD ["go","test","hydra-dht/dht/"]