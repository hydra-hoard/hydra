FROM karanchahal/go-protoc:1.0.0

WORKDIR /go/src/hydra-dht
COPY . .

RUN sh build.sh

CMD ["go","test","hydra-dht/dht/"]