# creating directory structure
echo 'creating directory structure'
if [ ! -d ./src ]; then
    mkdir src
fi
if [ ! -d ./pkg ]; then
    mkdir pkg
fi
if [ ! -d ./bin ]; then
    mkdir bin
fi

echo 'Configuring environment variables to point to this project'

# Configuring GO PATH to this project 
export GOPATH=$PWD
export PATH=$PATH:$(go env GOPATH)/bin

echo 'Generating Protobuf Artifacts'
cd src/protobuf
sh proto.sh