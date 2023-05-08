# Run the 'protoc' compiler to generate the Python code.
python3 -m grpc_tools.protoc -I protos \
  --python_out=./src/protobuf \
  --grpc_python_out=./src/protobuf \
  ./protos/"${1}"