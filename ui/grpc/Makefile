grpc:
	protoc -I=../../pkg/types items.proto management.proto \
	  --js_out=import_style=commonjs+dts:. \
	  --grpc-web_out=import_style=commonjs+dts,mode=grpcwebtext:.
