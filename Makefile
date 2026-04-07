GRPC_OUT = grpc_generated

.PHONY: proto

proto:
	uv run python -m grpc_tools.protoc \
		-I./protos \
		--python_out=./$(GRPC_OUT) \
		--grpc_python_out=./$(GRPC_OUT) \
		./protos/worker.proto
	@sed -i 's/^import worker_pb2 as/from grpc_generated import worker_pb2 as/' $(GRPC_OUT)/worker_pb2_grpc.py
	@echo "Worker gRPC stubs generated in $(GRPC_OUT)"
