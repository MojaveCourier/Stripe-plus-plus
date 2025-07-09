cd project/src/proto
/users/qiliang/Stripe-plus-plus/project/third_party/grpc/bin/protoc --proto_path=. --grpc_out=. --plugin=protoc-gen-grpc=./../../third_party/grpc/bin/grpc_cpp_plugin coordinator.proto
/users/qiliang/Stripe-plus-plus/project/third_party/grpc/bin/protoc --proto_path=. --cpp_out=. coordinator.proto
/users/qiliang/Stripe-plus-plus/project/third_party/grpc/bin/protoc --proto_path=. --grpc_out=. --plugin=protoc-gen-grpc=./../../third_party/grpc/bin/grpc_cpp_plugin proxy.proto
/users/qiliang/Stripe-plus-plus/project/third_party/grpc/bin/protoc --proto_path=. --cpp_out=. proxy.proto
/users/qiliang/Stripe-plus-plus/project/third_party/grpc/bin/protoc --proto_path=. --grpc_out=. --plugin=protoc-gen-grpc=./../../third_party/grpc/bin/grpc_cpp_plugin datanode.proto
/users/qiliang/Stripe-plus-plus/project/third_party/grpc/bin/protoc --proto_path=. --cpp_out=. datanode.proto