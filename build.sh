make clean
cd src/launch/ &&
protoc -I. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` launch.proto &&
protoc -I. --cpp_out=. launch.proto &&
python -m grpc_tools.protoc -I. --python_out=../lambdaize/libmu/ --grpc_python_out=../lambdaize/libmu/ launch.proto &&
cd - &&
cd src/lambdaize/libmu/ &&
protoc -I. --python_out=. joblog.proto &&
cd - &&
./autogen.sh &&
./configure &&
make -j
