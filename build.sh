mvn clean package

if [[ $? -ne 0 ]]; then
echo "Build failed..."
exit 1
fi

rm -rf ./build/*
mkdir -p build/tlink/lib
mkdir -p build/tlink/logs
cp -r ./bin build/tlink
cp -r ./conf build/tlink/
cp -r ./target/lib/* build/tlink/lib
cp ./target/tlink-1.0.0.jar build/tlink/lib
