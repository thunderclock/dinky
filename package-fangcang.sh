docker build --platform=linux/amd64 -t dinky-fangcang:1.2.3-flink1.18-fangcang -f deploy/docker/Dockerfile .
docker tag dinky-fangcang:1.2.3-flink1.18-fangcang swr.cn-southwest-2.myhuaweicloud.com/fangcang/dinky-fangcang:1.2.3-flink1.18-fangcang-02
docker push swr.cn-southwest-2.myhuaweicloud.com/fangcang/dinky-fangcang:1.2.3-flink1.18-fangcang-02