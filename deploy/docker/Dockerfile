ARG FLINK_VERSION
ARG DINKY_VERSION

FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/flink:1.18.1-scala_2.12-java17 as builder
ARG FLINK_VERSION
ADD build/dinky-release*.tar.gz /opt
RUN  mv /opt/dinky-release* /opt/dinky  && mkdir /opt/dinky/config/static
ADD  build/dist/ /opt/dinky/config/static

FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/flink:1.18.1-scala_2.12-java17
ARG FLINK_VERSION
COPY --from=builder /opt/dinky /opt/dinky
RUN rm -f /opt/flink/lib/flink-table-planner-loader*.jar  &&  cp /opt/flink/opt/flink-python*.jar /opt/flink/lib/ && cp /opt/flink/opt/flink-table-planner*.jar /opt/flink/lib/ 2>/dev/null || : && ln -s /opt/flink/lib/*  /opt/dinky/extends/flink${FLINK_VERSION}/
ENV FLINK_VERSION=${FLINK_VERSION}
ENV DINKY_HOME=/opt/dinky/
ENV H2_DB=./tmp/db/h2


USER root

WORKDIR /opt/dinky/


RUN mkdir -p /opt/dinky/customJar /opt/dinky/logs && chmod -R 777 /opt/dinky/ && mkdir /dinky && chmod -R 777 /dinky/ && sed -i 's/-Xms512M -Xmx2048M -XX:ermSize=512M/-XX:+UseContainerSupport -XX:InitialRAMPercentage=70.0 -XX:MaxRAMPercentage=70.0/g' ./bin/auto.sh && sed -i 's/-XX:PermSize=512M/-XX:MetaspaceSize=512M/g' ./bin/auto.sh && sed -i 's/-XX:MaxPermSize=1024M/-XX:MaxMetaspaceSize=1024M/g' ./bin/auto.sh

EXPOSE 8888

CMD  ./bin/auto.sh startOnPending
