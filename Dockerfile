FROM openjdk:8-jre-alpine
ADD ./build/libs/ /opt/firehose
WORKDIR /opt/firehose
CMD ["java", "-cp", "./*", "com.gojek.esb.launch.Main"]

