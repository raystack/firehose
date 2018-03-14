FROM openjdk:8-jdk
ADD . /eglc
WORKDIR /eglc
RUN ["./gradlew", "build"]
ADD "http://artifactory-gojek.golabs.io/artifactory/gojek-release-local/com/gojek/esb/esb-influx-db-sink/2.0.0/esb-influx-db-sink-2.0.0.jar" /eglc/build/libs/
CMD ["java", "-cp", "build/libs/*", "com.gojek.esb.launch.Main"]
