FROM openjdk:22-jdk-bullseye

RUN apt update
RUN apt install -y maven wget unzip

RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v28.0/protoc-28.0-linux-s390_64.zip \
    && unzip protoc-28.0-linux-s390_64.zip -d /usr/local \
    && rm protoc-28.0-linux-s390_64.zip \
    && chmod +x /usr/local/bin/protoc
