FROM openjdk:8u212-jre-slim-stretch

RUN mkdir /apps

COPY ./client-test.tar .
COPY ./sample-keys /apps/sample-keys
COPY ./src /apps/src
COPY ./benchmark-config-docker.toml /apps/

RUN apt-get update && apt-get install wget unzip -y && \
    wget -O /tmp/kelpie.zip https://github.com/scalar-labs/kelpie/releases/download/1.1.0/kelpie-1.1.0.zip && \
    unzip /tmp/kelpie.zip && mv /kelpie-1.1.0/bin/kelpie /usr/local/bin/kelpie && \
    rm -rf /tmp/kelpie.zip kelpie-1.1.0 /var/lib/apt/lists/*

RUN tar xf client-test.tar && \
    rm -f client-test.tar && \
    mv client-test/lib /usr/local && \
    mv /client-test/kelpie-test.jar /apps && \
    rm -rf /client-test