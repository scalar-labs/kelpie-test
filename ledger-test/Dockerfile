FROM openjdk:8u212-jre-slim-stretch
ENV KELPIE_VERSION 1.1.0

RUN apt-get update && apt-get install wget unzip -y && \
    wget -O /tmp/kelpie.zip https://github.com/scalar-labs/kelpie/releases/download/${KELPIE_VERSION}/kelpie-${KELPIE_VERSION}.zip && \
    unzip /tmp/kelpie.zip && \
    mv /kelpie-${KELPIE_VERSION}/bin/kelpie /usr/local/bin/kelpie && \
    mv /kelpie-${KELPIE_VERSION}/lib/* /usr/local/lib/ && \
    rm -rf /tmp/kelpie.zip kelpie-${KELPIE_VERSION} /var/lib/apt/lists/*

RUN mkdir -p /ledger-test/build/libs/

COPY ledger-test-all.jar /ledger-test/build/libs/
COPY sample-keys /ledger-test/sample-keys
COPY benchmark-config.toml /ledger-test/
