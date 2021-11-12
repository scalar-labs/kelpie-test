FROM docker.io/busybox:1.32 AS tools

ENV DOCKERIZE_VERSION v0.6.1

# Install dockerize
RUN set -x && \
    wget -q "https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz" && \
    tar -xzvf "dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz" && \
    ./dockerize --version

FROM openjdk:8u212-jre-slim-stretch

COPY --from=tools dockerize /usr/local/bin/

ENV KELPIE_VERSION 1.2.0

RUN apt-get update && apt-get install wget unzip -y && \
    wget -O /tmp/kelpie.zip https://github.com/scalar-labs/kelpie/releases/download/${KELPIE_VERSION}/kelpie-${KELPIE_VERSION}.zip && \
    unzip /tmp/kelpie.zip && \
    mv /kelpie-${KELPIE_VERSION}/bin/kelpie /usr/local/bin/kelpie && \
    mv /kelpie-${KELPIE_VERSION}/lib/* /usr/local/lib/ && \
    rm -rf /tmp/kelpie.zip kelpie-${KELPIE_VERSION} /var/lib/apt/lists/*

WORKDIR /scalardb/kelpie

RUN mkdir -p scalardb-test/build/libs/

COPY scalardb-test-all.jar scalardb-test/build/libs/
COPY grpc-config.toml.tmpl grpc-config.toml.tmpl

CMD ["dockerize", "-template", "grpc-config.toml.tmpl:grpc-config.toml", \
    "kelpie", "--config", "grpc-config.toml"]
