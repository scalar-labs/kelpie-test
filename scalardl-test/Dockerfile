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

WORKDIR /scalar/kelpie

RUN mkdir -p scalardl-test/build/libs/

COPY build/classes scalardl-test/build/classes
COPY scalardl-test-all.jar scalardl-test/build/libs/
COPY sample-keys scalardl-test/sample-keys
COPY config.toml.tmpl config.toml.tmpl

CMD ["dockerize", "-template", "config.toml.tmpl:config.toml", \
    "kelpie", "--config", "config.toml"]
