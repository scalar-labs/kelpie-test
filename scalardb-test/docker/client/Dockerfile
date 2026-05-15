FROM eclipse-temurin:21-jdk-jammy

ARG KELPIE_VERSION=1.2.4
ARG SCHEMA_LOADER_VERSION=3.18.0

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      curl unzip openssh-client jq ca-certificates \
      python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# cqlsh for loading the CQL schema from the client container.
RUN pip3 install --no-cache-dir cqlsh

RUN curl -fsSL -o /tmp/kelpie.zip \
      "https://github.com/scalar-labs/kelpie/releases/download/${KELPIE_VERSION}/kelpie-${KELPIE_VERSION}.zip" && \
    unzip -q /tmp/kelpie.zip -d /opt && \
    mv "/opt/kelpie-${KELPIE_VERSION}" /opt/kelpie && \
    rm /tmp/kelpie.zip
ENV PATH=/opt/kelpie/bin:$PATH

RUN curl -fsSL -o /opt/scalardb-schema-loader.jar \
      "https://github.com/scalar-labs/scalardb/releases/download/v${SCHEMA_LOADER_VERSION}/scalardb-schema-loader-${SCHEMA_LOADER_VERSION}.jar"
ENV SCHEMA_LOADER_JAR=/opt/scalardb-schema-loader.jar

COPY entrypoint.sh /usr/local/bin/client-entrypoint.sh
RUN chmod +x /usr/local/bin/client-entrypoint.sh

WORKDIR /work/scalardb-test
ENTRYPOINT ["/usr/local/bin/client-entrypoint.sh"]
CMD ["bash"]
