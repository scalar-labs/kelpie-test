FROM eclipse-temurin:21-jdk-jammy

ARG KELPIE_VERSION=1.2.4

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      curl unzip openssh-client jq ca-certificates \
      python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# cqlsh for loading the CQL schema from the client container.
RUN pip3 install --no-cache-dir cqlsh

RUN curl -fsSL -o /tmp/kelpie.zip \
      "https://github.com/scalar-labs/kelpie/releases/download/v${KELPIE_VERSION}/kelpie-${KELPIE_VERSION}.zip" && \
    unzip -q /tmp/kelpie.zip -d /opt && \
    mv "/opt/kelpie-${KELPIE_VERSION}" /opt/kelpie && \
    rm /tmp/kelpie.zip
ENV PATH=/opt/kelpie/bin:$PATH

COPY entrypoint.sh /usr/local/bin/client-entrypoint.sh
RUN chmod +x /usr/local/bin/client-entrypoint.sh

WORKDIR /work/scalardb-test
ENTRYPOINT ["/usr/local/bin/client-entrypoint.sh"]
CMD ["bash"]
