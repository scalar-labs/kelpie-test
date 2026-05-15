FROM cassandra:5.0

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openssh-server sudo procps iproute2 && \
    rm -rf /var/lib/apt/lists/*

# Allow key-based root login; the killer SSHs in to send the kill/restart.
RUN sed -i 's/^#*PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config

# Switch the commitlog to synchronous fsync per write so verification runs see
# real durability behavior under SIGKILL faults. Batch mode rejects the
# `commitlog_sync_period` setting that comes with the default `periodic` mode,
# so the period line is removed.
RUN sed -i \
      -e 's/^commitlog_sync:.*/commitlog_sync: batch/' \
      -e '/^commitlog_sync_period:/d' \
      /etc/cassandra/cassandra.yaml

RUN mkdir -p /run/sshd /var/run/cassandra && \
    chown cassandra:cassandra /var/run/cassandra

COPY cassandra-init /etc/init.d/cassandra
COPY entrypoint.sh /usr/local/bin/cluster-entrypoint.sh
RUN chmod +x /etc/init.d/cassandra /usr/local/bin/cluster-entrypoint.sh

EXPOSE 22 7000 7001 9042

ENTRYPOINT ["/usr/local/bin/cluster-entrypoint.sh"]
