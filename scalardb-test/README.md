# Benchmark and verification Test for Scalar DB

## Usage
1. Set up an environment
    - This job requires a client to execute this job and Cassandra cluster or Cosmos DB which can be accessed from the client

1. Update the `JdbcConfig` instead of `JdbcDatabaseConfig` in the following files If you are using Scalar DB 3.0.0 or older versions.
    * [Common](src/main/java/kelpie/scalardb/Common.java)
    * [NontransactionalTransferProcessor](src/main/java/kelpie/scalardb/transfer/jdbc/NontransactionalTransferProcessor.java)

1. Build
    ```console
    $ gradle shadowJar
    ```

1. Make your config file based on `benchmark-config.toml` or `verification-config.toml`

1. Download Kelpie binary (zip) from https://github.com/scalar-labs/kelpie/releases/ and unzip it
    - You can also build Kelpie from the source


1. Execute a job
    ```console
    $ ${KELPIE}/bin/kelpie --config your_config.toml
    ```

## Workload
- Payment transaction
  - Transfer money between randomly selected 2 accounts
    1. Start a transaction
    2. Read 2 accounts
    3. Calculate new balances
    4. Update 2 accounts with the new balances
    5. Commit the transaction

- Account
  - An account has 2 types `0` and `1`
    - Basically, the type `0` of an account is used
    - When the source account ID and the destination account ID is the same, the money will be sent from type `0` to `1` of the account ID
  - The number of accounts can be set at `[test_config]` `num_accounts` in your config file

## Cassandra Killer
- Cassandra Killer is an injector to kill nodes randomly and restart the nodes
  - The nodes to be killed are selected randomly from `[killer_config]` `contact_points`
  - The interval is set at `[killer_config]` `max_kill_interval_sec`

### How to inject Cassandra Killer
1. Add a module of Cassandra Killer to `[[modules.injectors]]` and configure `[killer_config]` parameters in your config file

    ```toml
    [modules]
      #...
      [[modules.injectors]]
        name = "scalardb.injector.CassandraKiller"
        path = "scalardb-test/build/libs/scalardb-test-all.jar"

    #...

    [killer_config]
      ssh_user = "centos"
      ssh_port = 22
      ssh_private_key = "/home/centos/.ssh/private_key"
      contact_points = "<node1>,<node2>,<node3>"
      max_kill_interval_sec = 300
    ```

2. Execute a job with `--inject` option
```console
$ ${KELPIE}/bin/kelpie --config your_config.toml --inject
```
