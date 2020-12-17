# Kelpie Lamb
You can benchmark your contract with Lamb.

## Build

```console
$ ./gradlew shadowJar
```

## How to run a test
1. Edit lamb_config.toml
2. Edit contract_config.json
3. Edit variable_config.json
4. Run a test with Kelpie

```console
$ ${KELPIE}/bin/kelpie --config lamb_config.toml
```

### With docker
#### Docker build
```console
$ ./gradlew docker
```

#### Run a test
- Your Lamb config file should specify the file path for your contracts on the docker image
  - The path of contract_config.json and variable_config.json are fixed to `/lamb/contract_config.json` and `/lamb/variable_config.json`
```console
$ docker run scalarlabs/kelpie-lamb \
         -v /path/to/your_lamb_config.toml:/lamb/lamb_config.toml \
         -v /path/to/your_contract_config.json:/lamb/contract_config.json \
         -v /path/to/your_variable_config.json:/lamb/variable_config.json \
         -v /path/to/your_population_contract.class:/lamb/your_population_contract.class \
         -v /path/to/your_target_contract.class:/lamb/your_target_contract.class
```

## Benchmark configuration
You can configure a benchmark job by a toml file like `lamb_config.toml`.
It's a Kelpie configuration file. You don't have to modify `[modules]` for your benchmark. You can refer to Kelpie's [README](https://github.com/scalar-labs/kelpie) about `[common]` and `[stats]` table.

### [benchmark_config]
A population contract, a target contract and your contract information are specified in this table. They are used for executing contracts in your benchmark.
- `contract_config`: A file for the contract configuration
- `variable_config`: A file for the contracts' variables
- `population_contract`: The name of a contract which populate initial assets before benchmarking
- `target_contract`: The name of a contract which you want to benchmark
- `populateion_concurrency`: The number of concurrency to execute the population contract. The default value is 1.
- `num_populateions`: The number of times to execute the population contract per thread. The number of total execution times is the product of `population_concurrency` and `num_populations`. The default value is 1.

### [client_config]
They are information for your Scalar DL server.
- `dl_server`: The address of Scalar DL server
- `certificate`: A certificate file which will be registered to the server to register and execute contracts
- `private_key`: The corresponding private key with the certificate to handle your contracts

## Contract configuration


## Variable configuration
