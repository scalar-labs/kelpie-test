# Benchmark test for Scalar DL contract

## Build

```console
$ ./gradlew shadowJar
```

## How to run a test
1. Edit contract-list.json
2. Edit argument-configuration.toml
3. Edit lamb-configuration.toml
4. Run a test with Kelpie

```console
$ ${KELPIE}/bin/kelpie --config lamb-configuration.toml
```
