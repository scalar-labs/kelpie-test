# Kelpie client-test

## How to build docker images

to build the image

```console
$ gradle docker

BUILD SUCCESSFUL in 2s
5 actionable tasks: 3 executed, 2 up-to-date
```

to verify the docker image is built

```console
$ docker images
REPOSITORY                        TAG                      IMAGE ID            CREATED             SIZE
scalarlabs/kelpie-test-client     latest                   164f177e8f1a        6 minutes ago       206MB
```

## How to run kelpie inside docker

```console
$ docker run -ti scalarlabs/kelpie-test-client
root@04953c357473:/# kelpie --config client-test/benchmark-config.toml
2020-07-16 00:25:25,725 [INFO  com.scalar.kelpie.Kelpie] Checking a job config...
2020-07-16 00:25:25,838 [INFO  com.scalar.kelpie.Kelpie] Loading modules...
2020-07-16 00:25:27,196 [INFO  com.scalar.kelpie.Kelpie] Starting the job...
```