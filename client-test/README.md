# How to build

to build the image with run

```console
$ make build
gradle shadowJar

BUILD SUCCESSFUL in 842ms
2 actionable tasks: 2 up-to-date
docker build -t scalarlabs/kelpie-test-client:latest .
Sending build context to Docker daemon  133.3MB
Step 1/7 : FROM openjdk:8u212-jre-slim-stretch
 ---> 1d98c8a8ff74
Step 2/7 : RUN apt-get update && apt-get install wget unzip -y &&     wget -O /tmp/kelpie.zip https://github.com/scalar-labs/kelpie/releases/download/1.1.0/kelpie-1.1.0.zip &&     unzip /tmp/kelpie.zip &&     mv /kelpie-1.1.0/bin/kelpie /usr/local/bin/kelpie &&     mv /kelpie-1.1.0/lib/* /usr/local/lib/ &&     rm -rf /tmp/kelpie.zip kelpie-1.1.0 /var/lib/apt/lists/*
 ---> Using cache
 ---> b3da6268e126
Step 3/7 : RUN mkdir /client-test
 ---> Using cache
 ---> 8b26f1c86175
Step 4/7 : COPY build /client-test/build
 ---> Using cache
 ---> 45c46cfdbfe5
Step 5/7 : COPY sample-keys /client-test/sample-keys
 ---> Using cache
 ---> c50c5c8e7c70
Step 6/7 : COPY src client-test/src
 ---> Using cache
 ---> b72c69ca4e60
Step 7/7 : COPY benchmark-config.toml /client-test/
 ---> Using cache
 ---> a838d0b0c38f
Successfully built a838d0b0c38f
Successfully tagged scalarlabs/kelpie-test-client:latest
```
