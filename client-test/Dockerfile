FROM openjdk:8u212-jre-slim-stretch

COPY ./client-test.tar .
COPY ./kelpie /usr/local/bin

RUN tar xf client-test.tar && rm -f client-test.tar

RUN mv client-test/lib /usr/local && rm -rf client-test