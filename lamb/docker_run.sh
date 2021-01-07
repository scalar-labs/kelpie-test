#!/bin/bash

usage_exit() {
  echo "Usage: $0 -l LAMB_CONFIG -c CONTRACT_CONFIG -v VARIABLE_CONFIG -p POPULATION_CONTRACT -t TARGET_CONTRACT" 1>&2
  exit 1
}

# TODO: skip the pre or main process
while getopts l:c:v:p:t:h OPT
do
  case ${OPT} in
    l) LAMB_CONFIG=${OPTARG}
      ;;
    c) CONTRACT_CONFIG=${OPTARG}
      ;;
    v) VARIABLE_CONFIG=${OPTARG}
      ;;
    p) POPULATION_CONTRACT=${OPTARG}
      ;;
    t) TARGET_CONTRACT=${OPTARG}
      ;;
    h) usage_exit
      ;;
    \?) usage_exit
      ;;
    esac
done

docker run \
  -v ${LAMB_CONFIG}:/lamb/lamb_config.toml \
  -v ${CONTRACT_CONFIG}:/lamb/contract_config.json \
  -v ${VARIABLE_CONFIG}:/lamb/variable_config.json \
  -v ${POPULATION_CONTRACT}:/lamb/${POPULATION_CONTRACT##*/} \
  -v ${TARGET_CONTRACT}:/lamb/${TARGET_CONTRACT##*/} \
  ghcr.io/scalar-labs/kelpie-lamb
