#!/bin/bash

LAMB_CONFIG="/lamb/lamb_config.toml"
CONTRACT_CONFIG="/lamb/contract_config.json"
VARIABLE_CONFIG="/lamb/variable_config.json"

if [ ! -f "${LAMB_CONFIG}" ] \
  || [ ! -f "${CONTRACT_CONFIG}" ] \
  || [ ! -f "${VARIABLE_CONFIG}" ]; then
  echo "ERROR: LAMB_CONFIG, CONTRACT_CONFIG and VARIABLE_CONFIG are required"
  exit 1
fi

cd /lamb
# TODO: skip the pre or main process
kelpie --config ${LAMB_CONFIG}
