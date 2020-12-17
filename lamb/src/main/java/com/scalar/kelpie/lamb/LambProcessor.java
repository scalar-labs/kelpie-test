package com.scalar.kelpie.lamb;

import com.scalar.dl.client.service.ClientService;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.io.FileNotFoundException;
import java.io.IOException;
import javax.json.JsonObject;

public class LambProcessor extends TimeBasedProcessor {
  private static final String TEST_CONFIG_TABLE = "benchmark_config";
  private static final String CONTRACTS = "target_contract";

  private final ClientService service;
  private final String contractName;
  private final ContractConfigManager configManager;
  private final ArgumentBuilder argumentBuilder;

  public LambProcessor(Config config) throws IOException, FileNotFoundException {
    super(config);
    this.service = Common.getClientService(config);
    this.configManager = new ContractConfigManager(config);
    // TODO: multiple contracts
    this.contractName = config.getUserString(TEST_CONFIG_TABLE, CONTRACTS);
    this.argumentBuilder = configManager.getArgumentBuilder(contractName);
  }

  @Override
  public void executeEach() {
    try {
      JsonObject arguments = argumentBuilder.build();
      service.executeContract(contractName, arguments);
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void close() {
    service.close();
  }
}
