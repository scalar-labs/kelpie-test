package com.scalar.kelpie.lamb;

import com.scalar.kelpie.config.Config;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.util.Optional;

public class ContractConfigManager {
  private static final String TEST_CONFIG_TABLE = "test_config";
  private static final String CONTRACT_CONFIG = "contract_config";
  private static final String VARIABLE_CONFIG = "variable_config";
  private static final String CLASS_FILE = "class_file";
  private static final String PROPERTIES = "properties";
  private static final String ARGUMENTS = "arguments";

  private final JsonObject contractConfig;
  private final JsonObject variableConfig;

  public ContractConfigManager(Config config) throws IOException, FileNotFoundException {
    String contractConfigFile = config.getUserString(TEST_CONFIG_TABLE, CONTRACT_CONFIG);
    try (InputStream stream = new FileInputStream(contractConfigFile);
        JsonReader reader = Json.createReader(stream)) {
      this.contractConfig = reader.readObject();
    }

    String argumentConfigFile = config.getUserString(TEST_CONFIG_TABLE, VARIABLE_CONFIG);
    try (InputStream stream = new FileInputStream(argumentConfigFile);
        JsonReader reader = Json.createReader(stream)) {
      this.variableConfig = reader.readObject();
    }
  }

  public String getClassPath(String contractName) {
    return contractConfig.getString(CLASS_FILE);
  }

  public Optional<JsonObject> getProperties(String contractName) {
    JsonObject properties =  contractConfig.getJsonObject(PROPERTIES);

    return Optional.ofNullable(properties);
  }

  public ArgumentBuilder getArgumentBuilder(String contractName) {
    JsonObject contractArguments =
        contractConfig.getJsonObject(contractName).getJsonObject(ARGUMENTS);
    JsonObject contractVariableConfig = variableConfig.getJsonObject(contractName);

    return new ArgumentBuilder(contractArguments, contractVariableConfig);
  }
}
