package com.scalar.kelpie.lamb;

import com.scalar.dl.client.service.ClientService;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.PreProcessException;
import com.scalar.kelpie.modules.PreProcessor;
import io.github.resilience4j.retry.Retry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import javax.json.JsonObject;

public class LambPreparer extends PreProcessor {
  private final long DEFAULT_POPULATION_CONCURRENCY = 1L;
  private final long DEFAULT_NUM_POPULATIONS = 1L;
  private static final String TEST_CONFIG_TABLE = "benchmark_config";
  private static final String POPULATION_CONTRACT = "population_contract";
  private static final String POPULATION_CONCURRENCY = "population_concurrency";
  private static final String NUM_POPULATIONS = "num_populations";
  private static final String TARGET_CONTRACT = "target_contract";

  private final ClientService service;
  private final ContractConfigManager contractConfigManager;
  private final String populationContractName;
  private final String targetContractName;

  public LambPreparer(Config config) throws IOException {
    super(config);
    this.service = Common.getClientService(config);
    this.contractConfigManager = new ContractConfigManager(config);
    this.populationContractName = config.getUserString(TEST_CONFIG_TABLE, POPULATION_CONTRACT);
    this.targetContractName = config.getUserString(TEST_CONFIG_TABLE, TARGET_CONTRACT);
  }

  @Override
  public void execute() {
    service.registerCertificate();

    registerContracts(populationContractName);
    registerContracts(targetContractName);

    populateRecords();
  }

  @Override
  public void close() {
    service.close();
  }

  private void registerContracts(String contractName) {
    String classPath = contractConfigManager.getClassPath(contractName);
    Optional<JsonObject> properties = contractConfigManager.getProperties(contractName);
    try {
      service.registerContract(contractName, contractName, classPath, properties);
    } catch (Exception e) {
      throw new PreProcessException("Contract " + contractName + " registration failed", e);
    }
  }

  private void populateRecords() {
    logInfo("insert initial values ... ");

    int populationConcurrency =
        (int)
            config.getUserLong(
                TEST_CONFIG_TABLE, POPULATION_CONCURRENCY, DEFAULT_POPULATION_CONCURRENCY);
    ArgumentBuilder argumentBuilder =
        contractConfigManager.getArgumentBuilder(populationContractName);
    ExecutorService es = Executors.newFixedThreadPool(populationConcurrency);

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, populationConcurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(new PopulationRunner(argumentBuilder), es);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();

    logInfo("all assets have been inserted");
  }

  private class PopulationRunner implements Runnable {
    private final ClientService service;
    private final int numPopulations;
    private final ArgumentBuilder argumentBuilder;

    public PopulationRunner(ArgumentBuilder argumentBuilder) {
      this.service = Common.getClientService(config);
      this.argumentBuilder = argumentBuilder;
      this.numPopulations =
          (int) config.getUserLong(TEST_CONFIG_TABLE, NUM_POPULATIONS, DEFAULT_NUM_POPULATIONS);
    }

    public void run() {
      try {
        IntStream.range(0, numPopulations)
            .forEach(
                i -> {
                  populateWithRetry();
                });
      } catch (Exception e) {
        throw new PreProcessException("Population failed", e);
      } finally {
        service.close();
      }
    }

    private void populateWithRetry() {
      JsonObject argument = argumentBuilder.build();
      Runnable populate = () -> service.executeContract(populationContractName, argument);

      Retry retry = Common.getRetryWithFixedWaitDuration("populate");
      Runnable decorated = Retry.decorateRunnable(retry, populate);
      try {
        decorated.run();
      } catch (Exception e) {
        logError("population failed repeatedly!");
        throw e;
      }
    }
  }
}
