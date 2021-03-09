package kelpie.scalardb.transfer.cosmos;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.scalar.kelpie.config.Config;

public class CosmosUtil {

  public static CosmosClient createCosmosClient(Config config) {
    String endpoint = config.getUserString("cosmos_config", "endpoint");
    String key = config.getUserString("cosmos_config", "key");

    return new CosmosClientBuilder()
        .endpoint(endpoint)
        .key(key)
        .directMode()
        .consistencyLevel(ConsistencyLevel.STRONG)
        .buildClient();
  }
}
