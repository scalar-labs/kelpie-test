package kelpie.scalardb.transfer.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.transfer.TransferCommon;

public class NontransactionalTransferProcessor extends TimeBasedProcessor {
  private final CosmosClient client;
  private final CosmosContainer container;
  private CosmosStoredProcedure storedProcedure;
  private final int numAccounts;
  private final boolean useStoredProcedure;
  private final String TABLE = TransferCommon.TABLE + "_cosmos";
  private final String QUERY_FOR_SP = "SELECT * FROM " + TABLE + "_cosmos t WHERE t.id = ";

  public NontransactionalTransferProcessor(Config config) {
    super(config);

    client = CosmosUtil.createCosmosClient(config);
    container = client.getDatabase(TransferCommon.NAMESPACE).getContainer(TABLE);
    numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    useStoredProcedure = config.getUserBoolean("cosmos_config", "use_stored_procedure");
    if (useStoredProcedure) {
      storedProcedure = container.getScripts().getStoredProcedure("upsert.js");
    }
  }

  @Override
  public void executeEach() {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

    try {
      transfer(fromId, toId, amount);
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (Exception e) {
      logWarn("Failed to close CosmosClient", e);
    }
  }

  private void transfer(int fromId, int toId, int amount) {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    String fromCosmosId = fromId + ":" + fromType;
    String toCosmosId = toId + ":" + toType;

    Account from =
        container.readItem(fromCosmosId, new PartitionKey(fromId), Account.class).getItem();
    Account to = container.readItem(toCosmosId, new PartitionKey(toId), Account.class).getItem();

    from.setBalance(from.getBalance() - amount);
    to.setBalance(to.getBalance() + amount);

    if (useStoredProcedure) {
      storedProcedure.execute(
          Arrays.asList(from, QUERY_FOR_SP + "\"" + from.getId() + "\""),
          new CosmosStoredProcedureRequestOptions()
              .setPartitionKey(new PartitionKey(from.getAccountId())));
      storedProcedure.execute(
          Arrays.asList(to, QUERY_FOR_SP + "\"" + to.getId() + "\""),
          new CosmosStoredProcedureRequestOptions()
              .setPartitionKey(new PartitionKey(to.getAccountId())));
    } else {
      container.upsertItem(from, new PartitionKey(fromId), new CosmosItemRequestOptions());
      container.upsertItem(to, new PartitionKey(toId), new CosmosItemRequestOptions());
    }
  }
}
