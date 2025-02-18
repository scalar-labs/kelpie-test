package kelpie.scalardb.transfer.dynamo;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class NontransactionalTransferProcessor extends TimeBasedProcessor {
  private final DynamoDbClient client;
  private final int numAccounts;

  public NontransactionalTransferProcessor(Config config) {
    super(config);

    client = DynamoUtil.createDynamoClient(config);
    numAccounts = (int) config.getUserLong("test_config", "num_accounts");
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
      logWarn("Failed to close the DynamoDB client", e);
    }
  }

  private void transfer(int fromId, int toId, int amount) {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    Map<String, AttributeValue> fromKey = new HashMap<>();
    fromKey.put("account_id", AttributeValue.builder().n(Integer.toString(fromId)).build());
    fromKey.put("account_type", AttributeValue.builder().n(Integer.toString(fromType)).build());

    Map<String, AttributeValue> toKey = new HashMap<>();
    toKey.put("account_id", AttributeValue.builder().n(Integer.toString(toId)).build());
    toKey.put("account_type", AttributeValue.builder().n(Integer.toString(toType)).build());

    try {
      GetItemRequest getFrom =
          GetItemRequest.builder().tableName("transfer").key(fromKey).consistentRead(true).build();
      Map<String, AttributeValue> from = client.getItem(getFrom).item();
      GetItemRequest getTo =
          GetItemRequest.builder().tableName("transfer").key(toKey).consistentRead(true).build();
      Map<String, AttributeValue> to = client.getItem(getTo).item();

      int fromBalance = Integer.parseInt(from.get("balance").n());
      int toBalance = Integer.parseInt(to.get("balance").n());
      if (fromBalance - amount < 0) {
        throw new RuntimeException("insufficient balance in the from account.");
      }

      Map<String, AttributeValueUpdate> fromValues = new HashMap<>();
      fromValues.put(
          "balance",
          AttributeValueUpdate.builder()
              .value(AttributeValue.builder().n(Integer.toString(fromBalance - amount)).build())
              .build());
      Map<String, AttributeValueUpdate> toValues = new HashMap<>();
      toValues.put(
          "balance",
          AttributeValueUpdate.builder()
              .value(AttributeValue.builder().n(Integer.toString(toBalance + amount)).build())
              .build());

      UpdateItemRequest fromRequest =
          UpdateItemRequest.builder()
              .tableName("transfer")
              .key(fromKey)
              .attributeUpdates(fromValues)
              .build();
      UpdateItemRequest toRequest =
          UpdateItemRequest.builder()
              .tableName("transfer")
              .key(toKey)
              .attributeUpdates(toValues)
              .build();
      client.updateItem(fromRequest);
      client.updateItem(toRequest);
    } catch (DynamoDbException e) {
      logWarn(e.getMessage());
      throw e;
    }
  }
}
