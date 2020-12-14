package sample_contract;

import com.scalar.dl.ledger.contract.Contract;
import com.scalar.dl.ledger.database.Ledger;
import java.util.Optional;
import java.util.stream.IntStream;
import javax.json.Json;
import javax.json.JsonObject;

public class BatchCreate extends Contract {

  @Override
  public JsonObject invoke(Ledger ledger, JsonObject argument, Optional<JsonObject> properties) {
    int numPerGroup = properties.get().getInt("num_per_group");
    int groupId = argument.getInt("group_id");
    int balance = argument.getInt("balance");

    int startId = groupId * numPerGroup;
    int endId = (groupId + 1) * numPerGroup;

    IntStream.range(startId, endId)
        .forEach(
            id -> {
              JsonObject json = Json.createObjectBuilder().add("balance", balance).build();
              String assetId = String.valueOf(id);
              ledger.put(assetId, json);
            });

    return null;
  }
}
