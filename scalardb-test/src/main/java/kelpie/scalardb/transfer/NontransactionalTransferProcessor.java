package kelpie.scalardb.transfer;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class NontransactionalTransferProcessor extends TimeBasedProcessor {
  private final DistributedStorage storage;
  private final int numAccounts;

  public NontransactionalTransferProcessor(Config config) {
    super(config);
    this.storage = TransferCommon.getStorage(config);
    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");
  }

  @Override
  public void executeEach() throws Exception {
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

   transfer(fromId, toId, amount);
  }

  @Override
  public void close() {
    storage.close();
  }

  private void transfer(int fromId, int toId, int amount) throws ExecutionException {
    int fromType = 0;
    int toType = 0;
    if (fromId == toId) {
      toType = 1; // transfer between the same account
    }

    Get fromGet = TransferCommon.prepareGet(fromId, fromType);
    Get toGet = TransferCommon.prepareGet(toId, toType);

    Optional<Result> fromResult = storage.get(fromGet);
    Optional<Result> toResult = storage.get(toGet);
    int fromBalance = TransferCommon.getBalanceFromResult(fromResult.get());
    int toBalance = TransferCommon.getBalanceFromResult(toResult.get());

    Put fromPut = TransferCommon.preparePut(fromId, fromType, fromBalance - amount);
    Put toPut = TransferCommon.preparePut(toId, toType, toBalance + amount);
    try {
    storage.put(fromPut);
    storage.put(toPut);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
