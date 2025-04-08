package kelpie.scalardb.transfer;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.Common;

public class TransferReadOnlyProcessor extends TimeBasedProcessor {
  private final DistributedTransactionManager manager;
  private final int numAccounts;

  public TransferReadOnlyProcessor(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config);

    this.numAccounts = (int) config.getUserLong("test_config", "num_accounts");
  }

  @Override
  public void executeEach() throws Exception {
    int id = ThreadLocalRandom.current().nextInt(numAccounts);
    Get get = TransferCommon.prepareGet(id, 0);
    Optional<Result> result = manager.get(get);
    if (!result.isPresent()) {
      logWarn("The record doesn't exist. ID: " + id);
    }
  }

  @Override
  public void close() {
    try {
      manager.close();
    } catch (Exception e) {
      logWarn("Failed to close the transaction manager", e);
    }
  }
}
