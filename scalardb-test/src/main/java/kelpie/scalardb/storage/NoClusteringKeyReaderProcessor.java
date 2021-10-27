package kelpie.scalardb.storage;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.transfer.TransferCommon;

public class NoClusteringKeyReaderProcessor extends TimeBasedProcessor {
  private final DistributedStorage storage;
  private final int numKeys;

  public NoClusteringKeyReaderProcessor(Config config) {
    super(config);
    storage = TransferCommon.getStorage(config);
    numKeys = (int) config.getUserLong("test_config", "num_keys");
  }

  @Override
  protected void executeEach() throws Exception {
    int pkey = ThreadLocalRandom.current().nextInt(numKeys);
    Get get = NoClusteringKeySchema.prepareGet(pkey);
    Optional<Result> result = storage.get(get);
    if (!result.isPresent()) {
      logWarn("the results should exist, but not");
    }
  }

  @Override
  public void close() throws Exception {
    storage.close();
  }
}
