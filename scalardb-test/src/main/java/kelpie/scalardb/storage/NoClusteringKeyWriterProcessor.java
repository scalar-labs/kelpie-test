package kelpie.scalardb.storage;

import static kelpie.scalardb.storage.NoClusteringKeySchema.preparePut;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.concurrent.ThreadLocalRandom;
import kelpie.scalardb.transfer.TransferCommon;

public class NoClusteringKeyWriterProcessor extends TimeBasedProcessor {
  private final DistributedStorage storage;
  private final int numKeys;

  public NoClusteringKeyWriterProcessor(Config config) {
    super(config);
    storage = TransferCommon.getStorage(config);
    numKeys = (int) config.getUserLong("test_config", "num_keys");
  }

  @Override
  protected void executeEach() throws Exception {
    int pkey = ThreadLocalRandom.current().nextInt(numKeys);
    int colValue = ThreadLocalRandom.current().nextInt();
    Put put = preparePut(pkey, colValue);
    storage.put(put);
  }

  @Override
  public void close() throws Exception {
    storage.close();
  }
}
