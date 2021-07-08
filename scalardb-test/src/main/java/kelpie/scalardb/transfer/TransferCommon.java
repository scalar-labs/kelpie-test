package kelpie.scalardb.transfer;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import com.scalar.kelpie.config.Config;
import io.github.resilience4j.retry.Retry;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import kelpie.scalardb.Common;

public class TransferCommon {
  public static final String KEYSPACE = "transfer";
  public static final String TABLE = "tx_transfer";
  public static final String ACCOUNT_ID = "account_id";
  public static final String ACCOUNT_TYPE = "account_type";
  public static final String BALANCE = "balance";

  public static final int INITIAL_BALANCE = 10000;
  public static final int NUM_TYPES = 2;

  public static DistributedTransactionManager getTransactionManager(Config config) {
    return Common.getTransactionManager(config, KEYSPACE, TABLE);
  }

  public static DistributedStorage getStorage(Config config) {
    DistributedStorage storage = Common.getStorage(config);
    storage.with(KEYSPACE, TABLE);
    return storage;
  }

  public static Get prepareGet(int id, int type) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));

    return new Get(partitionKey, clusteringKey).withConsistency(Consistency.LINEARIZABLE);
  }

  public static Put preparePut(int id, int type, int amount) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(ACCOUNT_TYPE, type));
    return new Put(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .withValue(new IntValue(BALANCE, amount));
  }

  public static List<Result> readRecordsWithRetry(Config config) {
    DistributedTransactionManager manager = getTransactionManager(config);
    Retry retry = Common.getRetryWithExponentialBackoff("readRecords");
    Supplier<List<Result>> decorated =
        Retry.decorateSupplier(retry, () -> readRecords(manager, config));

    try {
      return decorated.get();
    } catch (Exception e) {
      throw new RuntimeException("Reading records failed repeatedly", e);
    } finally {
      manager.close();
    }
  }

  private static List<Result> readRecords(DistributedTransactionManager manager, Config config) {
    int numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    List<Result> results = new ArrayList<>();

    boolean isFailed = false;
    for (int i = 0; i < numAccounts; i++) {
      for (int j = 0; j < TransferCommon.NUM_TYPES; j++) {
        DistributedTransaction transaction = null;
        try {
          transaction = manager.start();
          Get get = TransferCommon.prepareGet(i, j);
          transaction.get(get).ifPresent(results::add);
          transaction.commit();
        } catch (TransactionException e) {
          // continue to read other records
          isFailed = true;
          if (transaction != null) {
            try {
              transaction.abort();
            } catch (AbortException ex) {
              // ignore
            }
          }
        }
      }
    }

    if (isFailed) {
      // for Retry
      throw new RuntimeException("at least 1 record couldn't be read");
    }

    return results;
  }

  public static int getBalanceFromResult(Result result) {
    return ((IntValue) result.getValue(BALANCE).get()).get();
  }

  public static int getTotalInitialBalance(Config config) {
    int numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    return INITIAL_BALANCE * NUM_TYPES * numAccounts;
  }

  public static int getActualTotalVersion(List<Result> results) {
    return results.stream().mapToInt(r -> (new TransactionResult(r)).getVersion() - 1).sum();
  }

  public static int getActualTotalBalance(List<Result> results) {
    return results.stream().mapToInt(r -> ((IntValue) r.getValue(BALANCE).get()).get()).sum();
  }
}
