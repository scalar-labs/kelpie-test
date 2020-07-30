package kelpie.scalardb.transfer;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import com.scalar.kelpie.config.Config;
import java.util.List;
import kelpie.scalardb.Common;

public class TransferCommon {
  private static final String KEYSPACE = "transfer";
  private static final String TABLE = "tx_transfer";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";

  public static final int INITIAL_BALANCE = 10000;
  public static final int NUM_TYPES = 2;

  public static DistributedTransactionManager getTransactionManager(Config config) {
    return Common.getTransactionManager(config, KEYSPACE, TABLE);
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

  public static int getBalanceFromResult(Result result) {
    return ((IntValue) result.getValue(BALANCE).get()).get();
  }

  public static int getTotalInitialBalance(Config config) {
    int numAccounts = (int) config.getUserLong("test_config", "num_accounts");
    return INITIAL_BALANCE * NUM_TYPES * numAccounts;
  }

  public static int getActualTotalVersion(List<Result> results) {
    return results.stream().mapToInt(r -> ((TransactionResult) r).getVersion() - 1).sum();
  }

  public static int getActualTotalBalance(List<Result> results) {
    return results.stream().mapToInt(r -> ((IntValue) r.getValue(BALANCE).get()).get()).sum();
  }
}
