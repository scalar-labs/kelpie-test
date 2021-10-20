package kelpie.scalardb.transfer;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.kelpie.config.Config;
import javax.annotation.Nullable;
import kelpie.scalardb.Common;

public class LedgerTransferCommon {
  public static final String KEYSPACE = "ledger_transfer";
  public static final String TABLE = "tx_transfer";
  public static final String ACCOUNT_ID = "account_id";
  public static final String AGE = "age";
  public static final String BALANCE = "balance";
  public static final String METADATA = "metadata";
  public static final String CONFIG_TABLE_NAME = "test_config";
  public static final int INITIAL_BALANCE = 10000;
  public static final long DEFAULT_METADATA_SIZE = 100;

  public static DistributedTransactionManager getTransactionManager(Config config) {
    return Common.getTransactionManager(config, KEYSPACE, TABLE);
  }

  public static Scan prepareScanForLatest(int id) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));

    return new Scan(partitionKey)
        .withOrdering(new Ordering(AGE, Order.DESC))
        .withLimit(1)
        .withConsistency(Consistency.LINEARIZABLE);
  }

  public static Get prepareGet(int id) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(AGE, 0));

    return new Get(partitionKey, clusteringKey).withConsistency(Consistency.LINEARIZABLE);
  }

  public static Put preparePut(int id, int age, int amount, @Nullable String metadata) {
    Key partitionKey = new Key(new IntValue(ACCOUNT_ID, id));
    Key clusteringKey = new Key(new IntValue(AGE, age));
    Put put =
        new Put(partitionKey, clusteringKey)
            .withConsistency(Consistency.LINEARIZABLE)
            .withValue(new IntValue(BALANCE, amount));
    if (metadata != null) {
      put.withValue(new TextValue(METADATA, metadata));
    }
    return put;
  }

  public static int getAgeFromResult(Result result) {
    return result.getValue(AGE).get().getAsInt();
  }

  public static int getBalanceFromResult(Result result) {
    return result.getValue(BALANCE).get().getAsInt();
  }
}
