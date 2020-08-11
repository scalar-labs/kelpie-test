package kelpie.scalardb.sensor;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.io.IntValue;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import com.scalar.db.io.Key;
import com.scalar.kelpie.config.Config;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;
import kelpie.scalardb.Common;

public class SensorCommon {
  private static final String KEYSPACE = "sensor";
  private static final String TABLE = "tx_sensor";
  private static final String TIMESTAMP = "timestamp";
  private static final String DEVICE_ID = "device_id";
  private static final String REVISION = "revision";

  public static DistributedTransactionManager getTransactionManager(Config config) {
    return Common.getTransactionManager(config, KEYSPACE, TABLE);
  }

  public static Scan prepareScan(int timestamp) {
    Key partitionKey = new Key(new IntValue(TIMESTAMP, timestamp));

    return new Scan(partitionKey)
        .withOrdering(new Scan.Ordering(DEVICE_ID, Scan.Ordering.Order.ASC))
        .withConsistency(Consistency.LINEARIZABLE);
  }

  public static Put preparePut(int timestamp, int deviceId, int revision) {
    Key partitionKey = new Key(new IntValue(TIMESTAMP, timestamp));
    Key clusteringKey = new Key(new IntValue(DEVICE_ID, deviceId));
    return new Put(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .withValue(new IntValue(REVISION, revision));
  }

  public static boolean hasDuplicatedRevision(List<Result> results) {
    IntStream revisions = results.stream().mapToInt(r -> getRevisionFromResult(r));

    Set<Integer> tempSet = new HashSet<>();
    return revisions.anyMatch(rev -> !tempSet.add(rev));
  }

  public static int getMaxRevision(List<Result> results) {
    OptionalInt maxRevision = results.stream().mapToInt(r -> getRevisionFromResult(r)).max();

    return maxRevision.orElse(0);
  }

  private static int getRevisionFromResult(Result result) {
    return ((IntValue) result.getValue(REVISION).get()).get();
  }
}
