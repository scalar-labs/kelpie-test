package kelpie.scalardb.sensor;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;

public class SensorCommon {
  private static final String NAMESPACE = "sensor";
  private static final String TABLE = "tx_sensor";
  private static final String TIMESTAMP = "timestamp";
  private static final String DEVICE_ID = "device_id";
  private static final String REVISION = "revision";

  public static Scan prepareScan(int timestamp) {
    Key partitionKey = Key.ofInt(TIMESTAMP, timestamp);
    return Scan.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(partitionKey)
        .ordering(Scan.Ordering.asc(DEVICE_ID))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  public static Put preparePut(int timestamp, int deviceId, int revision) {
    Key partitionKey = Key.ofInt(TIMESTAMP, timestamp);
    Key clusteringKey = Key.ofInt(DEVICE_ID, deviceId);
    return Put.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .consistency(Consistency.LINEARIZABLE)
        .intValue(REVISION, revision)
        .build();
  }

  public static boolean hasDuplicatedRevision(List<Result> results) {
    IntStream revisions = results.stream().mapToInt(SensorCommon::getRevisionFromResult);

    Set<Integer> tempSet = new HashSet<>();
    return revisions.anyMatch(rev -> !tempSet.add(rev));
  }

  public static int getMaxRevision(List<Result> results) {
    OptionalInt maxRevision = results.stream().mapToInt(SensorCommon::getRevisionFromResult).max();

    return maxRevision.orElse(0);
  }

  public static int getRevisionFromResult(Result result) {
    return result.getInt(REVISION);
  }
}
