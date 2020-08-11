package kelpie.scalardb.sensor;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;

public class SensorPreparer extends PreProcessor {
  public SensorPreparer(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    logInfo("nothing to do for the preparation");
  }

  @Override
  public void close() {}
}
