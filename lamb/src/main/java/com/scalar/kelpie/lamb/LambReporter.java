package com.scalar.kelpie.lamb;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PostProcessor;

public class LambReporter extends PostProcessor {

  public LambReporter(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    getSummary();
  }

  @Override
  public void close() {}
}
