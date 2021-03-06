package print;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.Injector;
import java.util.Random;

public class PrintInjector extends Injector {
  private Random random;

  public PrintInjector(Config config) {
    super(config);
    this.random = new Random(System.currentTimeMillis());
  }

  @Override
  public void inject() {
    try {
      int waitTime = random.nextInt(5000);
      logInfo("Waitng for injection... " + waitTime + " ms");
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      // ignore
    }

    logInfo("Dummy injection");
  }

  @Override
  public void eject() {
    try {
      int waitTime = random.nextInt(5000);
      logInfo("Waitng for ejection... " + waitTime + " ms");
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      // ignore
    }

    logInfo("Dummy ejection");
  }

  @Override
  public void close() {}
}
