package de.claasklar;

import de.claasklar.benchmark.SSB;
import io.opentelemetry.api.trace.StatusCode;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws InterruptedException {

    var benchmark = SSB.createSSBEmbedded(10);
    var span = benchmark.getApplicationSpan();
    try {
      benchmark.runAll();
    } catch (Exception e) {
      span.recordException(e);
      span.setStatus(StatusCode.ERROR);
    } finally {
      span.end();
      Thread.sleep(Duration.ofSeconds(10));
    }
  }
}
