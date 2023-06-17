package de.claasklar;

import static com.mongodb.client.model.Filters.*;

import de.claasklar.benchmark.SSB;
import de.claasklar.generation.*;
import io.opentelemetry.api.trace.StatusCode;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws InterruptedException {

    var config = SSB.createSSBDenormalized(10);
    var span = config.second();
    try {
      config.first().runAll();
    } catch (Exception e) {
      span.recordException(e);
      span.setStatus(StatusCode.ERROR);
    } finally {
      span.end();
      Thread.sleep(Duration.ofSeconds(10));
    }
  }
}
