package de.claasklar.benchmark;

import de.claasklar.database.Database;
import de.claasklar.phase.IndexPhase;
import de.claasklar.phase.LoadPhase;
import de.claasklar.phase.PhaseTopic;
import de.claasklar.phase.TransactionPhase;
import de.claasklar.util.TelemetryConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Benchmark {

  private final Logger logger = LoggerFactory.getLogger(Benchmark.class);

  private final IndexPhase indexPhase;
  private final LoadPhase loadPhase;
  private final TransactionPhase transactionPhase;
  private final Database database;
  private final List<ExecutorService> executors;
  private final Span applicationSpan;
  private final PhaseTopic phaseTopic;

  public Benchmark(
      IndexPhase indexPhase,
      LoadPhase loadPhase,
      TransactionPhase transactionPhase,
      Database database,
      List<ExecutorService> executors,
      Span applicationSpan,
      PhaseTopic phaseTopic) {
    this.indexPhase = indexPhase;
    this.loadPhase = loadPhase;
    this.transactionPhase = transactionPhase;
    this.database = database;
    this.executors = executors;
    this.applicationSpan = applicationSpan;
    this.phaseTopic = phaseTopic;
  }

  public void runAll() throws InterruptedException {
    try {
      phaseTopic.notifyObservers(PhaseTopic.BenchmarkPhase.INDEX);
      indexPhase.createIndexes();
      phaseTopic.notifyObservers(PhaseTopic.BenchmarkPhase.LOAD);
      loadPhase.load();
      phaseTopic.notifyObservers(PhaseTopic.BenchmarkPhase.TRANSACTION);
      transactionPhase.run();
      logger.atInfo().log(() -> "version: " + TelemetryConfig.version());
      Thread.sleep(Duration.ofSeconds(10));
      executors.forEach(ExecutorService::shutdown);
      executors.stream()
          .parallel()
          .forEach(
              it -> {
                try {
                  var threadShutdown = it.awaitTermination(1, TimeUnit.MINUTES);
                  if (!threadShutdown) {
                    logger.atWarn().log("could not terminate thread executor");
                  }
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              });
      database.close();
      phaseTopic.notifyObservers(PhaseTopic.BenchmarkPhase.END);
    } catch (Exception e) {
      logger.atError().log(e.getMessage());
      applicationSpan.recordException(e);
      applicationSpan.setStatus(StatusCode.ERROR);
    } finally {
      executors.forEach(ExecutorService::shutdownNow);
      applicationSpan.end();
      Thread.sleep(Duration.ofSeconds(10));
    }
  }

  public Span getApplicationSpan() {
    return applicationSpan;
  }
}
