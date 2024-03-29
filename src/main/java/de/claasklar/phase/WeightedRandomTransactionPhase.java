package de.claasklar.phase;

import de.claasklar.random.distribution.RandomNumberGenerator;
import de.claasklar.specification.TopSpecification;
import de.claasklar.util.Pair;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeightedRandomTransactionPhase implements TransactionPhase {
  private static final Logger logger =
      LoggerFactory.getLogger(WeightedRandomTransactionPhase.class);

  private final long totalCount;
  private final int threadCount;
  private final double targetOps;
  private final List<Pair<Double, TopSpecification>> weightedSpecifications;
  private final RandomNumberGenerator random;
  private final Span applicationSpan;
  private final Tracer tracer;

  /**
   * @param totalCount total count of executions
   * @param threadCount number of parallel threads
   * @param targetOps targeted operations per millisecond
   * @param weightedSpecifications weighted list of specifications
   * @param random RandomNumberGenerator
   * @param applicationSpan Śpan, that runs for the duration of the application
   * @param tracer Tracer
   */
  public WeightedRandomTransactionPhase(
      long totalCount,
      int threadCount,
      double targetOps,
      List<Pair<Double, TopSpecification>> weightedSpecifications,
      RandomNumberGenerator random,
      Span applicationSpan,
      Tracer tracer) {
    this.totalCount = totalCount;
    if (targetOps <= 0) {
      throw new IllegalArgumentException("cannot execute zero or less than zero operations");
    }
    this.threadCount = threadCount;
    this.targetOps = targetOps;
    this.weightedSpecifications = weightedSpecifications;
    this.random = random;
    this.applicationSpan = applicationSpan;
    this.tracer = tracer;
  }

  @Override
  public void run() {
    var transactionSpan =
        tracer
            .spanBuilder("transaction phase")
            .setParent(Context.current().with(applicationSpan))
            .startSpan();
    logger.atInfo().log("start of transaction phase");
    logger.atInfo().log(
        () ->
            weightedSpecifications.stream()
                .map(it -> it.second().getName() + " with weight " + it.first())
                .collect(Collectors.joining("\n")));
    try {
      var threads = new LinkedList<Thread>();
      for (int i = 0; i < threadCount; i++) {
        threads.add(
            new Thread(
                new TransactionRunnable(
                    totalCount / threadCount,
                    targetOps / threadCount,
                    weightedSpecifications,
                    random,
                    transactionSpan,
                    tracer)));
      }

      logger.atInfo().log(() -> "starting " + threadCount + " threads");
      for (var thread : threads) {
        thread.start();
      }
      for (var thread : threads) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          transactionSpan.recordException(e);
          logger.atWarn().log(e.getMessage());
        }
      }
    } catch (Exception e) {
      logger.atError().log(e::getMessage);
      transactionSpan.recordException(e);
    } finally {
      logger.atInfo().log("end of transaction phase");
      transactionSpan.end();
    }
  }
}
