package de.claasklar.phase;

import de.claasklar.random.distribution.RandomNumberGenerator;
import de.claasklar.specification.TopSpecification;
import de.claasklar.util.Pair;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionPhase {
  private static final Logger logger = LoggerFactory.getLogger(TransactionPhase.class);

  private final long totalCount;
  private final int threadCount;
  private final int targetOps;
  private final List<Pair<Double, TopSpecification>> weightedSpecifications;
  private final RandomNumberGenerator random;
  private final Span applicationSpan;
  private final Tracer tracer;

  public TransactionPhase(
      long totalCount,
      int threadCount,
      int targetOps,
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

  public void run() {
    var transactionSpan =
        tracer
            .spanBuilder("transaction phase")
            .setParent(Context.current().with(applicationSpan))
            .startSpan();
    logger.atInfo().log("start of transaction phase");
    try {
      var threads = new LinkedList<Thread>();
      for (int i = 0; i < threadCount; i++) {
        threads.add(
            new Thread(
                new TransactionRunnable(
                    totalCount / threadCount,
                    (double) targetOps / (double) threadCount,
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
