package de.claasklar.phase;

import de.claasklar.specification.PrimaryWriteSpecification;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadPhase {
  private static final Logger logger = LoggerFactory.getLogger(LoadPhase.class);
  private final PrimaryWriteSpecification primaryWriteSpecification;
  private final long targetCount;
  private final int numThreads;
  private final Span applicationSpan;
  private final ExecutorService executor;
  private final Semaphore semaphore;
  private final Tracer tracer;

  public LoadPhase(
      PrimaryWriteSpecification primaryWriteSpecification,
      long targetCount,
      Span applicationSpan,
      Tracer tracer) {
    this(
        primaryWriteSpecification,
        targetCount,
        Runtime.getRuntime().availableProcessors() * 4,
        applicationSpan,
        tracer);
  }

  public LoadPhase(
      PrimaryWriteSpecification primaryWriteSpecification,
      long targetCount,
      int numThreads,
      Span applicationSpan,
      Tracer tracer) {
    this.primaryWriteSpecification = primaryWriteSpecification;
    this.targetCount = targetCount;
    this.numThreads = numThreads;
    this.applicationSpan = applicationSpan;
    this.executor = Executors.newFixedThreadPool(numThreads);
    this.semaphore = new Semaphore(numThreads);
    this.tracer = tracer;
  }

  public void load() {
    var loadSpan =
        tracer
            .spanBuilder("load phase")
            .setParent(Context.current().with(applicationSpan))
            .startSpan();
    logger.atInfo().log("beginning of load phase");
    try {
      for (int i = 0; i < targetCount; i++) {
        submitTask(primaryWriteSpecification.runnable());
      }
      logger.atInfo().log("waiting for all tasks to finish");
      executor.shutdown();
      var finished = executor.awaitTermination(5, TimeUnit.MINUTES);
      if (!finished) {
        loadSpan.addEvent("not all tasks could be finished in time");
        logger.atWarn().log("not all tasks could be finished in time");
        executor.shutdownNow();
      }
      logger.atInfo().log("all tasks finished");
    } catch (Exception e) {
      loadSpan.recordException(e);
      loadSpan.setStatus(StatusCode.ERROR);
      throw new RuntimeException(e);
    } finally {
      logger.atInfo().log("end of load phase");
      loadSpan.end();
    }
  }

  private void submitTask(Runnable r) {
    try {
      semaphore.acquire();
      executor.execute(
          () -> {
            try {
              r.run();
            } finally {
              semaphore.release();
            }
          });
    } catch (RejectedExecutionException e) {
      semaphore.release();
      throw e;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
